#
# Copyright 2015-2021, Institute for Systems Biology
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Populate the DB with data for the next IDC version

import sys
import os
import argparse
from pathlib import Path
import time
from datetime import datetime, timezone, timedelta
import logging
from logging import INFO, DEBUG
import pydicom
import hashlib
from subprocess import run, PIPE
import shutil
from multiprocessing import Process, Queue
from queue import Empty
from base64 import b64decode
from pydicom.errors import InvalidDicomError
from uuid import uuid4
from google.cloud import storage
from idc.models import Version, Collection, Patient, Study, Series, Instance, Retired, WSI_metadata, instance_source
from sqlalchemy import select,delete
from sqlalchemy.orm import Session
from utilities.tcia_helpers import  get_hash, get_TCIA_studies_per_patient, get_TCIA_patients_per_collection,\
    get_TCIA_series_per_study, get_TCIA_instance_uids_per_series, get_TCIA_instances_per_series, get_collection_values_and_counts
from utilities.get_collection_dois import get_data_collection_doi, get_analysis_collection_dois
from google.api_core.exceptions import Conflict

from python_settings import settings
import settings as etl_settings

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base

from ingestion.sources import TCIA, Pathology

rootlogger = logging.getLogger('root')
errlogger = logging.getLogger('root.err')

PATIENT_TRIES=3


BUF_SIZE = 65536
def md5_hasher(file_path):
    md5 = hashlib.md5()
    with open(file_path, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            md5.update(data)
    return md5.hexdigest()


# Hash a sorted list of hashes
def get_merkle_hash(hashes):
    md5 = hashlib.md5()
    hashes.sort()
    for hash in hashes:
        md5.update(hash.encode())
    return md5.hexdigest()


def rollback_copy_to_prestaging_bucket(client, args, series):
    bucket = client.bucket(args.prestaging_bucket)
    for instance in series.instances:
        try:
            results = bucket.blob(f'{instance.uuid}.dcm').delete()
        except:
            errlogger.error('p%s: Failed to delete blob %s.dcm during validation rollback',args.id, instance.uuid)
            raise


def validate_series_in_gcs(args, collection, patient, study, series):
    client = storage.Client(project=args.project)
    # blobs_info = get_series_info(storage_client, args.project, args.staging_bucket)
    bucket = client.get_bucket(args.prestaging_bucket)
    try:
        for instance in series.instances:
            blob = bucket.blob(f'{instance.uuid}.dcm')
            blob.reload()
            assert instance.hash == b64decode(blob.md5_hash).hex()
            assert instance.instance_size == blob.size

    except Exception as exc:
        rollback_copy_to_prestaging_bucket(client, args, series)
        errlogger.error('p%s: GCS validation failed for %s/%s/%s/%s/%s',
            args.id, collection.collection_id, patient.submitter_case_id, study.study_instance_uid, series.series_instance_uid, instance.sop_instance_uid)
        raise exc


# Copy the series instances downloaded from TCIA/NBIA from disk to the prestaging bucket
def copy_disk_to_prestaging_bucket(args, series):
    # Do the copy as a subprocess in order to use the gsutil -m option
    try:
        # Copy the series to GCS
        src = "{}/{}/*".format(args.dicom, series.series_instance_uid)
        dst = "gs://{}/".format(args.prestaging_bucket)
        result = run(["gsutil", "-m", "-q", "cp", src, dst])
        if result.returncode < 0:
            errlogger.error('p%s: \tcopy_disk_to_prestaging_bucket failed for series %s', args.id, series.series_instance_uid)
            raise RuntimeError('p%s: copy_disk_to_prestaging_bucket failed for series %s', args.id, series.series_instance_uid)
        # rootlogger.debug("p%s: Uploaded instances to GCS", args.id)
    except Exception as exc:
        errlogger.error("\tp%s: Copy to prestage bucket failed for series %s", args.id, series.series_instance_uid)
        raise RuntimeError("p%s: Copy to prestage bucketfailed for series %s", args.id, series.series_instance_uid) from exc


# # Copy a completed collection from the prestaging bucket to the staging bucket
# def copy_prestaging_to_staging_bucket(args, collection):
#     rootlogger.info("Copying prestaging bucket to staging bucket")
#     try:
#         # Copy the series to GCS
#         src = "gs://{}/*".format(args.prestaging_bucket)
#         dst = "gs://{}/".format(args.staging_bucket)
#         result = run(["gsutil", "-m", "-q", "cp", src, dst])
#         if result.returncode < 0:
#             errlogger.error('\tp%s: copy_prestaging_to_staging_bucket failed for collection %s', args.id, collection.collection_id)
#             raise RuntimeError('p%s: copy_prestaging_to_staging_bucket failed for collection %s', args.id, collection.collection_id)
#         rootlogger.debug(("p%s: Uploaded instances to GCS", args.id))
#     except Exception as exc:
#         errlogger.error("\tp%s: Copy from prestaging to staging bucket for collection %s failed", args.id, collection.collection_id)
#         raise RuntimeError("p%s: Copy from prestaging to staging bucket for collection %s failed", args.id, collection.collection_id) from exc


# def copy_staging_bucket_to_final_bucket(args, version):
#     try:
#         # Copy the series to GCS
#         src = "gs://{}/*".format(args.staging_bucket)
#         dst = "gs://{}/".format(args.bucket)
#         result = run(["gsutil", "-m", "-q", "cp", src, dst])
#         if result.returncode < 0:
#             errlogger.error('\tp%s: copy_staging_bucket_to_final_bucket failed for version %s', args.id, version.idc_version_number)
#             raise RuntimeError('p%s: copy_staging_bucket_to_final_bucket failed for version %s', args.id, version.idc_version_number)
#         rootlogger.debug(("p%s: Uploaded instances to GCS"))
#     except Exception as exc:
#         errlogger.error("\tp%s: Copy from prestaging to staging bucket for collection %s failed", args.id, version.idc_version_number)
#         raise RuntimeError("p%s: Copy from prestaging to staging bucket for collection %s failed", args.id, version.idc_version_number) from exc


def empty_bucket(bucket):
    try:
        src = "gs://{}/*".format(bucket)
        run(["gsutil", "-m", "-q", "rm", src])
        rootlogger.debug(("Emptied bucket %s", bucket))
    except Exception as exc:
        errlogger.error("Failed to empty bucket %s", bucket)
        raise RuntimeError("Failed to empty bucket %s", bucket) from exc

def create_prestaging_bucket(args):
    client = storage.Client(project='idc-dev-etl')

    # Try to create the destination bucket
    new_bucket = client.bucket(args.prestaging_bucket)
    new_bucket.iam_configuration.uniform_bucket_level_access_enabled = True
    new_bucket.versioning_enabled = False
    try:
        result = client.create_bucket(new_bucket, location='US-CENTRAL1', project=args.project)
        # return(0)
    except Conflict:
        # Bucket exists
        pass
    except Exception as e:
        # Bucket creation failed somehow
        errlogger.error("Error creating bucket %s: %s",args.prestaging_bucket, e)
        return(-1)


def copy_disk_to_gcs(args, collection, patient, study, series):
    storage_client = storage.Client(project=args.project)

    # Delete the zip file before we copy to GCS so that it is not copied
    os.remove("{}/{}.zip".format(args.dicom, series.series_instance_uid))

    # Copy the instances to the staging bucket
    copy_disk_to_prestaging_bucket(args, series)

    # Ensure that they were copied correctly
    validate_series_in_gcs(args, collection, patient, study, series)

    # Delete the series from disk
    shutil.rmtree("{}/{}".format(args.dicom, series.series_instance_uid), ignore_errors=True)

def copy_gcs_to_gcs(args, instance, gcs_url):
    storage_client = storage.Client(project=args.project)
    src_bucket = args.src_bucket
    dst_bucket = storage_client.bucket((args.prestaging_bucket))
    src_blob = src_bucket.blob(gcs_url)
    dst_blob = dst_bucket.blob(f'{instance.uuid}.dcm')
    token, bytes_rewritten, total_bytes = dst_blob.rewrite(src_blob)
    while token:
        rootlogger.debug('******p%s: Rewrite bytes_rewritten %s, total_bytes %s', bytes_rewritten, total_bytes)
        token, bytes_rewritten, total_bytes = dst_blob.rewrite(src_blob, token=token)

    # new_blob = src_bucket.copy_blob(blob, dst_bucket, new_name = f'{instance.uuid}.dcm')


def accum_sources(parent, children):
    sources = list(parent.sources)
    for child in children:
        sources = [x | y for (x, y) in zip(sources, child.sources)]
    return sources


def retire_instance(sess, args, instance, source):
    instance_data = []
    if instance.source == source:
        # Record info about retired instances in the Retired table.
        # This is useful in creating the DICOM store.
        instance_data.append(
            dict(
                timestamp=instance.timestamp,
                sop_instance_uid=instance.sop_instance_uid,
                instance_uuid=instance.uuid,
                hash=instance.hash,
                source=instance.source,
                instance_size=instance.instance_size,
                init_idc_version=instance.init_idc_version,
                rev_idc_version=instance.rev_idc_version,
                series_instance_uid=instance.series.series_instance_uid,
                study_instance_uid=instance.series.study.study_instance_uid,
                submitter_case_id=instance.series.study.patient.submitter_case_id,
                collection_id=instance.study.series.study.patient.collection.collection_id,
                series_uuid=instance.series.uuid,
                study_uuid=instance.series.study.uuid,
                idc_case_id=instance.series.study.patient.submitter_case_id
            )
        )
        sess.bulk_insert_mappings(Retired, instance_data)

        sess.delete(instance)
        sess.commit()
    else:
        instance.min_timestamp = instance.max_timestamp = datetime.utcnow()


def build_instances_tcia(sess, args, source, version, collection, patient, study, series):
    # Download a zip of the instances in a series
    # It will be write the zip to a file dicom/<series_instance_uid>.zip in the
    # working directory, and expand the zip to directory dicom/<series_instance_uid>

    # When TCIA provided series timestamps, we'll us that for timestamp.
    now = datetime.now(timezone.utc)

    # rootlogger.debug("      p%s: Series %s, building instances; %s", args.id, series.series_instance_uid, time.asctime())

    # Delete the series from disk in case it is there from a previous run
    try:
        shutil.rmtree("{}/{}".format(args.dicom, series.series_instance_uid), ignore_errors=True)
    except:
        # It wasn't there
        pass

    download_start = time.time_ns()
    get_TCIA_instances_per_series(args.dicom, series.series_instance_uid, args.server)
    download_time = (time.time_ns() - download_start)/10**9
    # rootlogger.debug("      p%s: Series %s, download time: %s", args.id, series.series_instance_uid, (time.time_ns() - download_start)/10**9)
    # rootlogger.debug("      p%s: Series %s, downloading instance data; %s", args.id, series.series_instance_uid, time.asctime())

    # Get a list of the files from the download
    dcms = [dcm for dcm in os.listdir("{}/{}".format(args.dicom, series.series_instance_uid))]

    if 'LICENSE' in dcms:
        os.remove("{}/{}/LICENSE".format(args.dicom, series.series_instance_uid))
        dcms.remove('LICENSE')


    # Ensure that the zip has the expected number of instances
    # rootlogger.debug("      p%s: Series %s, check series length; %s", args.id, series.series_instance_uid, time.asctime()

    if not len(dcms) == len(series.instances):
        errlogger.error("      p%s: Invalid zip file for %s/%s/%s/%s", args.id,
            collection.collection_id, patient.submitter_case_id, study.study_instance_uid, series.series_instance_uid)
        # Return without marking all instances done. This will be prevent the series from being done.
        return
        # raise RuntimeError("      \p%s: Invalid zip file for %s/%s/%s/%s", args.id,
        #     collection.collection_id, patient.submitter_case_id, study.study_instance_uid, series.series_instance_uid)
    # rootlogger.debug("      p%s: Series %s download successful", args.id, series.series_instance_uid)

    # TCIA file names are based on the position of the image in a scan. We need to extract the SOPInstanceUID
    # so that we can know the instance.
    # Use pydicom to open each file to get its UID and rename the file with its associated uuid that we
    # generated when we expanded this series.

    # Replace the TCIA assigned file name
    # Also compute the md5 hash and length in bytes of each
    # rootlogger.debug("      p%s: Series %s, changing instance filename; %s", args.id, series.series_instance_uid, time.asctime())
    pydicom_times=[]
    psql_times=[]
    rename_times=[]
    metadata_times=[]
    begin = time.time_ns()
    instances = {instance.sop_instance_uid:instance for instance in series.instances}

    for dcm in dcms:
        try:
            pydicom_times.append(time.time_ns())
            SOPInstanceUID = pydicom.dcmread("{}/{}/{}".format(args.dicom, series.series_instance_uid, dcm), stop_before_pixels=True).SOPInstanceUID
            pydicom_times.append(time.time_ns())
        except InvalidDicomError:
            errlogger.error("       p%s: Invalid DICOM file for %s/%s/%s/%s", args.id,
                collection.collection_id, patient.submitter_case_id, study.study_instance_uid, series.series_instance_uid)
            if args.server == 'NLST':
                # For NLST only, just delete the invalid file
                os.remove("{}/{}/{}".format(args.dicom, series.series_instance_uid, dcm))
                continue
            else:
                # Return without marking all instances done. This will be prevent the series from being done.
                return

        psql_times.append(time.time_ns())
        ## instance = next(instance for instance in series.instances if instance.sop_instance_uid == SOPInstanceUID)
        instance = instances[SOPInstanceUID]
        psql_times.append(time.time_ns())

        rename_times.append(time.time_ns())
        uuid = instance.uuid
        file_name = "{}/{}/{}".format(args.dicom, series.series_instance_uid, dcm)
        blob_name = "{}/{}/{}.dcm".format(args.dicom, series.series_instance_uid, uuid)
        if os.path.exists(blob_name):
            errlogger.error("       p%s: Duplicate DICOM files for %s/%s/%s/%s/%s", args.id,
                collection.collection_id, patient.submitter_case_id, study.study_instance_uid, series.series_instance_uid, SOPInstanceUID)
            if args.server == 'NLST':
                # For NLST only, just delete the duplicate
                os.remove("{}/{}/{}".format(args.dicom, series.series_instance_uid, dcm))
                continue
            else:
                # Return without marking all instances done. This will be prevent the series from being done.
                return

        os.rename(file_name, blob_name)
        rename_times.append(time.time_ns())

        metadata_times.append(time.time_ns())
        instance.hash = md5_hasher(blob_name)
        instance.instance_size = Path(blob_name).stat().st_size
        instance.timestamp = datetime.utcnow()
        metadata_times.append(time.time_ns())

    if args.server == 'NLST':
        # For NLST only, delete any instances for which there is not a corresponding file
        for instance in series.instances:
            if not os.path.exists("{}/{}/{}.dcm".format(args.dicom, series.series_instance_uid, instance.uuid)):
                sess.execute(delete(Instance).where(Instance.uuid==instance.uuid))
                series.instances.remove(instance)

    instances_time = time.time_ns() - begin
    # rootlogger.debug("      p%s: Renamed all files for series %s; %s", args.id, series.series_instance_uid, time.asctime())
    # rootlogger.debug("      p%s: Series %s instances time: %s", args.id, series.series_instance_uid, instances_time/10**9)
    # rootlogger.debug("      p%s: Series %s pydicom time: %s", args.id, series.series_instance_uid, (sum(pydicom_times[1::2]) - sum(pydicom_times[0::2]))/10**9)
    # rootlogger.debug("      p%s: Series %s psql time: %s", args.id, series.series_instance_uid, (sum(psql_times[1::2]) - sum(psql_times[0::2]))/10**9)
    # rootlogger.debug("      p%s: Series %s rename time: %s", args.id, series.series_instance_uid, (sum(rename_times[1::2]) - sum(rename_times[0::2]))/10**9)
    # rootlogger.debug("      p%s: Series %s metadata time: %s", args.id, series.series_instance_uid, (sum(metadata_times[1::2]) - sum(metadata_times[0::2]))/10**9)

    copy_start = time.time_ns()
    try:
        copy_disk_to_gcs(args, collection, patient, study, series)
    except:
        # Copy failed. Return without marking all instances done. This will be prevent the series from being done.
        return
    copy_time = (time.time_ns() - copy_start)/10**9
    # rootlogger.debug("      p%s: Series %s, copying time; %s", args.id, series.series_instance_uid, (time.time_ns() - copy_start)/10**9)

    mark_done_start = time.time ()
    for instance in series.instances:
        instance.done = True
    mark_done_time = time.time() - mark_done_start
    # rootlogger.debug("      p%s: Series %s, completed build_instances; %s", args.id, series.series_instance_uid, time.asctime())
    rootlogger.debug("        p%s: Series %s: download: %s, instances: %s, pydicom: %s, psql: %s, rename: %s, metadata: %s, copy: %s, mark_done: %s",
                     args.id, series.series_instance_uid,
                     download_time,
                     instances_time/10**9,
                     (sum(pydicom_times[1::2]) - sum(pydicom_times[0::2]))/10**9,
                     (sum(psql_times[1::2]) - sum(psql_times[0::2]))/10**9,
                     (sum(rename_times[1::2]) - sum(rename_times[0::2]))/10 **9,
                     (sum(metadata_times[1::2]) - sum(metadata_times[0::2])) / 10 ** 9,
                     copy_time,
                     mark_done_time)


def build_instances_path(sess, args, source, version, collection, patient, study, series):
    # Download a zip of the instances in a series
    # It will be write the zip to a file dicom/<series_instance_uid>.zip in the
    # working directory, and expand the zip to directory dicom/<series_instance_uid>

    # When TCIA provided series timestamps, we'll us that for timestamp.
    now = datetime.now(timezone.utc)

    stmt = select(WSI_metadata.sop_instance_uid, WSI_metadata.gcs_url, WSI_metadata.hash, WSI_metadata.size).\
        where(WSI_metadata.series_instance_uid == series.series_instance_uid)
    result = sess.execute(stmt)
    src_instance_metadata = {i.sop_instance_uid:{'gcs_url':i.gcs_url, 'hash':i.hash, 'size': i.size} \
                             for i in result.fetchall()}
    for instance in series.instances:
        if not instance.done:
            instance.size = src_instance_metadata[instance.sop_instance_uid]['size']
            instance.hash = src_instance_metadata[instance.sop_instance_uid]['hash']
            instance.rev_idc_version = args.version
            instance.timestamp = datetime.utcnow()
            copy_gcs_to_gcs(args, instance, src_instance_metadata[instance.sop_instance_uid]['gcs_url'])
            instance.done = True


def retire_series(sess, args, series, source):
    # If this object has children from source, delete them
    if series.sources[source]:
        for instance in series.instances:
            retire_instance(sess, args, instance, source)
        series.sources[source] = False
        # Update sources of this object
        for instance in series.instances:
            series.sources = [a or b for a, b in zip(series.sources, instance.sources)]
        # If this object is not empty, return
        if any(series.sources):
            return
        sess.delete(series)
        sess.commit()
    else:
        series.min_timestamp = series.max_timestamp = datetime.utcnow()


def expand_series(sess, args, source, series):
    source_objects = source.instances(series)
    if series.is_new:
        metadata = []
        for instance in source_objects:
            metadata.append(
                dict(
                    series_instance_uid = series.series_instance_uid,
                    init_idc_version=args.version,
                    rev_idc_version=args.version,
                    sop_instance_uid=instance,
                    uuid=uuid4(),
                    revised=False,
                    done=False,
                    is_new=True,
                    expanded=False,
                    source=source.source,
                    hash="",
                    size=0
                )
            )
        sess.bulk_insert_mappings(Instance, metadata)
    else:
        idc_objects = {object.sop_instance_uid: object for object in series.instance}

        new_objects = [id for id in source_objects if id not in idc_objects]
        retired_objects = [idc_objects[id] for id in idc_objects if id not in source_objects]
        existing_objects = [idc_objects[id] for id in source_objects if id in idc_objects]

        for instance in retired_objects:
            retire_instance(sess, args, instance, instance_source['path'].value)

        for instance in existing_objects:
            if source.instance_hashes_differ(instance):
                instance.revised = False
                instance.done = False
                instance.is_new = False
                instance.expanded = False


        metadata = []
        for instance in new_objects:
            metadata.append(
                dict(
                    series_instance_uid = series.series_instance_uid,
                    init_idc_version=args.version,
                    rev_idc_version=args.version,
                    sop_instance_uid = instance,
                    uuid = uuid4(),
                    revised = False,
                    done = False,
                    is_new = True,
                    expanded = False,
                    source=source.source,
                    hash="",
                    size=0
                )
            )
        sess.bulk_insert_mappings(Instance, metadata)
    series.expanded = True
    sess.commit()
    # rootlogger.debug("      p%s: Expanded series %s", args.id, series.series_instance_uid)


def build_series(sess, args, source, series_index, version, collection, patient, study, series):
    if not series.done:
        begin = time.time()
        if not series.expanded:
            tcia_time = expand_series(sess, args, source, series)
        else:
            tcia_time=0
        begin1 = time.time_ns()
        rootlogger.info("      p%s: Series %s; %s; %s instances, tcia: %s, expand: %s", args.id, series.series_instance_uid, series_index, len(series.instances), tcia_time, time.time()-begin)
        get_len_time = (time.time_ns() - begin1) / 10 ** 9

        begin2 = time.time_ns()
        if source.source_id == instance_source.tcia.value:
            build_instances_tcia(sess, args, source, version, collection, patient, study, series)
        else:
            build_instances_path(sess, args, source, version, collection, patient, study, series)
        build_instances_time = (time.time_ns() - begin2) / 10 ** 9

        if all(instance.done for instance in series.instances):
            series.min_timestamp = min(instance.timestamp for instance in series.instances)
            series.max_timestamp = max(instance.timestamp for instance in series.instances)

            # Get hash of children
            hash = source.idc_series_hash(series)
            # Test whether anything has changed
            if hash != series.hashes[source.source_id]:
                hashes = list(series.hashes)
                hashes[source.source_id] = hash
                series.hashes = hashes

                # Assume that all instances in a series have the same source
                sources = list(series.sources)
                sources[source.source_id] = True
                series.sources = sources
                series.series_instances = len(series.instances)
                series.rev_idc_version = max(instance.rev_idc_version for instance in series.instances)

                if not series.is_new:
                    series.revised = True
                    series.uuid = uuid4()

            series.done = True
            sess.commit()
            duration = str(timedelta(seconds=(time.time() - begin)))
            rootlogger.debug("      p%s: Series %s, %s, get num instances: %s, build instances: %s", args.id, series.series_instance_uid, series_index, get_len_time, build_instances_time)

            rootlogger.info("      p%s: Series %s, %s, completed in %s", args.id, series.series_instance_uid, series_index, duration)
    else:
        rootlogger.info("      p%s: Series %s, %s, previously built", args.id, series.series_instance_uid, series_index)


def retire_study(sess, args, study, source):
    # If this object has children from source, delete them
    if study.sources[source]:
        for series in study.seriess:
            retire_series(sess, args, series, source)
        study.sources[source] = False
        # Update sources of this object
        for series in study.seriess:
            study.sources = [a or b for a, b in zip(study.sources, series.sources)]
        # If this object is not empty, return
        if any(study.sources):
            return
        sess.delete(study)
        sess.commit()
    else:
        study.min_timestamp = study.max_timestamp = datetime.utcnow()


def expand_study(sess, args, source, study, data_collection_doi, analysis_collection_dois):
    source_objects = source.series(study)
    if study.is_new:
        metadata = []
        for series in source_objects:
            metadata.append(
                dict(
                    study_instance_uid = study.study_instance_uid,
                    init_idc_version=args.version,
                    rev_idc_version=args.version,
                    series_instance_uid=series,
                    uuid = uuid4(),
                    source_doi=analysis_collection_dois[series] \
                        if series in analysis_collection_dois
                        else data_collection_doi,
                    sources = (False,False),
                    hashes = ("",""),
                    revised=False,
                    done=False,
                    is_new=True,
                    expanded=False
                )
            )
        sess.bulk_insert_mappings(Series, metadata)
    else:
        idc_objects = {object.series_instance_uid: object for object in study.series}

        new_objects = [id for id in source_objects if id not in idc_objects]
        retired_objects = [idc_objects[id] for id in idc_objects if id not in source_objects]
        existing_objects = [idc_objects[id] for id in source_objects if id in idc_objects]

        for series in retired_objects:
            retire_series(sess, args, series, instance_source['path'].value)

        for series in existing_objects:
            if source.series_hashes_differ(series):
                series.revised = False
                series.done = False
                series.is_new = False
                series.expanded = False


        metadata = []
        for series in new_objects:
            metadata.append(
                dict(
                    study_instance_uid = study.study_instance_uid,
                    init_idc_version=args.version,
                    rev_idc_version=args.version,
                    series_instance_uid = series,
                    uuid = uuid4(),
                    source_doi=analysis_collection_dois[series.series_instance_uid] \
                        if series.series_instance_uid in analysis_collection_dois
                    else data_collection_doi,
                    sources = (False,False),
                    hashes = ("",""),
                    revised = False,
                    done = False,
                    is_new = True,
                    expanded = False
                )
            )
        sess.bulk_insert_mappings(Series, metadata)
    study.expanded = True
    sess.commit()
    # rootlogger.debug("    p%s: Expanded study %s",args.id,  study.study_instance_uid)


def build_study(sess, args, source, study_index, version, collection, patient, study, data_collection_doi, analysis_collection_dois):
    if not study.done:
        begin = time.time()
        if not study.expanded:
            expand_study(sess, args, source, study, data_collection_doi, analysis_collection_dois)
        rootlogger.info("    p%s: Study %s, %s, %s series, expand time: %s", args.id, study.study_instance_uid, study_index, len(study.seriess), time.time()-begin)
        for series in study.seriess:
            series_index = f'{study.seriess.index(series)+1} of {len(study.seriess)}'
            build_series(sess, args, source, series_index, version, collection, patient, study, series)

        if all([series.done for series in study.seriess]):
            study.min_timestamp = min([series.min_timestamp for series in study.seriess if series.min_timestamp != None])
            study.max_timestamp = max([series.max_timestamp for series in study.seriess if series.max_timestamp != None])

            # Get hash of children
            hash = source.idc_study_hash(study)
            # Test whether anything has changed
            if hash != study.hashes[source.source_id]:
                hashes = list(study.hashes)
                hashes[source.source_id] = hash
                study.hashes = hashes

                study.sources = accum_sources(study, study.seriess)
                study.study_instances = sum([series.series_instances for series in study.seriess])
                study.rev_idc_version = max(series.rev_idc_version for series in study.seriess)

                if not study.is_new:
                    study.revised = True
                    study.uuid = uuid4()

            study.done = True
            sess.commit()
            duration = str(timedelta(seconds=(time.time() - begin)))
            rootlogger.info("    p%s: Study %s, %s,  completed in %s", args.id, study.study_instance_uid, study_index, duration)
    else:
        rootlogger.info("    p%s: Study %s, %s, previously built", args.id, study.study_instance_uid, study_index)


def retire_patient(sess, args, patient, source):
    # If this object has children from source, delete them
    if patient.sources[source]:
        for study in patient.studies:
            retire_study(sess, args, study, source)
        patient.sources[source] = False
        # Update sources of this object
        for study in patient.studies:
            patient.sources = [a or b for a, b in zip(patient.sources, study.sources)]
        # If this object is not empty, return
        if any(study.sources):
            return
        sess.delete(study)
        sess.commit()
    else:
        patient.min_timestamp = patient.max_timestamp = datetime.utcnow()


def expand_patient(sess, args, source, patient):
    source_objects = source.studies(patient)    # patient_ids = [patient['PatientId'] for patient in patients]
    if patient.is_new:
        metadata = []
        for study in source_objects:
            metadata.append(
                dict(
                    submitter_case_id = patient.submitter_case_id,
                    init_idc_version=args.version,
                    rev_idc_version=args.version,
                    study_instance_uid=study,
                    uuid = uuid4(),
                    sources=(False, False),
                    hashes=("", ""),
                    revised=False,
                    done=False,
                    is_new=True,
                    expanded=False
                )
            )
        sess.bulk_insert_mappings(Study, metadata)
    else:
        idc_objects = {object.study_instance_uid: object for object in patient.studies}

        new_objects = [id for id in source_objects if id not in idc_objects]
        retired_objects = [idc_objects[id] for id in idc_objects if id not in source_objects]
        existing_objects = [idc_objects[id] for id in source_objects if id in idc_objects]

        for study in retired_objects:
            retire_study(sess, args, study, instance_source['path'].value)

        for study in existing_objects:
            if source.study_hashes_differ(study):
                study.revised = False
                study.done = False
                study.is_new = False
                study.expanded = False


        metadata = []
        for study in new_objects:
            metadata.append(
                dict(
                    submitter_case_id = patient.submitter_case_id,
                    init_idc_version=args.version,
                    rev_idc_version=args.version,
                    study_instance_uid = study,
                    uuid = uuid4(),
                    sources = (False,False),
                    hashes = ("",""),
                    revised = False,
                    done = False,
                    is_new = True,
                    expanded = False
                )
            )
        sess.bulk_insert_mappings(Study, metadata)
    patient.expanded = True
    sess.commit()
    # rootlogger.debug("  p%s: Expanded patient %s",args.id, patient.submitter_case_id)



def build_patient(sess, args, source, patient_index, data_collection_doi, analysis_collection_dois, version, collection, patient):
    if not patient.done:
        begin = time.time()
        if not patient.expanded:
            expand_patient(sess, args, source, patient)
        rootlogger.info("  p%s: Patient %s, %s, %s studies, expand_time: %s, %s", args.id, patient.submitter_case_id, patient_index, len(patient.studies), time.time()-begin, time.asctime())
        for study in patient.studies:
            study_index = f'{patient.studies.index(study) + 1} of {len(patient.studies)}'
            build_study(sess, args, source, study_index, version, collection, patient, study, data_collection_doi, analysis_collection_dois)
        if all([study.done for study in patient.studies]):
            patient.min_timestamp = min([study.min_timestamp for study in patient.studies if study.min_timestamp != None])
            patient.max_timestamp = max([study.max_timestamp for study in patient.studies if study.max_timestamp != None])

            # Get hash of children
            hash = source.idc_patient_hash(patient)
            # Test whether anything has changed
            if hash != patient.hashes[source.source_id]:
                hashes = list(patient.hashes)
                hashes[source.source_id] = hash
                patient.hashes = hashes

                patient.sources = accum_sources(patient, patient.studies)
                patient.rev_idc_version = max(study.rev_idc_version for study in patient.studies)

                if not patient.is_new:
                    patient.revised = True

            patient.done = True
            sess.commit()
            duration = str(timedelta(seconds=(time.time() - begin)))
            rootlogger.info("  p%s: Patient %s, %s, completed in %s, %s", args.id, patient.submitter_case_id, patient_index, duration, time.asctime())
    else:
        rootlogger.info("  p%s: Patient %s, %s, previously built", args.id, patient.submitter_case_id, patient_index)


def worker(input, output, args, data_collection_doi, analysis_collection_dois):
    # rootlogger.debug('p%s: Worker starting: args: %s', args.id, args)
    with Session(args.sql_engine) as sess:
        source = (args.source)(sess)
        for more_args in iter(input.get, 'STOP'):
            for attempt in range(PATIENT_TRIES):
                # index, idc_version_number, collection_id, submitter_case_id = more_args
                index, collection_id, submitter_case_id = more_args
                try:
                    # version = sess.query(Version).filter_by(idc_version_number=idc_version_number).one()
                    version = sess.query(Version).one()
                    # collection = next(collection for collection in version.collections if collection.collection_id==collection_id)
                    collection = sess.query(Collection).where(Collection.collection_id==collection_id).one()
                    patient = next(patient for patient in collection.patients if patient.submitter_case_id==submitter_case_id)
                    # rootlogger.debug("p%s: In worker, sess: %s, submitter_case_id: %s", args.id, sess, submitter_case_id)
                    build_patient(sess, args, source, index, data_collection_doi, analysis_collection_dois, version, collection, patient)
                    break
                except Exception as exc:
                    errlogger.error("p%s, exception %s; reattempt %s on patient %s/%s, %s; %s", args.id, exc, attempt, collection.collection_id, patient.submitter_case_id, index, time.asctime())
                    sess.rollback()

            if attempt == PATIENT_TRIES-1:
                errlogger.error("p%s, Failed to process patient: %s", args.id, patient.submitter_case_id)
                sess.rollback()
            output.put(patient.submitter_case_id)




def retire_collection(sess, args, collection, source):
    # If this object has children from source, delete them
    if collection.sources[source]:
        for patient in collection.patients:
            retire_patient(sess, args, patient, source)
        collection.sources[source] = False
        # Update sources of this object
        for patient in collection.patients:
            collection.sources = [a or b for a, b in zip(collection.sources, patient.sources)]
        # If this object is not empty, return
        if any(collection.sources):
            return
        sess.delete(collection)
        sess.commit()
    else:
        collection.min_timestamp = collection.max_timestamp = datetime.utcnow()


def expand_collection(sess, args, source, collection):
    # Since we are starting, delete everything from the prestaging bucket.
    rootlogger.info("Emptying prestaging bucket")
    begin = time.time()
    create_prestaging_bucket(args)
    empty_bucket(args.prestaging_bucket)
     # Since we are starting, delete everything from the prestaging bucket.
    duration = str(timedelta(seconds=(time.time() - begin)))
    rootlogger.info("Emptying prestaging bucket completed in %s", duration)

    source_objects = source.patients(collection)
    # Check for duplicates
    if len(source_objects) != len(set(source_objects)):
        errlogger.error("\tp%s: Duplicate patients in expansion of collection %s", args.id,
                        collection.collection_id)
        raise RuntimeError("p%s: Duplicate patients expansion of collection %s", args.id,
                           collection.collection_id)

    if collection.is_new:
        # ...then all its children are new
        metadata = []
        for patient in source_objects:
            metadata.append(
                dict(
                    collection_id = collection.collection_id,
                    init_idc_version=args.version,
                    rev_idc_version=args.version,
                    submitter_case_id=patient,
                    idc_case_id = uuid4(),
                    sources = [False, False],
                    hashes = ["",""],
                    revised=False,
                    done=False,
                    is_new=True,
                    expanded=False
                )
            )
        sess.bulk_insert_mappings(Patient, metadata)
    else:
        idc_objects = {object.submitter_case_id: object for object in collection.patients}

        new_objects = [id for id in source_objects if id not in idc_objects]
        retired_objects = [idc_objects[id] for id in idc_objects if id not in source_objects]
        existing_objects = [idc_objects[id] for id in idc_objects if id in source_objects]

        for patient in retired_objects:
            retire_patient(sess, args, patient, instance_source['path'].value)

        for patient in existing_objects:
            if source.patient_hashes_differ(patient):
                patient.revised = False
                patient.done = False
                patient.is_new = False
                patient.expanded = False


        metadata = []
        for patient in new_objects:
            metadata.append(
                dict(
                    collection_id = collection.collection_id,
                    init_idc_version=args.version,
                    rev_idc_version=args.version,
                    submitter_case_id = patient,
                    idc_case_id = uuid4(),
                    sources = [False, False],
                    hashes = ["",""],
                    revised = False,
                    done = False,
                    is_new = True,
                    expanded = False
                )
            )
        sess.bulk_insert_mappings(Patient, metadata)
    collection.expanded = True
    sess.commit()
    # rootlogger.debug("p%s: Expanded collection %s",args.id, collection.collection_id)


def build_collection(sess, args, source, collection_index, version, collection):
    begin = time.time()
    args.prestaging_bucket = f"{args.prestaging_bucket_prefix}{collection.collection_id.lower().replace(' ','_').replace('-','_')}"
    if not collection.expanded:
        expand_collection(sess, args, source, collection)
    rootlogger.info("Collection %s, %s, %s patients", collection.collection_id, collection_index, len(collection.patients))
    # Get the lists of data and analyis series in this patient
    data_collection_doi = get_data_collection_doi(collection.collection_id, server=args.server)
    if data_collection_doi=="" and collection.collection_id=='NLST':
        data_collection_doi = '10.7937/TCIA.hmq8-j677'
    pre_analysis_collection_dois = get_analysis_collection_dois(collection.collection_id, server=args.server)
    analysis_collection_dois = {x['SeriesInstanceUID']: x['SourceDOI'] for x in pre_analysis_collection_dois}

    if args.num_processes==0:
        # for series in sorted_seriess:
        args.id = 0
        for patient in collection.patients:
            patient_index = f'{collection.patients.index(patient) + 1} of {len(collection.patients)}'
            if not patient.done:
                build_patient(sess, args, source, patient_index, data_collection_doi, analysis_collection_dois, version, collection, patient)
            else:
                if (collection.patients.index(patient) % 100 ) == 0:
                    rootlogger.info("  p%s: Patient %s, %s, previously built", args.id, patient.submitter_case_id,
                                patient_index)

    else:
        processes = []
        # Create queues
        task_queue = Queue()
        done_queue = Queue()

        # List of patients enqueued
        enqueued_patients = []

        # Start worker processes
        for process in range(args.num_processes):
            args.id = process+1
            processes.append(
                Process(target=worker, args=(task_queue, done_queue, args, data_collection_doi, analysis_collection_dois )))
            processes[-1].start()

        # Enqueue each patient in the the task queue
        args.id = 0
        for patient in collection.patients:
            patient_index = f'{collection.patients.index(patient) + 1} of {len(collection.patients)}'
            if not patient.done:
                # task_queue.put((patient_index, version.idc_version_number, collection.collection_id, patient.submitter_case_id))
                task_queue.put((patient_index, collection.collection_id, patient.submitter_case_id))
                enqueued_patients.append(patient.submitter_case_id)
            else:
                if (collection.patients.index(patient) % 100 ) == 0:
                    rootlogger.info("  p%s: Patient %s, %s, previously built", args.id, patient.submitter_case_id,
                                patient_index)

        # Collect the results for each patient
        try:
            while not enqueued_patients == []:
                # Timeout if waiting too long
                results = done_queue.get(True)
                enqueued_patients.remove(results)

            # Tell child processes to stop
            for process in processes:
                task_queue.put('STOP')

            # Wait for them to stop
            for process in processes:
                process.join()


        except Empty as e:
            errlogger.error("Timeout in build_collection %s", collection.collection_id)
            for process in processes:
                process.terminate()
                process.join()
            sess.rollback()
            duration = str(timedelta(seconds=(time.time() - begin)))
            rootlogger.info("Collection %s, %s, NOT completed in %s", collection.collection_id, collection_index,
                            duration)

    if all([patient.done for patient in collection.patients]):
        collection.min_timestamp = min([patient.min_timestamp for patient in collection.patients if patient.min_timestamp != None])
        collection.max_timestamp = max([patient.max_timestamp for patient in collection.patients if patient.max_timestamp != None])

        # Get hash of children
        hash = source.idc_collection_hash(collection)
        # Test whether anything has changed
        if hash != collection.hashes[source.source_id]:
            hashes = list(collection.hashes)
            hashes[source.source_id] = hash
            collection.hashes = hashes
            collection.sources = accum_sources(collection, collection.patients)

            collection.rev_idc_version = max(patient.rev_idc_version for patient in collection.patients)

            if collection.is_new:
                collection.revised = True

        collection.done = True
        sess.commit()
        duration = str(timedelta(seconds=(time.time() - begin)))
        rootlogger.info("Collection %s, %s, completed in %s", collection.collection_id, collection_index,
                        duration)
    else:
        duration = str(timedelta(seconds=(time.time() - begin)))
        rootlogger.info("Collection %s, %s, not completed in %s", collection.collection_id, collection_index,
                        duration)



def expand_version(sess, args, source, version):
    # If we are here, we are beginning work on this version.
    try:
        todos = open(args.todos).read().splitlines()
    except:
        todos = []

    idc_objects_results = sess.query(Collection)
    idc_objects = {c.collection_id.lower():c for c in idc_objects_results}

    source_objects = source.collections()

    new_objects = [collection_id for collection_id in source_objects if collection_id not in idc_objects]
    retired_objects = [idc_objects[collection_id] for collection_id in idc_objects if collection_id not in source_objects]
    existing_objects = [idc_objects[collection_id] for collection_id in source_objects if collection_id in idc_objects]

    for collection in retired_objects:
        if  collection.collection_id in todos:
            retire_collection(sess, args, collection, instance_source['path'].value)

    for collection in existing_objects:
        if collection.collection_id in todos:
            if source.collection_hashes_differ(collection):
                collection.revised = False
                collection.done = False
                collection.is_new = False
                collection.expanded = False


    collection_data = []
    for collection in new_objects:
        if collection.collection_id in todos:
            collection_data.append(
                dict(
                    init_idc_version=args.version,
                    rev_idc_version=args.version,
                    collection_id = collection.collection_id,
                    sources = (False,False),
                    hashes = ("",""),
                    revised = False,
                    done = False,
                    is_new = True,
                    expanded = False,
                )
            )
    sess.bulk_insert_mappings(Collection, collection_data)
    version.expanded = True
    sess.commit()
    rootlogger.info("Expanded version")

def build_version(sess, args, source, version):
    # Session = sessionmaker(bind= sql_engine)
    # version = version_is_done(sess, args.version)
    begin = time.time()
    if not version.expanded:
        expand_version(sess, args, source, version)
    idc_collections = [c for c in sess.query(Collection)]
    rootlogger.info("Version %s; %s collections", args.version, len(idc_collections))
    # try:
    #     skips = open(args.skips).read().splitlines()
    # except:
    #     skips = []
    for collection in idc_collections:
        # if not collection.collection_id in skips:
        if True:
            collection_index = f'{idc_collections.index(collection) + 1} of {len(idc_collections)}'
            if not collection.done:
                build_collection(sess, args, source, collection_index, version, collection)
            else:
                rootlogger.info("Collection %s, %s, previously built", collection.collection_id, collection_index)

    # Check if we are really done
    # if all([collection.done for collection in idc_collections if not collection.collection_id in skips]):
    if all([collection.done for collection in idc_collections]):

        hash = source.idc_version_hash()
        duration = str(timedelta(seconds=(time.time() - begin)))
        # Check whether the hash has changed. If so then declare this a new version
        # otherwise revert the version number
        if hash != version.hashes[source.source_id]:
            hashes = list(version.hashes)
            hashes[source.source_id] = hash
            version.hashes = hashes
            version.min_timestamp = min([collection.min_timestamp for collection in idc_collections if collection.min_timestamp != None])
            version.max_timestamp = max([collection.max_timestamp for collection in idc_collections if collection.max_timestamp != None])
            version.done = True
            version.revised = True
            rootlogger.info("Built new %s version %s in %s", source.source.name, version.version, duration)
        else:
            version.versions[source.source_id] = args.version-1
            rootlogger.info("Version unchanged, remains at  %s in %s", version.version-1, duration)
        sess.commit()
    else:
        rootlogger.info("Not all collections are done. Rerun.")


def prebuild(args):
    # rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/path_ingest_v{}_log.log'.format(os.environ['PWD'], args.version))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(DEBUG)

    # errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/path_ingest_v{}_err.log'.format(os.environ['PWD'], args.version))
    errformatter = logging.Formatter('{%(pathname)s:%(lineno)d} %(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    rootlogger.debug('Args: %s', args)

    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{args.db}'
    sql_engine = create_engine(sql_uri, echo=True)
    # sql_engine = create_engine(sql_uri)
    args.sql_engine = sql_engine

    declarative_base().metadata.create_all(sql_engine)

    # Create a local working directory
    if os.path.isdir('{}'.format(args.dicom)):
        shutil.rmtree('{}'.format(args.dicom))
    os.mkdir('{}'.format(args.dicom))


    # Add a new Version with idc_version_number args.version, if it does not already exist
    with Session(sql_engine) as sess:


        source = (args.source)(sess)

        stmt = select(Version).distinct()
        result = sess.execute(stmt)
        version = result.fetchone()[0]

        if version.version != args.version:
            #
            version.expanded=False
            version.done=False
            version.is_new=True
            version.revised=False
            # versions = list(version.versions[0:])
            # versions[source.source_id] = args.version
            version.version = args.version

            sess.commit()
        if not version.done:
            build_version(sess, args, source, version)
        else:
            rootlogger.info("    version %s previously built", args.version)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=3, help='Version to work on')
    parser.add_argument('--client', default=storage.Client())
    args = parser.parse_args()
    parser.add_argument('--db', default=f'idc_path_v{args.version}.1', help='Database on which to operate')
    parser.add_argument('--project', default='idc-dev-etl')
    parser.add_argument('--src_bucket', default=storage.Bucket(args.client,'af-dac-wsi-conversion-results'))
    parser.add_argument('--prestaging_bucket_prefix', default=f'idc_v3_', help='Copy instances here before forwarding to --staging_bucket')
    parser.add_argument('--num_processes', default=8, help="Number of concurrent processes")
    parser.add_argument('--todos', default='{}/logs/path_ingest_v{}_todos.txt'.format(os.environ['PWD'], args.version ), help="Collections to include")
    parser.add_argument('--source', default=Pathology, help="Source from which to ingest")
    parser.add_argument('--server', default="", help="NBIA server to access")
    parser.add_argument('--dicom', default='/mnt/disks/idc-etl/dicom', help='Directory in which to expand downloaded zip files')
    args = parser.parse_args()

    print("{}".format(args), file=sys.stdout)

    rootlogger = logging.getLogger('root')
    # root_fh = logging.FileHandler('{}/logs/path_ingest_v{}_log.log'.format(os.environ['PWD'], args.version))
    # rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    # rootlogger.addHandler(root_fh)
    # root_fh.setFormatter(rootformatter)
    # rootlogger.setLevel(DEBUG)

    errlogger = logging.getLogger('root.err')
    # err_fh = logging.FileHandler('{}/logs/path_ingest_v{}_err.log'.format(os.environ['PWD'], args.version))
    # errformatter = logging.Formatter('{%(pathname)s:%(lineno)d} %(levelname)s:err:%(message)s')
    # errlogger.addHandler(err_fh)
    # err_fh.setFormatter(errformatter)
    #
    # rootlogger.debug('Args: %s', args)
    prebuild(args)
