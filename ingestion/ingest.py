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
from multiprocessing import Process, Queue, Lock
from queue import Empty
from base64 import b64decode
from pydicom.errors import InvalidDicomError
from uuid import uuid4
from google.cloud import storage
from idc.models import Version, Collection, Patient, Study, Series, Instance, Retired, WSI_metadata, instance_source
from sqlalchemy import select,delete
from sqlalchemy.orm import Session
from utilities.tcia_helpers import  get_hash, get_TCIA_studies_per_patient, get_TCIA_patients_per_collection,\
    get_TCIA_series_per_study, get_TCIA_instance_uids_per_series, get_TCIA_instances_per_series, \
    get_collection_values_and_counts, get_TCIA_instances_per_series_with_hashes
from utilities.get_collection_dois import get_data_collection_doi, get_analysis_collection_dois
from google.api_core.exceptions import Conflict

from python_settings import settings
import settings as etl_settings

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import register_composites

from ingestion.sources import TCIA, Pathology

rootlogger = logging.getLogger('root')
errlogger = logging.getLogger('root.err')

PATIENT_TRIES=5


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

# Validate that instances were received correctly
def validate_hashes(args, collection, patient, study, series, hashes):
    for instance in hashes:
        instance = instance.split(',')
        if md5_hasher(f'{args.dicom}/{series.series_instance_uid}/{instance[0]}') != instance[1]:
            errlogger.error("      p%s: Invalid hash for %s/%s/%s/%s", args.id,
            collection.collection_id, patient.submitter_case_id, study.study_instance_uid, series.series_instance_uid,\
            instance[0])
            return False
    return True


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
        result = run(["gsutil", "-m", "-q", "cp", src, dst], check=True)
        if result.returncode :
            errlogger.error('p%s: \tcopy_disk_to_prestaging_bucket failed for series %s', args.id, series.series_instance_uid)
            raise RuntimeError('p%s: copy_disk_to_prestaging_bucket failed for series %s', args.id, series.series_instance_uid)
        # rootlogger.debug("p%s: Uploaded instances to GCS", args.id)
    except Exception as exc:
        errlogger.error("\tp%s: Copy to prestage bucket failed for series %s", args.id, series.series_instance_uid)
        raise RuntimeError("p%s: Copy to prestage bucketfailed for series %s", args.id, series.series_instance_uid) from exc


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


# Copy an instance from a source bucket to a destination bucket. Currently used when ingesting pathology
# which is placed in some bucket after conversion from svs, etc. to DICOM WSI format
def copy_gcs_to_gcs(args, instance, gcs_url):
    storage_client = storage.Client(project=args.project)
    wsi_src_bucket = args.wsi_src_bucket
    dst_bucket = storage_client.bucket((args.prestaging_bucket))
    src_blob = wsi_src_bucket.blob(gcs_url)
    dst_blob = dst_bucket.blob(f'{instance.uuid}.dcm')
    token, bytes_rewritten, total_bytes = dst_blob.rewrite(src_blob)
    while token:
        rootlogger.debug('******p%s: Rewrite bytes_rewritten %s, total_bytes %s', bytes_rewritten, total_bytes)
        token, bytes_rewritten, total_bytes = dst_blob.rewrite(src_blob, token=token)


def accum_sources(parent, children):
    sources = list(parent.sources)
    for child in children:
        sources = [x | y for (x, y) in zip(sources, child.sources)]
    return sources


def retire_instance(sess, args, instance, source):
    instance_data = []
    # Deletion is performed on a source: TCIA, Pathology
    # Only delete this instance if it is from the specified source
    if instance.source.value == source.source_id:
        # Record info about retired instances in the Retired table.
        # This is useful in creating the DICOM store.
        instance_data.append(
            dict(
                timestamp=instance.timestamp,
                sop_instance_uid=instance.sop_instance_uid,
                source=instance.source,
                hash=instance.hash,
                size=instance.size,
                init_idc_version=instance.init_idc_version,
                rev_idc_version=instance.rev_idc_version,
                series_instance_uid=instance.series.series_instance_uid,
                study_instance_uid=instance.series.study.study_instance_uid,
                submitter_case_id=instance.series.study.patient.submitter_case_id,
                collection_id=instance.series.study.patient.collection.collection_id,
                instance_uuid=instance.uuid,
                series_uuid=instance.series.uuid,
                study_uuid=instance.series.study.uuid,
                idc_case_id=instance.series.study.patient.submitter_case_id,
                source_doi=instance.series.source_doi
            )
        )
        sess.bulk_insert_mappings(Retired, instance_data)


        # sess.delete(instance)
        # sess.commit()
    else:
        instance.min_timestamp = instance.max_timestamp = datetime.utcnow()

def build_some_instances_tcia(sess, args, source, version, collection, patient, study, series):
    # Do a download for each instance in a series that is not done.
    # It will be write the zip to a file dicom/<series_instance_uid>.zip in the
    # working directory, and expand the zip to directory dicom/<series_instance_uid>
    pass


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
    hashes = get_TCIA_instances_per_series_with_hashes(args.dicom, series.series_instance_uid)
    download_time = (time.time_ns() - download_start)/10**9
    if not validate_hashes(args, collection, patient, study, series, hashes):
        return
    # rootlogger.debug("      p%s: Series %s, download time: %s", args.id, series.series_instance_uid, (time.time_ns() - download_start)/10**9)
    # rootlogger.debug("      p%s: Series %s, downloading instance data; %s", args.id, series.series_instance_uid, time.asctime())

    # Get a list of the files from the download
    dcms = [dcm for dcm in os.listdir("{}/{}".format(args.dicom, series.series_instance_uid))]

    # if 'LICENSE' in dcms:
    #     os.remove("{}/{}/LICENSE".format(args.dicom, series.series_instance_uid))
    #     dcms.remove('LICENSE')
    #

    # Ensure that the zip has the expected number of instances
    if not len(dcms) == len(series.instances):
        errlogger.error("      p%s: Invalid zip file for %s/%s/%s/%s", args.id,
            collection.collection_id, patient.submitter_case_id, study.study_instance_uid, series.series_instance_uid)
        # Return without marking all instances done. This will prevent the series from being done.
        return
        # raise RuntimeError("      \p%s: Invalid zip file for %s/%s/%s/%s", args.id,
        #     collection.collection_id, patient.submitter_case_id, study.study_instance_uid, series.series_instance_uid)

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
        # If an instance is already done, don't need to do anything more
        if instance.done:
            # Delete file. We already have it.
            os.remove("{}/{}/{}".format(args.dicom, series.series_instance_uid, dcm))
            rootlogger.debug("      p%s: Instance %s previously done, ", args.id, series.series_instance_uid)

            continue
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
    if series.sources[source.source_id]:
        for instance in series.instances:
            retire_instance(sess, args, instance, source)
            sess.delete(instance)
        series_sources = list(series.sources)
        series_sources[source.source_id] = False
        series.sources = series_sources
        # Update sources of this object
        # for instance in series.instances:
        #     series.sources = [a or b for a, b in zip(series.sources, instance.sources)]
        # If this object is not empty, return
    else:
        series.min_timestamp = series.max_timestamp = datetime.utcnow()


def expand_series(sess, args, source, series):
    # not_done = 0
    # done = 0
    source_objects = source.instances(series)
    if len(source_objects) != len(set(source_objects)):
        errlogger.error("\tp%s: Duplicate instance in expansion of series %s", args.id,
                        series.series_instance_uid)
        raise RuntimeError("p%s: Duplicate instance in  expansion of series %s", args.id,
                           series.series_instance_uid)

    if series.is_new:
        metadata = []
        for instance in source_objects:
            # not_done += 1
            metadata.append(
                dict(
                    series_instance_uid = series.series_instance_uid,
                    min_timestamp = datetime.utcnow(),
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
        idc_objects = {object.sop_instance_uid: object for object in series.instances}

        new_objects = [id for id in source_objects if id not in idc_objects]
        retired_objects = [idc_objects[id] for id in idc_objects if id not in source_objects]
        existing_objects = [idc_objects[id] for id in source_objects if id in idc_objects]

        for instance in retired_objects:
            retire_instance(sess, args, instance, source)
            if instance.source == source.source_id:
                sess.delete(instance)

        for instance in existing_objects:
            differ = source.instance_hashes_differ(instance)
            if differ == 1:
                # not_done += 1
                rootlogger.debug('**Instance %s needs revision', instance.sop_instance_uid)
                # We are replacing the current version of this instance, so retire it.
                # Note we are effectively deleting the instance from the instance table
                # and replaceing it with a new instance having the same sop_instance_uid
                # but a different uuid. Instead we just replace the uuid and init the status
                # bits.
                retire_instance(sess, args, instance, source)
                # Stamp this instance, showing when it was checked
                instance.timestamp = datetime.utcnow()
                instance.uuid = uuid4()
                instance.rev_idc_version = args.version
                instance.revised = True
                instance.done = False
                instance.is_new = False
                instance.expanded = True
                instance.hash = ""
                instance.size = 0
            elif differ == 0:
                # done += 1
                # Stamp this instance, showing when it was checked
                instance.timestamp = datetime.utcnow()
                instance.done = True
                instance.expanded = True
                rootlogger.debug('**Instance %s unchanged', instance.sop_instance_uid)
            else:
                errlogger.error("Can't get hash for %s/%s/%s/%s/%s", \
                    instance.series_instance_uid.study_instance_uid.submitter_case_id.collection_id, \
                    instance.series_instance_uid.study_instance_uid.submitter_case_id, \
                    instance.series_instance_uid.study_instance_uid, \
                    instance.series_instance_uid, \
                    instance.sop_instance_uid
                    )
                # Bailout without completing expansion.
                return 1

        metadata = []

        for instance in new_objects:
            # not_done += 1
            metadata.append(
                dict(
                    series_instance_uid = series.series_instance_uid,
                    min_timestamp = datetime.utcnow(),
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
    return 0
    # rootlogger.debug("      p%s: Expanded series %s", args.id, series.series_instance_uid)


def build_series(sess, args, source, series_index, version, collection, patient, study, series):
    begin = time.time()
    if not series.expanded:
        failed = expand_series(sess, args, source, series)
        if failed:
            return
    rootlogger.info("      p%s: Series %s; %s; %s instances, expand: %s", args.id, series.series_instance_uid, series_index, len(series.instances), time.time()-begin)

    if not all(instance.done for instance in series.instances):
        if source.source_id == instance_source.tcia.value:
            # # Different paths depending on whether we are doing all instances or just some
            # if not_done == instances:
            #     # If all instances are not done
            #     build_all_instances_tcia(sess, args, source, version, collection, patient, study, series)
            # elif done:
            #     # if some, but not all instances, are done
            #     build_some_instances_tcia(sess, args, source, version, collection, patient, study, series)
            build_instances_tcia(sess, args, source, version, collection, patient, study, series)

        else:
            # Get instance data from path DB table/ GCS bucket.
            build_instances_path(sess, args, source, version, collection, patient, study, series)

    if all(instance.done for instance in series.instances):
        # series.min_timestamp = min(instance.timestamp for instance in series.instances)
        series.max_timestamp = max(instance.timestamp for instance in series.instances)

        # Get hash of children in the DB
        hash = source.idc_series_hash(series)
        # It should now match the source's (tcia's, path's,...) hash
        if source.src_series_hash(series.series_instance_uid) != hash:
            # errlogger.error('Hash match failed for series %s', series.series_instance_uid)
            raise Exception('Hash match failed for series %s', series.series_instance_uid)
        else:
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
                series.rev_idc_version = args.version

                if not series.is_new:
                    # This series was revised. Give it a new uuid.
                    series.revised = True
                    series.uuid = uuid4()
            else:
                rootlogger.info("      p%s: Series %s, %s, unchanged", args.id, series.series_instance_uid,
                                series_index)

            series.done = True
            sess.commit()
            duration = str(timedelta(seconds=(time.time() - begin)))
            rootlogger.info("      p%s: Series %s, %s, completed in %s", args.id, series.series_instance_uid, series_index, duration)


def retire_study(sess, args, study, source):
    # If this object has children from source, delete them
    if study.sources[source.source_id]:
        for series in study.seriess:
            retire_series(sess, args, series, source)
        study_sources = list(study.sources)
        study_sources[source.source_id] = False
        study.sources = study_sources
        # Update sources of this object
        # for series in study.seriess:
        #     study.sources = [a or b for a, b in zip(study.sources, series.sources)]
        # If this object is not empty, return
        # if any(study.sources):
        #     return
        # sess.delete(study)
        # sess.commit()
    else:
        study.min_timestamp = study.max_timestamp = datetime.utcnow()


def expand_study(sess, args, source, study, data_collection_doi, analysis_collection_dois):
    source_objects = source.series(study)
    if len(source_objects) != len(set(source_objects)):
        errlogger.error("\tp%s: Duplicate series in expansion of study %s", args.id,
                        study.study_instance_uid)
        raise RuntimeError("p%s: Duplicate series expansion of study %s", args.id,
                           study.study_instance_uid)

    if study.is_new:
        metadata = []
        for series in source_objects:
            metadata.append(
                dict(
                    study_instance_uid = study.study_instance_uid,
                    min_timestamp = datetime.utcnow(),
                    init_idc_version=args.version,
                    rev_idc_version=args.version,
                    series_instance_uid=series,
                    uuid = uuid4(),
                    source_doi=analysis_collection_dois[series] \
                        if series in analysis_collection_dois \
                        else data_collection_doi,
                    sources = (False, False),
                    hashes = ("", "", ""),
                    revised=False,
                    done=False,
                    is_new=True,
                    expanded=False
                )
            )
        sess.bulk_insert_mappings(Series, metadata)
    else:
        idc_objects = {object.series_instance_uid: object for object in study.seriess}

        new_objects = [id for id in source_objects if id not in idc_objects]
        retired_objects = [idc_objects[id] for id in idc_objects if id not in source_objects]
        existing_objects = [idc_objects[id] for id in source_objects if id in idc_objects]

        for series in retired_objects:
            retire_series(sess, args, series, source)
            # if the series does not include instances from an source, delete it
            if not any(series.sources):
                sess.delete(series)

        for series in existing_objects:
            if source.series_was_updated(series):
                rootlogger.debug('**Series %s needs revision', series.series_instance_uid)
                # Mark when we started work on this series
                series.min_timestamp = datetime.utcnow()
                series.revised = False
                series.done = False
                series.is_new = False
                series.expanded = False
            else:
                # Stamp this series showing when it was checked
                series.min_timestamp = datetime.utcnow()
                series.max_timestamp = datetime.utcnow()
                series.done = True
                series.expanded = True
                rootlogger.debug('Series %s unchanged', series.series_instance_uid)

        metadata = []
        for series in new_objects:
            metadata.append(
                dict(
                    study_instance_uid = study.study_instance_uid,
                    min_timestamp = datetime.utcnow(),
                    init_idc_version=args.version,
                    rev_idc_version=args.version,
                    series_instance_uid = series,
                    uuid = uuid4(),
                    source_doi=analysis_collection_dois[series] \
                        if series in analysis_collection_dois \
                        else data_collection_doi,
                    sources = (False, False),
                    hashes = ("", "", ""),
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
    begin = time.time()
    if not study.expanded:
        expand_study(sess, args, source, study, data_collection_doi, analysis_collection_dois)
    rootlogger.info("    p%s: Study %s, %s, %s series, expand time: %s", args.id, study.study_instance_uid, study_index, len(study.seriess), time.time()-begin)
    for series in study.seriess:
        series_index = f'{study.seriess.index(series) + 1} of {len(study.seriess)}'
        if not series.done:
            build_series(sess, args, source, series_index, version, collection, patient, study, series)
        else:
            rootlogger.info("      p%s: Series %s, %s, previously built", args.id, series.series_instance_uid, series_index)

    if all([series.done for series in study.seriess]):
        # study.min_timestamp = min([series.min_timestamp for series in study.seriess if series.min_timestamp != None])
        study.max_timestamp = max([series.max_timestamp for series in study.seriess if series.max_timestamp != None])

        # Get hash of children
        hash = source.idc_study_hash(study)
        if source.src_study_hash(study.study_instance_uid) != hash:
            # errlogger.error('Hash match failed for study %s', study.study_instance_uid)
            raise Exception('Hash match failed for study %s', study.study_instance_uid)
        else:
            # Test whether anything has changed
            if hash != study.hashes[source.source_id]:
                hashes = list(study.hashes)
                hashes[source.source_id] = hash
                study.hashes = hashes

                study.sources = accum_sources(study, study.seriess)
                study.study_instances = sum([series.series_instances for series in study.seriess])
                study.rev_idc_version = args.version

                if not study.is_new:
                    study.revised = True
                    study.uuid = uuid4()
            else:
                rootlogger.info("    p%s: Study %s, %s, unchanged", args.id, study.study_instance_uid,
                                study_index)

            study.done = True
            sess.commit()
            duration = str(timedelta(seconds=(time.time() - begin)))
            rootlogger.info("    p%s: Study %s, %s,  completed in %s", args.id, study.study_instance_uid, study_index, duration)


def retire_patient(sess, args, patient, source):
    # If this object has children from source, delete them
    if patient.sources[source.source_id]:
        for study in patient.studies:
            retire_study(sess, args, study, source)
        patient_sources = list(patient.sources)
        patient_sources[source.source_id] = False
        patient.sources = patient_sources
        # Update sources of this object
        # for study in patient.studies:
        #     patient.sources = [a or b for a, b in zip(patient.sources, study.sources)]
        # If this object is not empty, return
        # if any(study.sources):
        #     return
        # sess.delete(study)
        # sess.commit()
    else:
        patient.min_timestamp = patient.max_timestamp = datetime.utcnow()


def expand_patient(sess, args, source, patient):
    source_objects = source.studies(patient)    # patient_ids = [patient['PatientId'] for patient in patients]
    if len(source_objects) != len(set(source_objects)):
        errlogger.error("\tp%s: Duplicate studies in expansion of patient %s", args.id,
                        patient.submitter_case_id)
        raise RuntimeError("p%s: Duplicate studies expansion of collection %s", args.id,
                           patient.submitter_case_i)

    if patient.is_new:
        metadata = []
        for study in source_objects:
            metadata.append(
                dict(
                    submitter_case_id = patient.submitter_case_id,
                    min_timestamp = datetime.utcnow(),
                    init_idc_version=args.version,
                    rev_idc_version=args.version,
                    study_instance_uid=study,
                    uuid = uuid4(),
                    sources=(False, False),
                    hashes=("", "", ""),
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
            retire_study(sess, args, study, source)
            if not any(study.sources):
                sess.delete(study)

        for study in existing_objects:
            if source.study_was_updated(study):
                rootlogger.debug('**Study %s needs revision', study.study_instance_uid)
                # Mark when we started work on this study
                study.min_timestamp = datetime.utcnow()
                study.revised = False
                study.done = False
                study.is_new = False
                study.expanded = False
            else:
                # Stamp this study showing when it was checked
                study.min_timestamp = datetime.utcnow()
                study.max_timestamp = datetime.utcnow()
                study.done = True
                study.expanded = True
                rootlogger.debug('**Study %s unchanged', study.study_instance_uid)
        metadata = []
        for study in new_objects:
            metadata.append(
                dict(
                    submitter_case_id = patient.submitter_case_id,
                    min_timestamp = datetime.utcnow(),
                    init_idc_version=args.version,
                    rev_idc_version=args.version,
                    study_instance_uid = study,
                    uuid = uuid4(),
                    sources = (False, False),
                    hashes = ("", "", ""),
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
    begin = time.time()
    if not patient.expanded:
        expand_patient(sess, args, source, patient)
    rootlogger.info("  p%s: Patient %s, %s, %s studies, expand_time: %s, %s", args.id, patient.submitter_case_id, patient_index, len(patient.studies), time.time()-begin, time.asctime())
    for study in patient.studies:
        study_index = f'{patient.studies.index(study) + 1} of {len(patient.studies)}'
        if not study.done:
            build_study(sess, args, source, study_index, version, collection, patient, study, data_collection_doi, analysis_collection_dois)
        else:
            rootlogger.info("    p%s: Study %s, %s, previously built", args.id, study.study_instance_uid, study_index)
    if all([study.done for study in patient.studies]):
        # patient.min_timestamp = min([study.min_timestamp for study in patient.studies if study.min_timestamp != None])
        patient.max_timestamp = max([study.max_timestamp for study in patient.studies if study.max_timestamp != None])

        # Get hash of children
        hash = source.idc_patient_hash(patient)
        if source.src_patient_hash(collection.collection_id, patient.submitter_case_id) != hash:
            # errlogger.error('Hash match failed for patient %s', patient.submitter_case_id)
            raise Exception('Hash match failed for patient %s', patient.submitter_case_id)
        else:
            # Test whether anything has changed
            if hash != patient.hashes[source.source_id]:
                hashes = list(patient.hashes)
                hashes[source.source_id] = hash
                patient.hashes = hashes

                patient.sources = accum_sources(patient, patient.studies)
                patient.rev_idc_version = args.version

                if not patient.is_new:
                    patient.revised = True
            else:
                rootlogger.info("  p%s: Patient %s, %s, unchanged", args.id, patient.submitter_case_id, patient_index)

            patient.done = True
            sess.commit()
            duration = str(timedelta(seconds=(time.time() - begin)))
            rootlogger.info("  p%s: Patient %s, %s, completed in %s, %s", args.id, patient.submitter_case_id, patient_index, duration, time.asctime())


def worker(input, output, args, data_collection_doi, analysis_collection_dois, lock):
    # rootlogger.debug('p%s: Worker starting: args: %s', args.id, args)
    with Session(args.sql_engine) as sess:
        source = (args.source_class)(sess)
        source.lock = lock
        # rootlogger.info('p%s: Worker starting: args: %s', args.id, args)
        # rootlogger.info('p%s: Source: args: %s', args.id, source)
        # rootlogger.info('p%s: Access token: %s, Refresh token: %s', args.id, source.access_token, source.refresh_token)
        # rootlogger.info('p%s: Lock: _rand %s, _sem_lock: %s', args.id, source.lock._rand, source.lock._semlock)
        for more_args in iter(input.get, 'STOP'):
            for attempt in range(PATIENT_TRIES):
                time.sleep((2**attempt)-1)
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

            if attempt == PATIENT_TRIES:
                errlogger.error("p%s, Failed to process patient: %s", args.id, patient.submitter_case_id)
                sess.rollback()
            output.put(patient.submitter_case_id)


def retire_collection(sess, args, collection, source):
    # If this object has children from source, delete them
    if collection.sources[source.source_id]:
        for patient in collection.patients:
            retire_patient(sess, args, patient, source)
        collection_sources = list(collection.sources)
        collection_sources[source.source_id] = False
        collection.sources = collection_sources
        # Update sources of this object
        # for patient in collection.patients:
        #     collection.sources = [a or b for a, b in zip(collection.sources, patient.sources)]
        # If this object is not empty, return
        # if any(collection.sources):
        #     return
        # sess.delete(collection)
        # sess.commit()
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
                    min_timestamp = datetime.utcnow(),
                    init_idc_version=args.version,
                    rev_idc_version=args.version,
                    submitter_case_id=patient,
                    idc_case_id = uuid4(),
                    sources = [False, False],
                    hashes = ["", "", ""],
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
            retire_patient(sess, args, patient, source)
            if not any(patient.sources):
                sess.delete(patient)

        for patient in existing_objects:
            if source.patient_was_updated(patient):
                rootlogger.info('**Patient %s needs revision', patient.submitter_case_id)
                # Mark when we started work on this patient
                patient.min_timestamp = datetime.utcnow()
                patient.revised = False
                patient.done = False
                patient.is_new = False
                patient.expanded = False
            else:
                # Stamp this series showing when it was checked
                patient.min_timestamp = datetime.utcnow()
                patient.max_timestamp = datetime.utcnow()
                patient.done = True
                patient.expanded = True
                rootlogger.info('Patient %s unchanged', patient.submitter_case_id)
            patient.min_timestamp = datetime.utcnow()

        metadata = []
        for patient in new_objects:
            metadata.append(
                dict(
                    collection_id = collection.collection_id,
                    min_timestamp = datetime.utcnow(),
                    init_idc_version=args.version,
                    rev_idc_version=args.version,
                    submitter_case_id = patient,
                    idc_case_id = uuid4(),
                    sources = [False, False],
                    hashes = ["", "", ""],
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
    if data_collection_doi=="":
        if collection.collection_id=='NLST':
            data_collection_doi = '10.7937/TCIA.hmq8-j677'
        elif collection.collection_id=='CMMD':
            data_collection_doi = '10.7937/tcia.eqde-4b16'
        elif collection.collection_id == "Duke-Breast-Cancer-MRI":
            data_collection_doi = '10.7937/TCIA.e3sv-re93'
        elif collection.collection_id == 'QIBA-CT-Liver-Phantom':
            data_collection_doi = '10.7937/TCIA.RMV0-9Y95'
        elif collection.collection_id == 'Training-Pseudo':
            data_collection_doi == 'Training-Pseudo-TBD-DOI'
        elif collection.collection_id == 'B-mode-and-CEUS-Liver':
            data_collection_doi == '10.7937/TCIA.2021.v4z7-tc39'

        else:
            errlogger.error('No DOI for collection %s', collection.collection_id)
            return
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
                # if (collection.patients.index(patient) % 100 ) == 0:
                if True:
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
        lock = Lock()
        for process in range(args.num_processes):
            args.id = process+1
            processes.append(
                Process(target=worker, args=(task_queue, done_queue, args, data_collection_doi, analysis_collection_dois, lock )))
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

            sess.commit()

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
        # collection.min_timestamp = min([patient.min_timestamp for patient in collection.patients if patient.min_timestamp != None])
        collection.max_timestamp = max([patient.max_timestamp for patient in collection.patients if patient.max_timestamp != None])

        # Get hash of children
        hash = source.idc_collection_hash(collection)
        try:
            source_hash = source.src_collection_hash(collection.collection_id)
            if  source_hash != hash:
                errlogger.error('Hash match failed for collection %s', collection.collection_id)
            else:
                # Test whether anything has changed
                if hash != collection.hashes[source.source_id]:
                    hashes = list(collection.hashes)
                    hashes[source.source_id] = hash
                    collection.hashes = hashes
                    collection.sources = accum_sources(collection, collection.patients)

                    collection.rev_idc_version = args.version

                    if collection.is_new:
                        collection.revised = True
                else:
                    rootlogger.info("Collection %s, %s, unchanged", collection.collection_id, collection_index)

                collection.done = True
                sess.commit()
            duration = str(timedelta(seconds=(time.time() - begin)))
            rootlogger.info("Collection %s, %s, completed in %s", collection.collection_id, collection_index,
                            duration)
        except Exception as exc:
            errlogger.error('Could not validate collection hash for %s: %s', collection.collection_id, exc)

    else:
        duration = str(timedelta(seconds=(time.time() - begin)))
        rootlogger.info("Collection %s, %s, not completed in %s", collection.collection_id, collection_index,
                        duration)



def expand_version(sess, args, source, version, skips):
    # If we are here, we are beginning work on this version.
    # try:
    #     todos = open(args.todos).read().splitlines()
    # except:
    #     todos = []

    # Get the collections in the previous version
    idc_objects_results = sess.query(Collection)
    idc_objects = {c.collection_id:c for c in idc_objects_results}

    # Get the collections that the source knows about
    source_objects = source.collections()

    # New collections
    new_objects = [collection_id for collection_id in source_objects if collection_id not in idc_objects]
    # Collections that are no longer known about by the source
    retired_objects = [idc_objects[collection_id] for collection_id in idc_objects if collection_id not in source_objects]
    # Collections that are in the previous version and still known about by the source
    existing_objects = [idc_objects[collection_id] for collection_id in source_objects if collection_id in idc_objects]

    for collection in retired_objects:
        if not collection.collection_id in skips:
        # Remove from the collection table, moving instance data to the retired table
            rootlogger.info('Collection %s retiring', collection.collection_id)
            retire_collection(sess, args, collection, instance_source['path'].value)
            if not any(collection.sources):
                sess.delete(collection)


    for collection in existing_objects:
        if not collection.collection_id in skips:
            # If the our hash and source's hash differ then we need to revise this collection
            # if source.collection_hashes_differ(collection):
            if source.collection_was_updated(collection):
                rootlogger.debug('**Collection %s needs revision',collection.collection_id)
                # Mark when we started work on this collection
                collection.min_timestamp = datetime.utcnow()
                collection.revised = False
                collection.done = False
                collection.is_new = False
                collection.expanded = False
            else:
                collection.min_timestamp = datetime.utcnow()
                collection.max_timestamp = datetime.utcnow()
                collection.done = True
                collection.expanded = True
                rootlogger.debug('Collection %s unchanged',collection.collection_id)
            collection.min_timestamp = datetime.utcnow()

    collection_data = []
    for collection_id in new_objects:
        if not collection_id in skips:
            # The collection is new, so we must ingest it
            collection_data.append(
                dict(
                    min_timestamp = datetime.utcnow(),
                    init_idc_version=args.version,
                    rev_idc_version=args.version,
                    collection_id = collection_id,
                    sources = (True,False),
                    hashes = ("","",""),
                    revised = False,
                    done = False,
                    is_new = True,
                    expanded = False,
                )
            )
            rootlogger.info('Collection %s added', collection_id)

    sess.bulk_insert_mappings(Collection, collection_data)
    version.expanded = True
    sess.commit()
    rootlogger.info("Expanded version")

def build_version(sess, args, source, version):
    # Session = sessionmaker(bind= sql_engine)
    # version = version_is_done(sess, args.version)
    begin = time.time()
    try:
        skips = open(args.skips).read().splitlines()
    except:
        skips = []
    if not version.expanded:
        expand_version(sess, args, source, version, skips)
    idc_collections = [c for c in sess.query(Collection).order_by('collection_id')]
    rootlogger.info("Version %s; %s collections", args.version, len(idc_collections))
    for collection in idc_collections:
        if not collection.collection_id in skips:
        # if True:
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
    rootlogger.setLevel(INFO)

    # errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/path_ingest_v{}_err.log'.format(os.environ['PWD'], args.version))
    errformatter = logging.Formatter('{%(pathname)s:%(lineno)d} %(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    rootlogger.debug('Args: %s', args)

    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{args.db}'
    # sql_engine = create_engine(sql_uri, echo=True)
    sql_engine = create_engine(sql_uri)
    args.sql_engine = sql_engine

    conn = sql_engine.connect()
    register_composites(conn)
    # Base = declarative_base()
    # Base.metadata.create_all(sql_engine)
    #
    # declarative_base().metadata.create_all(sql_engine)

    # Create a local working directory
    if os.path.isdir('{}'.format(args.dicom)):
        shutil.rmtree('{}'.format(args.dicom))
    os.mkdir('{}'.format(args.dicom))


    # Add a new Version with idc_version_number args.version, if it does not already exist
    with Session(sql_engine) as sess:


        # source = (args.source)(sess)

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
            # We're not done. Ingest from each source
            for source_class in [TCIA, Pathology]:
                source = source_class(sess)
                source.lock = Lock()
                if not version.source_statuses[source.source_id].done:
                    args.source_class = source_class
                    build_version(sess, args, source, version)

            if all([source.done for source in version.source_statuses]):
                version.done = True

        else:
            rootlogger.info("    version %s previously built", args.version)


