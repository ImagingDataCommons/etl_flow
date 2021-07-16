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
from idc.models import Version, Collection, Patient, Study, Series, Instance, Retired
from sqlalchemy import select
from sqlalchemy.orm import Session
from utilities.tcia_helpers import  get_hash, get_TCIA_studies_per_patient, get_TCIA_patients_per_collection,\
    get_TCIA_series_per_study, get_TCIA_instance_uids_per_series, get_TCIA_instances_per_series, get_collection_values_and_counts
from utilities.get_collection_dois import get_data_collection_doi, get_analysis_collection_dois
from google.api_core.exceptions import Conflict

from python_settings import settings
import settings as etl_settings

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base



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


def copy_to_gcs(args, collection, patient, study, series):
    storage_client = storage.Client(project=args.project)

    # Delete the zip file before we copy to GCS so that it is not copied
    os.remove("{}/{}.zip".format(args.dicom, series.series_instance_uid))

    # Copy the instances to the staging bucket
    copy_disk_to_prestaging_bucket(args, series)

    # Ensure that they were copied correctly
    validate_series_in_gcs(args, collection, patient, study, series)

    # Delete the series from disk
    shutil.rmtree("{}/{}".format(args.dicom, series.series_instance_uid), ignore_errors=True)


def build_instances(sess, args, version, collection, patient, study, series):
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

    # Ensure that the zip has the expected number of instances
    # rootlogger.debug("      p%s: Series %s, check series length; %s", args.id, series.series_instance_uid, time.asctime())

    if not len(dcms) == len(series.instances):
        errlogger.error("      p%s: Invalid zip file for %s/%s/%s/%s", args.id,
            collection.collection_id, patient.submitter_case_id, study.study_instance_uid, series.series_instance_uid)
        raise RuntimeError("      \p%s: Invalid zip file for %s/%s/%s/%s", args.id,
            collection.collection_id, patient.submitter_case_id, study.study_instance_uid, series.series_instance_uid)
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
            raise RuntimeError("      p%s: Invalid DICOM file for %s/%s/%s/%s", args.id,
                collection.collection_id, patient.submitter_case_id, study.study_instance_uid, series.series_instance_uid)

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
            raise  RuntimeError("       p%s: Duplicate DICOM files for %s/%s/%s/%s/%s", args.id,
                collection.collection_id, patient.submitter_case_id, study.study_instance_uid, series.series_instance_uid, SOPInstanceUID)
        os.rename(file_name, blob_name)
        rename_times.append(time.time_ns())

        metadata_times.append(time.time_ns())
        instance.hash = md5_hasher(blob_name)
        instance.instance_size = Path(blob_name).stat().st_size
        instance.timestamp = datetime.utcnow()
        metadata_times.append(time.time_ns())

    instances_time = time.time_ns() - begin
    # rootlogger.debug("      p%s: Renamed all files for series %s; %s", args.id, series.series_instance_uid, time.asctime())
    # rootlogger.debug("      p%s: Series %s instances time: %s", args.id, series.series_instance_uid, instances_time/10**9)
    # rootlogger.debug("      p%s: Series %s pydicom time: %s", args.id, series.series_instance_uid, (sum(pydicom_times[1::2]) - sum(pydicom_times[0::2]))/10**9)
    # rootlogger.debug("      p%s: Series %s psql time: %s", args.id, series.series_instance_uid, (sum(psql_times[1::2]) - sum(psql_times[0::2]))/10**9)
    # rootlogger.debug("      p%s: Series %s rename time: %s", args.id, series.series_instance_uid, (sum(rename_times[1::2]) - sum(rename_times[0::2]))/10**9)
    # rootlogger.debug("      p%s: Series %s metadata time: %s", args.id, series.series_instance_uid, (sum(metadata_times[1::2]) - sum(metadata_times[0::2]))/10**9)

    copy_start = time.time_ns()
    copy_to_gcs(args, collection, patient, study, series)
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

def expand_series(sess, series):
    begin = time.time()
    instances = get_TCIA_instance_uids_per_series(series.series_instance_uid, args.server)
    tcia_time = time.time() - begin
    # If the study is new, then all the studies are new
    if series.is_new:
        instance_data = []
        for instance in instances:
            uuid = uuid4()
            instance_data.append(
                dict (
                     series_id=series.id,
                     idc_version_number=series.idc_version_number,
                     instance_initial_idc_version = series.idc_version_number,
                     sop_instance_uid=instance['SOPInstanceUID'],
                     timestamp=datetime(1970,1,1,0,0,0),
                     uuid=uuid,
                     gcs_url=f'gs://idc_dev/{uuid}.dcm',
                     hash="",
                     instance_size=0,
                     revised=True,
                     done=False,
                     is_new=True,
                     expanded=False
                )
            )
        sess.bulk_insert_mappings(Instance,instance_data)

        # for instance in instances:
        #     uuid = uuid4()
        #     series.instances.append(Instance(series_id=series.id,
        #                                  idc_version_number=series.idc_version_number,
        #                                  instance_initial_idc_version = series.idc_version_number,
        #                                  sop_instance_uid=instance['SOPInstanceUID'],
        #                                  timestamp=datetime(1970,1,1,0,0,0),
        #                                  uuid=uuid,
        #                                  gcs_url=f'gs://idc_dev/{uuid}.dcm',
        #                                  hash="",
        #                                  instance_size=0,
        #                                  revised=True,
        #                                  done=False,
        #                                  is_new=True,
        #                                  expanded=False))
        series.expanded = True
        sess.commit()
    else:
        # Need to add code for the case that the object is not new
        # Do that when we create version 3
        errlogger.error("p%s: Add code to handle not-new case in expand_series", args.id)
    return tcia_time


def build_series(sess, args, series_index, version, collection, patient, study, series, data_collection_doi, analysis_collection_dois):
    if not series.done:
        begin = time.time()
        if not series.expanded:
            tcia_time = expand_series(sess, series)
        else:
            tcia_time=0
        rootlogger.info("      p%s: Series %s; %s; %s instances, tcia: %s, expand: %s", args.id, series.series_instance_uid, series_index, len(series.instances), tcia_time, time.time()-begin)
        build_instances(sess, args, version, collection, patient, study, series)
        if all(instance.done for instance in series.instances):
            series.series_instances = len(series.instances)
            series.min_timestamp = min(instance.min_timestamp for instance in series.instances)
            series.max_timestamp = max(instance.max_timestamp for instance in series.instances)
            series.hash = get_merkle_hash([instance.hash for instance in series.instances])
            series.done = True
            sess.commit()
            duration = str(timedelta(seconds=(time.time() - begin)))
            rootlogger.info("      p%s: Series %s, %s, completed in %s", args.id, series.series_instance_uid, series_index, duration)
    else:
        rootlogger.info("      p%s: Series %s, %s, previously built", args.id, series.series_instance_uid, series_index)


def retire_series(sess, args, series):
    instance_data = []
    for instance in series.instances:
        instance_data.append(
            dict (
                timestamp=instance.timestamp,
                sop_instance_uid=instance.sop_instance_uid,
                uuid=instance.uuid,
                hash=instance.hash,
                instance_size=instance.instance_size,
                init_idc_version=instance.init_idc_version,
                rev_idc_version=instance.rev_idc_version,
                instance_uuid = instance.uuid,
                series_instance_uid = instance.series.series_instance_uid,
                series_uuid = instance.series.uuid,
                study_instance_uid = instance.series.study.study_instance_uid,
                study_uuid = instance.series.study.uuid,
                submitter_case_id = instance.series.study.patient.submitter_case_id,
                idc_case_id = instance.series.study.patient.submitter_case_id,
                collection_id = instance.study.series.study.patient.collection.collection_id
            )
        )
    sess.bulk_insert_mappings(Instance,instance_data)
    sess.delete(instance)

    sess.delete(series)
    sess.commit()


def expand_study(sess, collection, patient, study, data_collection_doi, analysis_collection_dois):
    rows = get_TCIA_series_per_study(collection.collection_id, patient.submitter_case_id, study.study_instance_uid, args.server)
    # If the study is new, then all the studies are new
    if study.is_new:
        series_data = []
        for row in rows:
            series_data.append(
                dict(
                     study_id=study.id,
                     idc_version_number=study.idc_version_number,
                     timestamp=datetime(1970,1,1,0,0,0),
                     series_initial_idc_version = study.idc_version_number,
                     series_instance_uid=row['SeriesInstanceUID'],
                     uuid=uuid4(),
                     series_instances=0,
                     source_doi=analysis_collection_dois[row['SeriesInstanceUID']] \
                         if row['SeriesInstanceUID'] in analysis_collection_dois
                         else data_collection_doi,
                     revised=True,
                     done=False,
                     is_new=True,
                     expanded=False
                )
            )
        sess.bulk_insert_mappings(Series, series_data)
        # study.seriess.append(Series(study_id=study.id,
        #                             idc_version_number=study.idc_version_number,
        #                             timestamp=datetime(1970, 1, 1, 0, 0, 0),
        #                             series_initial_idc_version=study.idc_version_number,
        #                             series_instance_uid=row['SeriesInstanceUID'],
        #                             uuid=uuid4(),
        #                             series_instances=0,
        #                             source_doi=analysis_collection_dois[row['SeriesInstanceUID']] \
        #                                 if row['SeriesInstanceUID'] in analysis_collection_dois
        #                             else data_collection_doi,
        #                             revised=True,
        #                             done=False,
        #                             is_new=True,
        #                             expanded=False))
        study.expanded = True
        sess.commit()
    else:
        # Need to add code for the case that the object is not new
        # Do that when we create version 3
        errlogger.error("p%s: Add code to handle not-new case in expand_study, args.id")


def build_study(sess, args, study_index, version, collection, patient, study, data_collection_doi, analysis_collection_dois):
    if not study.done:
        begin = time.time()
        if not study.expanded:
            expand_study(sess, collection, patient, study, data_collection_doi, analysis_collection_dois)
        rootlogger.info("    p%s: Study %s, %s, %s series, expand time: %s", args.id, study.study_instance_uid, study_index, len(study.seriess), time.time()-begin)
        for series in study.seriess:
            series_index = f'{study.seriess.index(series)+1} of {len(study.seriess)}'
            build_series(sess, args, series_index, version, collection, patient, study, series, data_collection_doi, analysis_collection_dois)
        if all([series.done for series in study.seriess]):
            study.study_instances = sum([series.series_instances for series in study.seriess])
            study.min_timestamp = min([series.min_timestamp for series in study.seriess])
            study.max_timestamp = max([series.max_timestamp for series in study.seriess])
            study.hash = get_merkle_hash([series.hash for series in study.seriess])
            study.done = True
            sess.commit()
            duration = str(timedelta(seconds=(time.time() - begin)))
            rootlogger.info("    p%s: Study %s, %s,  completed in %s", args.id, study.study_instance_uid, study_index, duration)
    else:
        rootlogger.info("    p%s: Study %s, %s, previously built", args.id, study.study_instance_uid, study_index)


def retire_study(sess, args, study):
    for series in study.seriess:
        retire_series(sess, args, series)
    sess.delete(study)
    sess.commit()

def expand_patient(sess, collection, patient):
    studies = get_TCIA_studies_per_patient(collection.collection_id, patient.submitter_case_id, args.server)
    # If the patient is new, then all the studies are new
    if patient.is_new:
        study_data = []
        for study in studies:
            study_data.append(
                 dict(patient_id=patient.id,
                 idc_version_number=patient.idc_version_number,
                 timestamp = datetime(1970,1,1,0,0,0),
                 study_initial_idc_version = patient.idc_version_number,
                 study_instance_uid=study['StudyInstanceUID'],
                 uuid=uuid4(),
                 study_instances = 0,
                 revised=True,
                 done=False,
                 is_new=True,
                 expanded=False
                      )
            )
        sess.bulk_insert_mappings(Study, study_data)
        patient.expanded = True
        sess.commit()
    else:
        # Need to add code for the case that the object is not new
        # Do that when we create version 3
        errlogger.error("p%s: Add code to handle not-new case in expand_patient", args.id)


def build_patient(sess, args, patient_index, data_collection_doi, analysis_collection_dois, version, collection, patient):
    if not patient.done:
        begin = time.time()
        if not patient.expanded:
            expand_patient(sess, collection, patient)
        rootlogger.info("  p%s: Patient %s, %s, %s studies, expand_time: %s, %s", args.id, patient.submitter_case_id, patient_index, len(patient.studies), time.time()-begin, time.asctime())
        for study in patient.studies:
            study_index = f'{patient.studies.index(study) + 1} of {len(patient.studies)}'
            build_study(sess, args, study_index, version, collection, patient, study, data_collection_doi, analysis_collection_dois)
        if all([study.done for study in patient.studies]):
            patient.min_timestamp = min([study.min_timestamp for study in patient.studies])
            patient.max_timestamp = max([study.max_timestamp for study in patient.studies])
            patient.hash = get_merkle_hash([study.hash for study in patient.studies])
            patient.done = True
            sess.commit()
            duration = str(timedelta(seconds=(time.time() - begin)))
            rootlogger.info("  p%s: Patient %s, %s, completed in %s, %s", args.id, patient.submitter_case_id, patient_index, duration, time.asctime())
    else:
        rootlogger.info("  p%s: Patient %s, %s, previously built", args.id, patient.submitter_case_id, patient_index)


def retire_patient(sess, args, patient):
    for study in patient.studies:
        retire_study(sess, args, study)
    sess.delete(patient)
    sess.commit()

def expand_collection(sess, args, collection):
    # If we are here, we are beginning work on this collection.
    # GCS data for the collection being built is accumulated in the prestaging bucket,
    # args.prestaging bucket.

    # Since we are starting, delete everything from the prestaging bucket.
    rootlogger.info("Emptying prestaging bucket")
    begin = time.time()
    create_prestaging_bucket(args)
    empty_bucket(args.prestaging_bucket)
     # Since we are starting, delete everything from the prestaging bucket.
    duration = str(timedelta(seconds=(time.time() - begin)))
    rootlogger.info("Emptying prestaging bucket completed in %s", duration)

    patients = get_TCIA_patients_per_collection(collection.collection_id, args.server)
    patient_ids = [patient['PatientId'] for patient in patients]
    if len(patient_ids) != len(set(patient_ids)):
        errlogger.error("\tp%s: Duplicate patient in expansion of collection %s", args.id,
                        collection.collection_id)
        raise RuntimeError("p%s: Duplicate patient in expansion of collection %s", args.id,
                        collection.collection_id)
    # If the collection is new, then all the patients are new
    if collection.is_new:
        our_patient_ids = [patient.submitter_case_id for patient in collection.patients]
        for patient in patients:
            if not patient['PatientId'] in our_patient_ids:
                collection.patients.append(Patient(collection_id = collection.id,
                                              idc_version_number = collection.idc_version_number,
                                              timestamp = datetime(1970,1,1,0,0,0),
                                              patient_initial_idc_version=collection.idc_version_number,
                                              submitter_case_id = patient['PatientId'],
                                              idc_case_id = uuid4(),
                                              revised = True,
                                              done = False,
                                              is_new = True,
                                              expanded = False))
        collection.expanded = True
        sess.commit()
    else:
        # Need to add code for the case that the object is not new
        # Do that when we create version 3
        errlogger.error("p%s: Add code to handle not-new case in expand_collection", args.id)


def worker(input, output, args, data_collection_doi, analysis_collection_dois):
    # rootlogger.debug('p%s: Worker starting: args: %s', args.id, args)
    for more_args in iter(input.get, 'STOP'):
        with Session(sql_engine) as sess:
            for attempt in range(PATIENT_TRIES):
                index, idc_version_number, collection_id, submitter_case_id = more_args
                try:
                    version = sess.query(Version).filter_by(idc_version_number=idc_version_number).one()
                    collection = next(collection for collection in version.collections if collection.collection_id==collection_id)
                    patient = next(patient for patient in collection.patients if patient.submitter_case_id==submitter_case_id)
                    # rootlogger.debug("p%s: In worker, sess: %s, submitter_case_id: %s", args.id, sess, submitter_case_id)
                    build_patient(sess, args, index, data_collection_doi, analysis_collection_dois, version, collection, patient)
                    break
                except Exception as exc:
                    errlogger.error("p%s, exception %s; reattempt %s on patient %s/%s, %s; %s", args.id, exc, attempt, collection_id, submitter_case_id, index, time.asctime())
                    sess.rollback()

            if attempt == PATIENT_TRIES-1:
                errlogger.error("p%s, Failed to process patient: %s", args.id, submitter_case_id)
                sess.rollback()
            output.put(submitter_case_id)


def build_collection(sess, args, collection_index, version, collection):
    if not collection.done:
    # if collection.collection_id == 'RIDER Breast MRI': # Temporary code for development
        begin = time.time()
        args.prestaging_bucket = f"{args.prestaging_bucket_prefix}{collection.collection_id.lower().replace(' ','_').replace('-','_')}"
        if not collection.expanded:
            expand_collection(sess, args, collection)
        rootlogger.info("Collection %s, %s, %s patients", collection.collection_id, collection_index, len(collection.patients))
        # Get the lists of data and analyis series in this patient
        data_collection_doi = get_data_collection_doi(collection.collection_id, server=args.server)
        pre_analysis_collection_dois = get_analysis_collection_dois(collection.collection_id, server=args.server)
        analysis_collection_dois = {x['SeriesInstanceUID']: x['SourceDOI'] for x in pre_analysis_collection_dois}

        if args.num_processes==0:
            # for series in sorted_seriess:
            args.id = 0
            for patient in collection.patients:
                patient_index = f'{collection.patients.index(patient) + 1} of {len(collection.patients)}'
                if not patient.done:
                    build_patient(sess, args, patient_index, data_collection_doi, analysis_collection_dois, version, collection, patient)
                else:
                    if (collection.patients.index(patient) % 100 ) == 0:
                        rootlogger.info("  p%s: Patient %s, %s, previously built", args.id, patient.submitter_case_id,
                                    patient_index)

            if all([patient.done for patient in collection.patients]):
                collection.min_timestamp = min([patient.min_timestamp for patient in collection.patients])
                collection.max_timestamp = max([patient.max_timestamp for patient in collection.patients])
                collection.hash = get_merkle_hash([patient.hash for patient in collection.patients])
                collection.done = True
                sess.commit()
                duration = str(timedelta(seconds=(time.time() - begin)))
                rootlogger.info("Collection %s, %s, completed in %s", collection.collection_id,
                                collection_index,
                                duration)
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
                    task_queue.put((patient_index, version.idc_version_number, collection.collection_id, patient.submitter_case_id))
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

                if all([patient.done for patient in collection.patients]):
                    collection.min_timestamp = min([patient.min_timestamp for patient in collection.patients])
                    collection.max_timestamp = max([patient.max_timestamp for patient in collection.patients])
                    collection.hash = get_merkle_hash([patient.hash for patient in collection.patients])
                    collection.done = True
                    sess.commit()
                    duration = str(timedelta(seconds=(time.time() - begin)))
                    rootlogger.info("Collection %s, %s, completed in %s", collection.collection_id, collection_index,
                                    duration)

            except Empty as e:
                errlogger.error("Timeout in build_collection %s", collection.collection_id)
                for process in processes:
                    process.terminate()
                    process.join()
                sess.rollback()
                duration = str(timedelta(seconds=(time.time() - begin)))
                rootlogger.info("Collection %s, %s, NOT completed in %s", collection.collection_id, collection_index,
                                duration)

    else:
        rootlogger.info("Collection %s, %s, previously built", collection.collection_id, collection_index)

def retire_collection(sess, args, collection):
    for patient in collection.patients:
        retire_patient(sess, args, patient)
    sess.delete(collection)
    sess.commit()


def expand_version(sess, args, version):
    # If we are here, we are beginning work on this version.
    try:
        skips = open(args.skips).read().splitlines()
    except:
        skips = []

    # stmt = select(Collection).distinct()
    # results = sess.execute(stmt)
    # idc_collections = results.fetchall()
    idc_collections_results = sess.query(Collection)

    # tcia_collection_ids = [collection['Collection'] for collection in get_TCIA_collections()]
    tcia_collections = [c.lower() for c in get_collection_values_and_counts(args.server)]
    idc_collections = {c.collection_id.lower():c for c in idc_collections_results}

    new_collections = [collection_id for collection_id in tcia_collections if collection_id not in idc_collections]
    retired_collections = [idc_collections[collection_id] for collection_id in idc_collections if collection_id not in tcia_collections]
    existing_collections = [idc_collections[collection_id] for collection_id in tcia_collections if collection_id in idc_collections]
    # new_collections = tcia_collection_ids - idc_collection_ids
    # retired_collections = idc_collection_ids - tcia_collection_ids
    # existing_collections = idc_collection_ids & tcia_collection_ids

    for collection in retired_collections:
        retire_collection(sess, args, collection)

    for collection in existing_collections:
        result = get_hash({'Collection': collection.collection_id})
        if result.status_code != 200:
            errlogger.error('Error %s getting collection %s hash', result.status_code, collection.collection_id )
            nbia_hash = ''
        else:
            nbia_hash = result.text
        if nbia_hash == collection.hash:
            collection.max_timestamp = collection.max_timestamp =  datetime.utcnow()
        else:
            collection.revised = False,
            collection.done = False,
            collection.is_new = False,
            collection.expanded = False

    collection_data = []
    for collection_id in new_collections:
        if not collection.collection_id in skips:
            collection_data.append(
                dict(
                    init_idc_version=args.version,
                    rev_idc_version=args.version,
                    collection_id = collection_id,
                    revised = True,
                    done = False,
                    is_new = True,
                    expanded = False
                )
            )
    sess.bulk_insert_mappings(Collection, collection_data)
    version.expanded = True
    sess.commit()
    rootlogger.info("Expanded version %s", version.idc_version_number)

def build_version(sess, args, version):
    # Session = sessionmaker(bind= sql_engine)
    # version = version_is_done(sess, args.version)
    if not version.done:
        begin = time.time()
        if not version.expanded:
            expand_version(sess, args, version)
        rootlogger.info("Version %s; %s collections", version.idc_version_number, len(version.collections))
        try:
            skips = open(args.skips).read().splitlines()
        except:
            skips = []
        for collection in version.collections:
            if not collection.collection_id in skips:
                collection_index = f'{version.collections.index(collection)+1} of {len(version.collections)}'
                build_collection(sess, args, collection_index, version, collection)
        # copy_staging_bucket_to_final_bucket(args,version)
        if all([collection.done for collection in version.collections if not collection.collection_id in skips]):
            hash = get_merkle_hash([collection.hash for collection in version.collections])
            duration = str(timedelta(seconds=(time.time() - begin)))
            if hash != version.hash:
                version.min_timestamp = min([collection.min_timestamp for collection in version.collections])
                version.max_timestamp = max([collection.maxtimestamp for collection in version.collections])
                version.done = True
                rootlogger.info("Built new version %s in %s", version.version, duration)
            else:
                version.version = args.version-1
                rootlogger.info("Version unchanges, remains at  %s in %s", version.version-1, duration)
            sess.commit()
        else:
            rootlogger.info("Not all collections are done. Rerun.")
    else:
        rootlogger.info("    version %s previously built", version.idc_version_number)


def prebuild(args):
    sql_uri = f'postgresql+psycopg2://{settings.DATABASE_USERNAME}:{settings.DATABASE_PASSWORD}@{settings.DATABASE_HOST}:{settings.DATABASE_PORT}/{args.db}'
    sql_engine = create_engine(sql_uri, echo=True)
    # sql_engine = create_engine(sql_uri)

    declarative_base().metadata.create_all(sql_engine)

    # Create a local working directory
    if os.path.isdir('{}'.format(args.dicom)):
        shutil.rmtree('{}'.format(args.dicom))
    os.mkdir('{}'.format(args.dicom))

    # Basically add a new Version with idc_version_number args.version, if it does not already exist
    with Session(sql_engine) as sess:
        stmt = select(Version).distinct()
        result = sess.execute(stmt)
        version = result.fetchone()[0]

        if version.version != args.version:
            version.expanded=False,
            version.done=False,
            version.is_new=True,
            version.revised=False
            version.version = args.version

            sess.commit()
        if not version.done:
            build_version(sess, args, version)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=3, help='Version to work on')
    args = parser.parse_args()
    parser.add_argument('--db', default=f'idc_v{args.version}', help='Database on which to operate')
    parser.add_argument('--project', default='idc-dev-etl')
    parser.add_argument('--prestaging_bucket_prefix', default=f'idc_v3_', help='Copy instances here before forwarding to --staging_bucket')
    parser.add_argument('--num_processes', default=0, help="Number of concurrent processes")
    parser.add_argument('--skips', default='{}/idc/v{}_skips.txt'.format(os.environ['PWD'], args.version ))
    parser.add_argument('--dicom', default='/mnt/disks/idc-etl/dicom', help='Directory in which to expand downloaded zip files')
    args = parser.parse_args()
    if args.project == 'idc-nlst':
        args.server = 'NLST'
    else:
        args.server = 'NBIA'

    # Directory to which to download files from TCIA/NBIA
    print("{}".format(args), file=sys.stdout)

    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/build_v{}_log.log'.format(os.environ['PWD'], args.version))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(DEBUG)

    errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/build_v{}_err.log'.format(os.environ['PWD'], args.version))
    errformatter = logging.Formatter('{%(pathname)s:%(lineno)d} %(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    rootlogger.info('Args: %s', args)
    prebuild(args)
