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

import sys
import os
import argparse
from pathlib import Path
import time
from datetime import datetime, timezone, timedelta
import logging
from logging import INFO
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
from idc.models import Version, Collection, Patient, Study, Series, Instance, sql_engine
from sqlalchemy import select
from sqlalchemy.orm import Session
from utilities.tcia_helpers import  get_TCIA_collections, get_TCIA_patients_per_collection, get_TCIA_studies_per_patient, \
    get_TCIA_series_per_study, get_TCIA_instance_uids_per_series, get_TCIA_instances_per_series, get_collection_values_and_counts
from utilities.identify_third_party_series import get_data_collection_doi, get_analysis_collection_dois
from google.api_core.exceptions import Conflict

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


def rollback_copy_to_prestaging_bucket(args, series):
    client = storage.Client()
    bucket = client.bucket(args.prestaging_bucket)
    for instance in series.instances:
        try:
            results = bucket.blob(f'{instance.instance_uuid}.dcm').delete()
        except:
            errlogger.error('p%s: Failed to delete blob %s.dcm during validation rollback',args.id, instance.instance_uuid)
            raise


def validate_series_in_gcs(storage_client, args, collection, patient, study, series):
    # blobs_info = get_series_info(storage_client, args.project, args.staging_bucket)
    bucket = storage_client.get_bucket(args.prestaging_bucket)
    try:
        for instance in series.instances:
            blob = bucket.blob(f'{instance.instance_uuid}.dcm')
            blob.reload()
            assert instance.instance_hash == b64decode(blob.md5_hash).hex()
            assert instance.instance_size == blob.size

    except Exception as exc:
        rollback_copy_to_prestaging_bucket(args, series)
        errlogger.error('p%s: GCS validation failed for %s/%s/%s/%s/%s',
            args.id, collection.tcia_api_collection_id, patient.submitter_case_id, study.study_instance_uid, instance.sop_instance_uid)
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
        rootlogger.debug(("p%s: Uploaded instances to GCS", args.id))
    except Exception as exc:
        errlogger.error("\tp%s: Copy to prestage bucket failed for series %s", args.id, series.series_instance_uid)
        raise RuntimeError("p%s: Copy to prestage bucketfailed for series %s", args.id, series.series_instance_uid) from exc


# Copy a completed collection from the prestaging bucket to the staging bucket
def copy_prestaging_to_staging_bucket(args, collection):
    rootlogger.info("Copying prestaging bucket to staging bucket")
    try:
        # Copy the series to GCS
        src = "gs://{}/*".format(args.prestaging_bucket)
        dst = "gs://{}/".format(args.staging_bucket)
        result = run(["gsutil", "-m", "-q", "cp", src, dst])
        if result.returncode < 0:
            errlogger.error('\tp%s: copy_prestaging_to_staging_bucket failed for collection %s', args.id, collection.tcia_api_collection_id)
            raise RuntimeError('p%s: copy_prestaging_to_staging_bucket failed for collection %s', args.id, collection.tcia_api_collection_id)
        rootlogger.debug(("p%s: Uploaded instances to GCS", args.id))
    except Exception as exc:
        errlogger.error("\tp%s: Copy from prestaging to staging bucket for collection %s failed", args.id, collection.tcia_api_collection_id)
        raise RuntimeError("p%s: Copy from prestaging to staging bucket for collection %s failed", args.id, collection.tcia_api_collection_id) from exc


def copy_staging_bucket_to_final_bucket(args, version):
    try:
        # Copy the series to GCS
        src = "gs://{}/*".format(args.staging_bucket)
        dst = "gs://{}/".format(args.bucket)
        result = run(["gsutil", "-m", "-q", "cp", src, dst])
        if result.returncode < 0:
            errlogger.error('\tp%s: copy_staging_bucket_to_final_bucket failed for version %s', args.id, version.idc_version_number)
            raise RuntimeError('p%s: copy_staging_bucket_to_final_bucket failed for version %s', args.id, version.idc_version_number)
        rootlogger.debug(("p%s: Uploaded instances to GCS"))
    except Exception as exc:
        errlogger.error("\tp%s: Copy from prestaging to staging bucket for collection %s failed", args.id, version.idc_version_number)
        raise RuntimeError("p%s: Copy from prestaging to staging bucket for collection %s failed", args.id, version.idc_version_number) from exc


def empty_bucket(bucket):
    try:
        src = "gs://{}/*".format(bucket)
        run(["gsutil", "-m", "-q", "rm", src])
        rootlogger.debug(("Emptied bucket %s", bucket))
    except Exception as exc:
        errlogger.error("Failed to empty bucket %s", bucket)
        raise RuntimeError("Failed to empty bucket %s", bucket) from exc

def create_prestaging_bucket(args):
    client = storage.Client(project=args.project)

    # Try to create the destination bucket
    new_bucket = client.bucket(args.prestaging_bucket)
    new_bucket.iam_configuration.uniform_bucket_level_access_enabled = True
    new_bucket.versioning_enabled = False
    try:
        result = client.create_bucket(new_bucket, location='US-CENTRAL1')
        # return(0)
    except Conflict:
        # Bucket exists
        pass
    except Exception as e:
        # Bucket creation failed somehow
        errlogger.error("Error creating bucket %s: %s",args.prestaging_bucket, e)
        return(-1)


def copy_to_gcs(args, collection, patient, study, series):
    storage_client = storage.Client()

    # Delete the zip file before we copy to GCS so that it is not copied
    os.remove("{}/{}.zip".format(args.dicom, series.series_instance_uid))

    # Copy the instances to the staging bucket
    copy_disk_to_prestaging_bucket(args, series)

    # Delete the series from disk
    shutil.rmtree("{}/{}".format(args.dicom, series.series_instance_uid), ignore_errors=True)

#Get info on each blob in a collection
def get_collection_iterator(storage_client, bucket_name, prefix):
    pages = storage_client.list_blobs(bucket_name)
    # pages = storage_client.list_blobs(bucket_name, prefix="dicom/")
    return pages


def get_bucket_metadata(storage_client, bucket_name, prefix):
    pages = get_collection_iterator(storage_client, bucket_name, prefix)
    blobs = []

    for page in pages.pages:
        blobs.extend(list(page))
    metadata = {blob.name:blob for blob in blobs}
    return metadata

def get_blobs_in_bucket(client, args):
    # blobs_info = get_series_info(storage_client, args.project, args.staging_bucket)
    try:
        blobs = client.bucket(args.bucket).list_blobs()
        return blobs
    except Exception as exc:
        errlogger.error('Get blob info failed with erro %s', exc)
        raise exc

def main(args):
    client = storage.Client()
    blob_info = get_blobs_in_bucket(client, args)

    with Session(sql_engine) as sess:
        stmt = select(Version).distinct()
        result = sess.execute(stmt)
        version = []
        for row in result:
            if row[0].idc_version_number == args.vnext:
                # We've at least started working on vnext
                version = row[0]
                break


if __name__ == '__main__':
    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/log.log'.format(os.environ['PWD']))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/err.log'.format(os.environ['PWD']))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    parser = argparse.ArgumentParser()
    parser.add_argument('--vnext', default=2, help='Next version to generate')
    parser.add_argument('--bucket', default='idc_dev', help='Bucket in which to save instances')
    parser.add_argument('--staging_bucket', default='idc_dev_staging', help='Copy instances here before forwarding to --bucket')
    parser.add_argument('--prestaging_bucket_prefix', default=f'idc_v2_', help='Copy instances here before forwarding to --staging_bucket')
    parser.add_argument('--num_processes', default=8, help="Number of concurrent processes")
    parser.add_argument('--skips', default='{}/idc/skips.txt'.format(os.environ['PWD']) )
    parser.add_argument('--bq_dataset', default='mvp_wave2', help='BQ dataset')
    parser.add_argument('--bq_aux_name', default='auxilliary_metadata', help='Auxilliary metadata table name')
    parser.add_argument('--project', default='idc-dev-etl')
    args = parser.parse_args()

    args.dicom = 'dicom'
    print("{}".format(args), file=sys.stdout)

    main(args)
