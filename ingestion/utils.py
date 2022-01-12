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

import shutil
import os
import hashlib
from base64 import b64decode

import logging
rootlogger = logging.getLogger('root')
errlogger = logging.getLogger('root.err')
from subprocess import run

from google.cloud import storage
from google.api_core.exceptions import Conflict


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
            assert instance.size == blob.size

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
    storage_client = args.client
    wsi_src_bucket = storage_client.bucket(gcs_url.split('gs://')[1].split('/',1)[0])
    blob_id = gcs_url.split('gs://')[1].split('/',1)[1]
    dst_bucket = storage_client.bucket((args.prestaging_bucket))
    src_blob = wsi_src_bucket.blob(blob_id)
    dst_blob = dst_bucket.blob(f'{instance.uuid}.dcm')
    token, bytes_rewritten, total_bytes = dst_blob.rewrite(src_blob)
    while token:
        rootlogger.debug('******p%s: Rewrite bytes_rewritten %s, total_bytes %s', args.id, bytes_rewritten, total_bytes)
        token, bytes_rewritten, total_bytes = dst_blob.rewrite(src_blob, token=token)
    dst_blob.reload()
    return dst_blob.size, b64decode(dst_blob.md5_hash).hex()


def accum_sources(parent, children):
    sources = children[0].sources
    for child in children[1:]:
        sources = [x | y for (x, y) in zip(sources, child.sources)]
    return sources
