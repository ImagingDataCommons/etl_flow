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
from subprocess import run
from google.cloud import storage
from google.api_core.exceptions import Conflict

from python_settings import settings

from idc.models import All_Collections

# rootlogger = logging.getLogger('root')
successlogger = logging.getLogger('root.success')
debuglogger = logging.getLogger('root.prog')
errlogger = logging.getLogger('root.err')

def is_skipped(skipped_collections, collection_id):
    if collection_id in skipped_collections:
        skipped = skipped_collections[collection_id]
    else:
        skipped = (False, False)
    return skipped

def to_webapp(collection_id):
    return collection_id.lower().replace('-','_').replace(' ','_')

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
# Return "" if the list is empty
def get_merkle_hash(hashes):
    if hashes:
        md5 = hashlib.md5()
        hashes.sort()
        for hash in hashes:
            md5.update(hash.encode())
        return md5.hexdigest()
    else:
        ""

# Validate that instances were received correctly
def validate_hashes(args, collection, patient, study, series, hashes):
    for instance in hashes:
        instance = instance.split(',')
        if md5_hasher(f'{args.dicom_dir}/{series.series_instance_uid}/{instance[0]}') != instance[1]:
            errlogger.error("      p%s: Invalid hash for %s/%s/%s/%s", args.pid,
            collection.collection_id, patient.submitter_case_id, study.study_instance_uid, series.series_instance_uid,\
            instance[0])
            return False
    return True


def rollback_copy_to_prestaging_bucket(client, args, series):
    bucket = client.bucket(args.prestaging_tcia_bucket)
    for instance in series.instances:
        try:
            results = bucket.blob(f'{instance.uuid}.dcm').delete()
        except:
            errlogger.error('p%s: Failed to delete blob %s.dcm during validation rollback',args.pid, instance.uuid)
            raise


def validate_series_in_gcs(args, collection, patient, study, series):
    # client = storage.Client(project=settings.DEV_PROJECT)
    client = storage.Client()
    # blobs_info = get_series_info(storage_client, args.project, args.staging_bucket)
    bucket = client.get_bucket(args.prestaging_tcia_bucket)
    try:
        for instance in series.instances:
            blob = bucket.blob(f'{instance.uuid}.dcm')
            blob.reload()
            assert instance.hash == b64decode(blob.md5_hash).hex()
            assert instance.size == blob.size

    except Exception as exc:
        rollback_copy_to_prestaging_bucket(client, args, series)
        errlogger.error('p%s: GCS validation failed for %s/%s/%s/%s/%s',
            args.pid, collection.collection_id, patient.submitter_case_id, study.study_instance_uid, series.series_instance_uid, instance.sop_instance_uid)
        raise exc


# Copy the series instances downloaded from TCIA/NBIA from disk to the prestaging bucket
def copy_disk_to_prestaging_bucket(args, series):
    # Do the copy as a subprocess in order to use the gsutil -m option
    try:
        # Copy the series to GCS
        src = "{}/{}/*".format(args.dicom_dir, series.series_instance_uid)
        dst = "gs://{}/".format(args.prestaging_tcia_bucket)
        # breakpoint() # Check if -J parameter is still broken
        result = run(["gsutil", "-m", "-q", "cp", src, dst], check=True)
        # result = run(["gsutil", "-m", "-q", "cp", "-J", src, dst], check=True)
        if result.returncode :
            errlogger.error('p%s: \tcopy_disk_to_prestaging_bucket failed for series %s', args.pid, series.series_instance_uid)
            raise RuntimeError('p%s: copy_disk_to_prestaging_bucket failed for series %s', args.pid, series.series_instance_uid)
        # rootlogger.debug("p%s: Uploaded instances to GCS", args.pid)
    except Exception as exc:
        errlogger.error("\tp%s: Copy to prestage bucket failed for series %s", args.pid, series.series_instance_uid)
        raise RuntimeError("p%s: Copy to prestage bucketfailed for series %s", args.pid, series.series_instance_uid) from exc


def empty_bucket(bucket):
    try:
        src = "gs://{}/*".format(bucket)
        run(["gsutil", "-m", "-q", "rm", src])
        debuglogger.debug("Emptied bucket %s", bucket)
    except Exception as exc:
        errlogger.error("Failed to empty bucket %s", bucket)
        raise RuntimeError("Failed to empty bucket %s", bucket) from exc


def create_prestaging_bucket(args, bucket):
    client = storage.Client(project=settings.DEV_PROJECT)

    # Try to create the destination bucket
    new_bucket = client.bucket(bucket)
    new_bucket.iam_configuration.uniform_bucket_level_access_enabled = True
    new_bucket.versioning_enabled = False
    try:
        result = client.create_bucket(new_bucket, location='US-CENTRAL1', project=settings.DEV_PROJECT)
        # return(0)
    except Conflict:
        # Bucket exists
        pass
    except Exception as e:
        # Bucket creation failed somehow
        errlogger.error("Error creating bucket %s: %s",bucket, e)
        return(-1)


def copy_disk_to_gcs(args, collection, patient, study, series):
    # storage_client = storage.Client(project=settings.DEV_PROJECT)

    # Delete the zip file before we copy to GCS so that it is not copied
    os.remove("{}/{}.zip".format(args.dicom_dir, series.series_instance_uid))

    # Copy the instances to the staging bucket
    copy_disk_to_prestaging_bucket(args, series)

    # Ensure that they were copied correctly
    validate_series_in_gcs(args, collection, patient, study, series)

    # Delete the series from disk
    shutil.rmtree("{}/{}".format(args.dicom_dir, series.series_instance_uid), ignore_errors=True)


# Copy an instance from a source bucket to a destination bucket. Currently used when ingesting IDC sourced data
# which is placed in some bucket after preparation
def copy_gcs_to_gcs(args, client, dst_bucket_name, instance, gcs_url):
    # storage_client = args.client
    idc_src_bucket = client.bucket(gcs_url.split('gs://')[1].split('/',1)[0])
    blob_id = gcs_url.split('gs://')[1].split('/',1)[1]
    dst_bucket = client.bucket(dst_bucket_name)
    src_blob = idc_src_bucket.blob(blob_id)
    dst_blob = dst_bucket.blob(f'{instance.uuid}.dcm')
    token, bytes_rewritten, total_bytes = dst_blob.rewrite(src_blob)
    while token:
        debuglogger.debug('******p%s: Rewrite bytes_rewritten %s, total_bytes %s', args.pid, bytes_rewritten, total_bytes)
        token, bytes_rewritten, total_bytes = dst_blob.rewrite(src_blob, token=token)
    dst_blob.reload()
    return dst_blob.size, b64decode(dst_blob.md5_hash).hex()


# The sources of a parent (that is not a series) is the source-wise OR of its children
def accum_sources(parent, children):
    sources = children[0].sources
    for child in children[1:]:
        sources = [x | y for (x, y) in zip(sources, child.sources)]
    return sources


# Generate a list of skipped collections. We always skip collections that don't have 'Public' access.
def list_skips(sess, source, skipped_groups, skipped_collections, included_collections=[]):
    # tables_dict = {table.__tablename__: table for table in Base.__subclasses__()}
    skips = [collection for collection in skipped_collections]
    # for group in skipped_groups:
    #     collections = sess.query(tables_dict[group].tcia_api_collection_id).all()
    #     for collection in collections:
    #         skips.append(collection.tcia_api_collection_id)
    if source == 'tcia':
        collections = sess.query(All_Collections.tcia_api_collection_id).filter(All_Collections.tcia_access != 'Public').all()
    else:
        collections = sess.query(All_Collections.tcia_api_collection_id).filter(All_Collections.idc_access != 'Public').all()
    for collection in collections:
        skips.append(collection.tcia_api_collection_id)
    skips = list(set(skips) - set(included_collections))
    skips.sort()
    return skips
