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

# One time use script to rename V1 instance blobs whose UUIDs were incorrectly
# copied from the V1 aux table. This script actually copies these blobs with
# correct names.
# This script does not actually repair the UUIDs in the instance, study and
# series table, nor does it delete the incorrectly named blobs.
# That is done separately.

import sys
import os
import argparse
import logging
from logging import INFO
from google.cloud import bigquery, storage


from python_settings import settings
import settings as etl_settings

settings.configure(etl_settings)
assert settings.configured


def get_metadata(args):
    client = bigquery.Client()
    query = f"""
        SELECT instance_uuid, correct_instance_uuid
        FROM {args.project}.{args.bqdataset}.{args.table}
        WHERE not instance_uuid = correct_instance_uuid"""

    query_job = client.query(query)

    metadata = [{'incorrect_uuid':row['instance_uuid'], 'correct_uuid':row['correct_instance_uuid']} for row in query_job]
    return metadata

def rename(args, metadata):
    client = storage.Client()
    dones = open(args.dones).read().splitlines()
    f = open(args.dones,'a')
    bucket = client.bucket(args.bucket)
    n = 1
    for row in metadata:
        correct_blob = f'{row["correct_uuid"]}.dcm'
        incorrect_blob = f'{row["incorrect_uuid"]}.dcm'
        if correct_blob in dones:
            rootlogger.info('%s: %s in dones', n, correct_blob)
        elif storage.Blob(bucket=bucket, name=correct_blob).exists(client):
            rootlogger.info('%s: %s exists', n, correct_blob)
            f.write(f'{correct_blob}\n')
        else:
            rootlogger.info('%s: Copying %s to %s', n, incorrect_blob, correct_blob)
            blob_copy = bucket.copy_blob(bucket.blob(incorrect_blob), bucket, correct_blob)
            f.write(f'{correct_blob}\n')
        n += 1


def rename_v1_blobs(args):
    metadata = get_metadata(args)
    rename(args, metadata)


if __name__ == '__main__':
    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/rename_v1_blobs_log.log'.format(os.environ['PWD']))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/rename_v1_blobs_err.log'.format(os.environ['PWD']))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    parser = argparse.ArgumentParser()
    parser.add_argument('--project', default="idc-dev-etl")
    parser.add_argument('--bqdataset', default='whc_dev')
    parser.add_argument('--table', default='auxiliary_metadata_with_correct_uuids')
    parser.add_argument('--bucket', default='idc_dev')
    parser.add_argument('--dones', default='./logs/rename_v1_blobs_dones.log')
    args = parser.parse_args()

    print("{}".format(args), file=sys.stdout)

    rename_v1_blobs(args)
