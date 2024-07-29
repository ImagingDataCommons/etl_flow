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

# Validate that the premerge buckets have the expected instances

import argparse
import io
import json
import os
import builtins
builtins.APPEND_PROGRESSLOGGER = True

import settings
# Noramlly the progresslogger file is trunacated. The following causes it to be appended.
# builtins.APPEND_PROGRESSLOGGER = True
from utilities.logging_config import successlogger, progresslogger, errlogger
from google.cloud import storage, bigquery
import subprocess


def get_expected_blobs_in_bucket(args):
    client = bigquery.Client()
    query = f"""
      SELECT distinct concat(se_uuid,'/', i_uuid, '.dcm') as blob_name
      FROM `idc-dev-etl.idc_v{args.version}_dev.all_joined_public_and_current` ajc
      WHERE se_rev_idc_version={args.version} AND collection_id = '{args.bucket["collection_id"]}' AND i_source='{args.bucket["source"]}'
      ORDER BY blob_name
  """

    result = client.query(query)
    while result.state != "DONE":
        result = client.get_job(result.job_id)
    if result.error_result != None:
        breakpoint()

    blob_names = set(result.to_dataframe()['blob_name'].to_list())
    return blob_names


def get_found_blobs_in_bucket(args):
    client = storage.Client()
    bucket = client.bucket(args.bucket['bucket_id'])

    with open(args.found_blobs, 'w') as f:
        result = subprocess.run(['gsutil', '-m', 'ls', f'gs://{args.bucket["bucket_id"]}/**'], stdout=f)
    found_blobs = [row.replace(f'gs://{args.bucket["bucket_id"]}/', '') for row in set(open(args.found_blobs).read().splitlines())]
    found_blobs.sort()
    with open(args.found_blobs, 'w') as f:
        for row in found_blobs:
            f.write(f'{row}\n')
    return


def check_all_instances_mp(args):
    try:
        found_blobs = set(open(args.found_blobs).read().splitlines())
        assert len(found_blobs) > 0
        progresslogger.info(f'Already have found blobs')
    except Exception as exc:
        progresslogger.info(f'Getting found blobs')
        get_found_blobs_in_bucket(args)
        found_blobs = set(open(args.found_blobs).read().splitlines())

    expected_blobs = get_expected_blobs_in_bucket(args)


    if found_blobs == expected_blobs:
        progresslogger.info(f"Bucket {args.bucket['bucket_id']} has the correct set of blobs")
        open(args.found_blobs, 'w').close()
        with open(args.validated_buckets, 'a') as f:
            f.write(f"{args.bucket['bucket_id']}\n")
    else:
        errlogger.error(f"Bucket {args.bucket['bucket_id']} does not have the correct set of blobs")
        errlogger.error(f"Unexpected blobs in bucket: {len(found_blobs - expected_blobs)}")
        for blob in found_blobs - expected_blobs:
            errlogger.error(blob)
        errlogger.error(f"Expected blobs not found in bucket: {len(expected_blobs - found_blobs)}")
        for blob in expected_blobs - found_blobs:
            errlogger.error(blob)
        breakpoint()

    return

def validate_all_buckets(args):
    try:
        validated = open(args.validated_buckets).read().splitlines()
    except:
        validated = []
    client = bigquery.Client()
    query=f"""
    SELECT DISTINCT collection_id, i_source source, CONCAT("idc_v{args.version}_", i_source, "_", REPLACE(REPLACE(LOWER(collection_id),"-", "_")," ","_")) bucket_id
    FROM `{settings.DEV_PROJECT}.idc_v{settings.CURRENT_VERSION}_dev.all_joined_public_and_current` aj
    WHERE i_rev_idc_version={settings.CURRENT_VERSION}
    """

    result = client.query(query)
    while result.state != "DONE":
        result = client.get_job(result.job_id)
    if result.error_result != None:
        breakpoint()

    buckets = [dict(row) for row in result]
    for bucket in buckets:
        if bucket['bucket_id'] not in validated:
            args.bucket = bucket
            progresslogger.info(f'Validating {bucket}')
            check_all_instances_mp(args)
        else:
            progresslogger.info(f'{bucket} prviously validated')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION)
    parser.add_argument('--processes', default=8)
    parser.add_argument('--dev_or_pub', default = 'dev', help='Validating a dev or pub bucket')
    parser.add_argument('--expected_blobs', default=f'{settings.LOG_DIR}/expected_blobs.txt', help='List of blobs names expected to be in above collections')
    parser.add_argument('--found_blobs', default=f'{settings.LOG_DIR}/success.log', help='List of blobs names found in bucket')
    parser.add_argument('--validated_buckets', default=f'{settings.LOG_DIR}/done_buckets.log', help='A file containing names of validated buckets')
    parser.add_argument('--batch', default=10000, help='Size of batch assigned to each process')
    parser.add_argument('--log_dir', default=f'/mnt/disks/idc-etl/logs/validate_open_buckets')

    args = parser.parse_args()
    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    validate_all_buckets(args)