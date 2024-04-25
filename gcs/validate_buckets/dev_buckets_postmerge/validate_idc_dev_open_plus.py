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

"""
Multiprocess script to validate that the idc-dev-open bucket
contains the expected set of blobs.
"""
import os
import argparse
from utilities.logging_config import successlogger, progresslogger, errlogger

import settings
from google.cloud import storage, bigquery




import argparse
import json
import settings
import builtins
builtins.APPEND_PROGRESSLOGGER = True
from utilities.logging_config import progresslogger

from validate_bucket_mp_plus import check_all_instances_mp
def get_buckets():
    client = storage.Client()
    bq_client = bigquery.Client()
    collections = {}
    query = f"""
    SELECT tcia_api_collection_id, dev_tcia_url, dev_idc_url
    FROM `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.all_collections`
    """

    result = bq_client.query(query).result()
    for row in result:
        collections[row['tcia_api_collection_id'].lower().replace('-','_').replace(' ','_')] = {"dev_tcia_url": row["dev_tcia_url"], "dev_idc_url": row["dev_idc_url"]}

    buckets = []
    for collection_id in collections:
        if client.bucket(f'idc_v{args.version}_tcia_{collection_id}').exists():
            buckets.append(client.bucket(f'idc_v{args.version}_tcia_{collection_id}'))
        if client.bucket(f'idc_v{args.version}_idc_{collection_id}').exists():
            buckets.append(client.bucket(f'idc_v{args.version}_idc_{collection_id}'))

    return buckets


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION)
    parser.add_argument('--processes', default=16)
    parser.add_argument('--bucket', default='idc-dev-open')
    # parser.add_argument('--src_project', default=settings.DEV_PROJECT)
    parser.add_argument('--dev_or_pub', default = 'dev', help='Validating a dev or pub bucket')
    parser.add_argument('--premerge', default=False, help='True when performing prior to merging premerge  buckets')
    parser.add_argument('--expected_blobs', default=f'{settings.LOG_DIR}/expected_blobs.txt', help='List of blobs names expected to be in above collections')
    parser.add_argument('--unexpected_blobs', default=f'{settings.LOG_DIR}/unexpected_blobs.txt', help='List of blobs names expected to be in above collections')
    parser.add_argument('--unexpected_bucket_counts', default=f'{settings.LOG_DIR}/unexpected_bucket_counts.txt', help='List of buckets and count of unexpected blobs in each')
    parser.add_argument('--staging_buckets_counts', default=f'{settings.LOG_DIR}/unexpected_bucket_counts.txt', help='List of buckets and count of unexpected blobs in each')

    # parser.add_argument('--found_blobs', default=f'{settings.LOG_DIR}/found_blobs.txt', help='List of blobs names found in bucket')
    parser.add_argument('--found_blobs', default=f'{settings.LOG_DIR}/success.log', help='List of blobs names found in bucket')
    parser.add_argument('--batch', default=10000, help='Size of batch assigned to each process')
    parser.add_argument('--log_dir', default=f'/mnt/disks/idc-etl/logs/validate_open_buckets')

    args = parser.parse_args()
    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    try:
        args.buckets = open(f'{settings.LOG_DIR}/staging_buckets.txt').read().splitlines()
    except:
        args.buckets = get_buckets()
        with open(f'{settings.LOG_DIR}/staging_buckets.txt', 'w') as f:
            f.write('\n'.join([b.name for b in args.buckets]))


    check_all_instances_mp(args, premerge=args.premerge)