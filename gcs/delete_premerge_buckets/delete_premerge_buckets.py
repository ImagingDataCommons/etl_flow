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

# Delete pre-staging buckets populated by ingestion.

import os
import argparse

from utilities.logging_config import successlogger, progresslogger
import settings
from google.cloud import storage, bigquery
from gcs.empty_bucket_mp.empty_bucket_mp import pre_delete

def get_collection_groups():
    client = bigquery.Client()
    collections = {}
    breakpoint() # FROM all_collections instead of all_included_collections?
    query = f"""
    SELECT idc_webapp_collection_id, dev_tcia_url, dev_idc_url
    FROM `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.all_included_collections`
    """

    result = client.query(query).result()
    for row in result:
        collections[row['idc_webapp_collection_id']] = {"dev_tcia_url": row["dev_tcia_url"], "dev_idc_url": row["dev_idc_url"]}

    return collections

def preview_copies(args, client, bucket_data):
    progresslogger.info('Deleting the following buckets')
    for collection_id in bucket_data:
        if client.bucket(f'idc_v{args.version}_tcia_{collection_id}').exists():
            progresslogger.info(f'Deleting idc_v{args.version}_tcia_{collection_id}')
        if client.bucket(f'idc_v{args.version}_idc_{collection_id}').exists():
            progresslogger.info(f'Deleting idc_v{args.version}_idc_{collection_id}')
    return


def delete_buckets(args):
    # client = storage.Client()
    # with sa_session() as sess:
    #     revised_collection_ids = sorted([row.collection_id for row in sess.query(Collection).filter(Collection.rev_idc_version == args.version).all()])
    #     for collection_id in revised_collection_ids:
    #         prestaging_collection_id = collection_id.lower().replace('-','_').replace(' ','_')
    #         for prefix in args.prestaging_bucket_prefix:
    #             prestaging_bucket = f"{prefix}{prestaging_collection_id}"
    #             if client.bucket(prestaging_bucket).exists():
    #                 args.bucket = prestaging_bucket
    #                 progresslogger.info(f'Deleting bucket {prestaging_bucket}')
    #                 # Delete the contents of the bucket
    #                 pre_delete(args)
    #                 # Delete the bucket itself
    #                 client.bucket(prestaging_bucket).delete()

    client = storage.Client()
    bucket_data= get_collection_groups()
    preview_copies(args, client, bucket_data)

    for collection_id in bucket_data:
        if client.bucket(f'idc_v{args.version}_tcia_{collection_id}').exists():
            # copy_prestaging_to_staging(args, f'idc_v{args.version}_tcia_{collection_id}', bucket_data[collection_id]['dev_tcia_url'], dones)
            prestaging_bucket = f'idc_v{args.version}_tcia_{collection_id}'
            args.bucket = prestaging_bucket
            progresslogger.info(f'Deleting bucket {prestaging_bucket}')
            # Delete the contents of the bucket
            pre_delete(args)
            # Delete the bucket itself
            client.bucket(prestaging_bucket).delete()
        if client.bucket(f'idc_v{args.version}_idc_{collection_id}').exists():
            # copy_prestaging_to_staging(args, f'idc_v{args.version}_idc_{collection_id}', bucket_data[collection_id]['dev_idc_url'], dones)
            prestaging_bucket = f'idc_v{args.version}_idc_{collection_id}'
            args.bucket = prestaging_bucket
            progresslogger.info(f'Deleting bucket {prestaging_bucket}')
            # Delete the contents of the bucket
            pre_delete(args)
            # Delete the bucket itself
            client.bucket(prestaging_bucket).delete()

    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION)
    # parser.add_argument('--prestaging_bucket_prefix', default=[f'idc_v{settings.CURRENT_VERSION}_tcia_', f'idc_v{settings.CURRENT_VERSION}_idc_'], help='Prefix of premerge buckets')
    parser.add_argument('--processes', default=32, help="Number of concurrent processes")
    parser.add_argument('--batch', default=100, help='Size of batch assigned to each process')
    args = parser.parse_args()

    delete_buckets(args)