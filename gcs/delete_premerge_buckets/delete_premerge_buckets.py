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
from gcs.empty_bucket_mp.empty_bucket_mp import del_all_instances

# def get_collection_groups():
#     client = bigquery.Client()
#     collections = {}
#     query = f"""
#     SELECT REPLACE(REPLACE(LOWER(tcia_api_collection_id),'-','_'),' ','_') idc_webapp_collection_id, dev_tcia_url, dev_idc_url
#     FROM `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.all_collections`
#     """
#
#     result = client.query(query).result()
#     for row in result:
#         collections[row['idc_webapp_collection_id']] = {"dev_tcia_url": row["dev_tcia_url"], "dev_idc_url": row["dev_idc_url"]}
#
#     return collections
#
# def preview_deletes(args, client, bucket_data):
#     progresslogger.info('Deleting the following buckets')
#     for collection_id in bucket_data:
#         if client.bucket(f'idc_v{args.version}_tcia_{collection_id}').exists():
#             progresslogger.info(f'Deleting idc_v{args.version}_tcia_{collection_id}')
#         if client.bucket(f'idc_v{args.version}_idc_{collection_id}').exists():
#             progresslogger.info(f'Deleting idc_v{args.version}_idc_{collection_id}')
#     return

def get_prestage_buckets(args):
    client = bigquery.Client()
    query = f"""
SELECT DISTINCT CONCAT('idc_v{args.version}_',source,'_', REPLACE(REPLACE(LOWER(collection_id), '-','_'),' ','_')) src_bucket
FROM `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.all_joined_public_and_current` 
WHERE i_rev_idc_version={settings.CURRENT_VERSION}
ORDER BY src_bucket"""

    result = client.query(query).result()
    progresslogger.info(f"Prestaging buckets:")
    for row in result:
        progresslogger.info(f'{row["src_bucket"]}')
    return client.query(query).result()


def delete_buckets(args):
    client = storage.Client()
    # bucket_data= get_collection_groups()
    # preview_deletes(args, client, bucket_data)
    bucket_data = get_prestage_buckets(args)

    for row in bucket_data:
        prestaging_bucket = row['src_bucket']
        if client.bucket(prestaging_bucket).exists():
            args.bucket = prestaging_bucket
            progresslogger.info(f'Deleting bucket {prestaging_bucket}')
            # Delete the contents of the bucket
            del_all_instances(args)
            # Delete the bucket itself
            client.bucket(prestaging_bucket).delete()
        else:
            progresslogger.info((f'{prestaging_bucket} previously deleted'))

    # for collection_id in bucket_data:
    #     if client.bucket(f'idc_v{args.version}_tcia_{collection_id}').exists():
    #         # copy_prestaging_to_staging(args, f'idc_v{args.version}_tcia_{collection_id}', bucket_data[collection_id]['dev_tcia_url'], dones)
    #         prestaging_bucket = f'idc_v{args.version}_tcia_{collection_id}'
    #         args.bucket = prestaging_bucket
    #         progresslogger.info(f'Deleting bucket {prestaging_bucket}')
    #         # Delete the contents of the bucket
    #         del_all_instances(args)
    #         # Delete the bucket itself
    #         client.bucket(prestaging_bucket).delete()
    #     if client.bucket(f'idc_v{args.version}_idc_{collection_id}').exists():
    #         # copy_prestaging_to_staging(args, f'idc_v{args.version}_idc_{collection_id}', bucket_data[collection_id]['dev_idc_url'], dones)
    #         prestaging_bucket = f'idc_v{args.version}_idc_{collection_id}'
    #         args.bucket = prestaging_bucket
    #         progresslogger.info(f'Deleting bucket {prestaging_bucket}')
    #         # Delete the contents of the bucket
    #         del_all_instances(args)
    #         # Delete the bucket itself
    #         client.bucket(prestaging_bucket).delete()

    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION)
    parser.add_argument('--processes', default=8, help="Number of concurrent processes")
    parser.add_argument('--batch', default=100, help='Size of batch assigned to each process')
    args = parser.parse_args()

    delete_buckets(args)