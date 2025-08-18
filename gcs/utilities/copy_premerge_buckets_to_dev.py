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

# Copy premerge buckets populated by ingestion to staging buckets.
# Ingestion copies data into premerge buckets named by version and
# collection, e.g. idc_v9_idc_tcga_brca. The data in these buckets must be
# copied to one of the idc-dev-etl staging buckets:
# idc-dev-open, idc-dev-cr, idc-dev-defaced, idc-dev-redacted, idc-dev-excluded.

import os
import argparse
from utilities.logging_config import successlogger, progresslogger, errlogger

import settings
from google.cloud import storage, bigquery
from copy_blobs_using_BQ_query.copy_blobs_mp import copy_all_blobs


def preview_copies(args):
    client = bigquery.Client()
    query = f"""
SELECT DISTINCT CONCAT('idc_v{args.version}_',source,'_', REPLACE(REPLACE(LOWER(collection_id), '-','_'),' ','_')) src_bucket, dev_bucket dst_bucket
FROM `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.all_joined_public_and_current` 
WHERE i_rev_idc_version={settings.CURRENT_VERSION}
ORDER BY src_bucket"""

    result = client.query(query).result()
    progresslogger.info(f"Copy preview:")
    for row in result:
        progresslogger.info(f'{row["src_bucket"]}-->{row["dst_bucket"]}')
    return client.query(query).result()


def copy_premerge_buckets(args):
    client = storage.Client()
    src_and_dst_buckets = preview_copies(args)

    for row in src_and_dst_buckets:
        args.src_bucket = row['src_bucket']
        args.dst_bucket = row['dst_bucket']

        query = f"""
        SELECT DISTINCT CONCAT(se_uuid, '/', i_uuid, '.dcm') blob_name
        FROM `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.all_joined` 
        WHERE i_rev_idc_version={settings.CURRENT_VERSION} 
        AND CONCAT('idc_v{args.version}_',source,'_', REPLACE(REPLACE(LOWER(collection_id), '-','_'),' ','_')) = '{args.src_bucket}'
        AND dev_bucket = '{args.dst_bucket}'
        ORDER BY blob_name"""

        progresslogger.info(f'p0: Copying {args.src_bucket}-->{args.dst_bucket}')
        copy_all_blobs(args, query)

    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='Version to work on')
    parser.add_argument('--processes', default=16, help="Number of concurrent processes")
    parser.add_argument('--batch', default=100, help='Size of batch assigned to each process')
    args = parser.parse_args()
    args.id = 0 # Default process ID

    copy_premerge_buckets(args)