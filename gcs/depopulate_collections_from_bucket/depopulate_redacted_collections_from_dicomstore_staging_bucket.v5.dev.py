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
Delete collections from the idc-dev-v5-dicomstore-staging bucket.
This is/was used for generating a bucket for dicom store import tocreate a dicomstore
without redacted collections.
Buckets for import into a dicomstore do not have retired instances, thus retired
instances are not deleted.
"""


import argparse
import os
from google.cloud import bigquery
import logging
from logging import INFO

from gcs.depopulate_collections_from_bucket.depopulate_collections_from_bucket import predelete

def get_collections_in_version(args):
    client = bigquery.Client()
    query = f"""
        SELECT c.* 
        FROM `idc-dev-etl.idc_v5.{args.bq_collections_table}` AS c
         ORDER BY c.tcia_api_collection_id 
        """
    result = client.query(query).result()
    collection_ids = [collection['tcia_api_collection_id'] for collection in result]
    return collection_ids



if __name__ == '__main__':
    group = 'redacted'
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=5, help='Next version to generate')
    args = parser.parse_args()
    parser.add_argument('--db', default=f'idc_v{args.version}')
    parser.add_argument('--bqdataset_name', default=f'idc_v{args.version}')
    parser.add_argument('--bq_collections_table', default=f'{group}_collections', help='Table listing collections in group')
    parser.add_argument('--retired', default=False, help="Copy retired instances in collection if True")
    parser.add_argument('--src_bucket', default='idc-dev-v5-dicomstore-staging', help='Bucket from which to delete instances')
    parser.add_argument('--processes', default=128, help="Number of concurrent processes")
    parser.add_argument('--batch', default=1000, help='Size of batch assigned to each process')
    parser.add_argument('--src_project', default='idc-dev-etl')
    parser.add_argument('--dst_project', default='idc-dev-etl')
    parser.add_argument('--log_dir', default=f'/mnt/disks/idc-etl/logs/depopulate_collections_from_dicomstore_staging_bucket')
    args = parser.parse_args()
    parser.add_argument('--dones', default=f'./logs/depopulate_{group}_bucket_from_{args.src_bucket}_v{args.version}_dones.txt')
    args = parser.parse_args()

    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler(f'{os.environ["PWD"]}/logs/{group}_dicomstore_staging_buckets.log')
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    successlogger = logging.getLogger('success')
    successlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')

    collections = get_collections_in_version(args)

    predelete(args, collections)

