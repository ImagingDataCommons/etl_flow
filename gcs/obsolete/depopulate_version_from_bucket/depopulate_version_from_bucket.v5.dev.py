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

import argparse
import os
from google.cloud import bigquery
import logging
from logging import INFO, DEBUG

from gcs.depopulate_version_from_bucket.depopulate_versions_from_bucket import predelete

def get_collections_in_version(args):
    client = bigquery.Client()
    query = f"""
        SELECT o.tcia_api_collection_id as tcia_api_collection_id
        FROM `idc-dev-etl.idc_v5.open_collections` AS o
--        ORDER BY o.tcia_api_collection_id 
        UNION ALL
        SELECT c.tcia_api_collection_id
        FROM `idc-dev-etl.idc_v5.cr_collections` AS c
--        ORDER BY c.tcia_api_collection_id 
        UNION ALL
        SELECT d.tcia_api_collection_id
        FROM `idc-dev-etl.idc_v5.defaced_collections` AS d
--         UNION ALL
--         SELECT d.tcia_api_collection_id
--         FROM `idc-dev-etl.idc_v5.redacted_collections` AS d
        ORDER BY tcia_api_collection_id
--        ORDER BY d.tcia_api_collection_id 

        """
    result = client.query(query).result()
    collection_ids = [collection['tcia_api_collection_id'] for collection in result]
    return collection_ids



if __name__ == '__main__':
    bucket = 'idc-open'
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=5, help='Next version to generate')
    args = parser.parse_args()
    parser.add_argument('--bqdataset_name', default=f'idc_v{args.version}')
    parser.add_argument('--bucket', default=f'{bucket}', help='Bucket from which to delete instances')
    parser.add_argument('--processes', default=8, help="Number of concurrent processes")
    parser.add_argument('--batch', default=100, help='Size of batch assigned to each process')
    parser.add_argument('--project', default='canceridc-data')
    parser.add_argument('--deleted_version', default=3, help='Version whose instances are to be deleted')
    args = parser.parse_args()
    parser.add_argument('--log_dir', default=f'/mnt/disks/idc-etl/logs/depopulate_version_{args.deleted_version}_from_{bucket}')
    parser.add_argument('--dones', default=f'./logs/depopulate_v{args.deleted_version}_dones.txt')
    args = parser.parse_args()

    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler(f'{os.environ["PWD"]}/logs/log.log')
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    proglogger = logging.getLogger('root.prog')
    prog_fh = logging.FileHandler(f'{os.environ["PWD"]}/logs/prog.log')
    progformatter = logging.Formatter('%(levelname)s:prog:%(message)s')
    proglogger.addHandler(prog_fh)
    prog_fh.setFormatter(progformatter)
    proglogger.setLevel(INFO)

    successlogger = logging.getLogger('success')
    successlogger.setLevel(DEBUG)

    errlogger = logging.getLogger('root.err')

    collections = get_collections_in_version(args)

    predelete(args, collections)

