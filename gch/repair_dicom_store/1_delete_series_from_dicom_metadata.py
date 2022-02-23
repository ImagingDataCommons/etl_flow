#
# Copyright 2015-2022, Institute for Systems Biology
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
import sys
import json
import time
from google.cloud import bigquery
from utilities.bq_helpers import load_BQ_from_json
from bq.gen_original_data_collections_table.schema import data_collections_metadata_schema
from utilities.tcia_helpers import get_collection_descriptions_and_licenses, get_collection_license_info
from utilities.tcia_scrapers import scrape_tcia_data_collections_page

def get_instances_in_series(args, client, SeriesInstanceUID):
    query = f"""
        SELECT SOPInstanceUID
        FROM `{args.src_project}.{args.dev_bqdataset_name}.dicom_metadata`
        WHERE SeriesInstanceUID = '{SeriesInstanceUID}'"""
    instances = [row['SOPInstanceUID'] for row in client.query(query).result()]
    return instances

def delete_instance(args, client, SOPInstanceUID):
    query = f"""
        DELETE 
        FROM `{args.src_project}.{args.dev_bqdataset_name}.dicom_metadata`
        WHERE SOPInstanceUID = '{SOPInstanceUID}'"""

    result = client.query(query).result

def delete_series(args):
    for series in args.series:
        instances = get_instances_in_series(args,args.client, series)

        for instance in instances:
            delete_instance(args, args.client, instance)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=8, help='Version to work on')
    parser.add_argument('--client', default=bigquery.Client())
    args = parser.parse_args()
    parser.add_argument('--src_project', default='idc-dev-etl')
    parser.add_argument('--dev_bqdataset_name', default=f'idc_v{args.version}_dev')
    parser.add_argument('--series', default=['1.3.6.1.4.1.5962.99.1.1284409015.247096130.1637666981559.2.0',
                                             '1.3.6.1.4.1.5962.99.1.3426307341.632503471.1639808879885.2.0'], help='A list of SeriesInstanceUIDs to be deleted')
    parser.add_argument('--log_dir', default=f'/mnt/disks/idc-etl/logs/delete_series_from_dicom_metadata')
    args = parser.parse_args()
    args.id = 0  # Default process ID

    delete_series(args)
