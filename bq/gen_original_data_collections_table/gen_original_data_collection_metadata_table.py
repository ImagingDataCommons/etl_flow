#!/usr/bin/env
#
# Copyright 2020, Institute for Systems Biology
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
import os
import json
import time
from google.cloud import bigquery
from utilities.bq_helpers import load_BQ_from_json, query_BQ
from bq.gen_original_data_collections_table.schema import data_collections_metadata_schema
from utilities.tcia_helpers import get_collection_descriptions
from utilities.tcia_scrapers import scrape_tcia_data_collections_page

def get_collections_in_version(client, args):
    query = f"""
    SELECT c.* 
    FROM `{args.project}.{args.bqdataset_name}.{args.bq_version_table}` as v
    JOIN `{args.project}.{args.bqdataset_name}.{args.bq_collection_table}` as c
    ON v.id = c.version_id
    LEFT JOIN `{args.project}.{args.bqdataset_name}.{args.bq_excluded_collections}` as ex
    ON LOWER(c.tcia_api_collection_id) = LOWER(ex.tcia_api_collection_id)
    WHERE v.id = {args.version} AND ex.tcia_api_collection_id IS NULL
    ORDER BY c.tcia_api_collection_id
    """
    result = client.query(query).result()
    collection_ids = [collection['tcia_api_collection_id'] for collection in result]
    return collection_ids


def build_metadata(args, collection_ids):
    # Get collection descriptions from TCIA
    collection_descriptions = get_collection_descriptions()

    # Scrape the TCIA Data Collections page for collection metadata
    collection_metadata = scrape_tcia_data_collections_page()

    rows = []
    found_ids = []
    lowered_collection_ids = {collection_id.lower():collection_id for collection_id in collection_ids}
    for collection_id, collection_data in collection_metadata.items():
        if collection_id.lower() in lowered_collection_ids:
            found_ids.append(lowered_collection_ids[collection_id.lower()])
            collection_data['tcia_api_collection_id'] = lowered_collection_ids[collection_id.lower()]
            collection_data['idc_webapp_collection_id'] = collection_id.lower().replace(' ','_').replace('-','_')
            if collection_id in collection_descriptions:
                collection_data['Description'] = collection_descriptions[collection_id]
            elif collection_data['tcia_api_collection_id'] in collection_descriptions:
                collection_data['Description'] = collection_descriptions[collection_data['tcia_api_collection_id']]
            rows.append(json.dumps(collection_data))
        else:
            print(f'{collection_id} not in IDC collections')

    # Make sure we found metadata for all out collections
    for collection in collection_ids:
        if not collection in found_ids:
            print(f'****No metadata for {collection}')

    metadata = '\n'.join(rows)
    return metadata

def gen_collections_table(args):
    BQ_client = bigquery.Client()
    collection_ids = get_collections_in_version(BQ_client, args)

    metadata = build_metadata(args, collection_ids)
    job = load_BQ_from_json(BQ_client, args.project, args.bqdataset_name, args.bqtable_name, metadata,
                            data_collections_metadata_schema, write_disposition='WRITE_TRUNCATE')
    while not job.state == 'DONE':
        print('Status: {}'.format(job.state))
        time.sleep(args.period * 60)
    print("{}: Completed collections metatdata upload \n".format(time.asctime()))

if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--project', default='idc-dev-etl')
    parser.add_argument('--version', default=2, help='IDC version for which to build the table')
    args = parser.parse_args()
    parser.add_argument('--bqdataset_name', default=f'idc_v{args.version}', help='BQ dataset name')
    parser.add_argument('--bqtable_name', default='original_collections_metadata', help='BQ table name')
    parser.add_argument('--bq_version_table', default='version', help='BQ table from which to get versions')
    parser.add_argument('--bq_collection_table', default='collection', help='BQ table from which to get collections in version')
    parser.add_argument('--bq_excluded_collections', default='excluded_collections', help='BQ table from which to get collections to exclude')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    gen_collections_table(args)