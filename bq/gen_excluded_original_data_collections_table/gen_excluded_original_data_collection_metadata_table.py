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
import json
import time
from google.cloud import bigquery
from utilities.bq_helpers import load_BQ_from_json
from bq.gen_original_data_collections_table.schema import data_collections_metadata_schema
from utilities.tcia_helpers import get_collection_descriptions_and_licenses, get_collection_license_info
from utilities.tcia_scrapers import scrape_tcia_data_collections_page

def get_collections_in_version(client, args):
    query = f"""
    SELECT c.* 
    FROM `{args.src_project}.{args.bqdataset_name}.{args.bq_collection_table}` as c
    JOIN `{args.src_project}.{args.bqdataset_name}.{args.bq_excluded_collections}` as ex
    ON LOWER(c.collection_id) = LOWER(ex.tcia_api_collection_id)
    ORDER BY c.collection_id
    """
    result = client.query(query).result()
    collection_ids = [collection['collection_id'] for collection in result]
    return collection_ids


def get_cases_per_collection(client, args):
    query = f"""
    SELECT
      c.collection_id,
      COUNT(DISTINCT p.submitter_case_id ),
    FROM
      `{args.src_project}.{args.bqdataset_name}.collection` as c
    JOIN
      `{args.src_project}.{args.bqdataset_name}.patient` as p 
    ON c.collection_id = p.collection_id
    GROUP BY
        c.collection_id
    """

    case_counts = {c.values()[0].lower(): c.values()[1] for c in client.query(query).result()}
    return case_counts


def build_metadata(client, args, collection_ids):
    # Get collection descriptions and license IDs from TCIA
    collection_descriptions = get_collection_descriptions_and_licenses()

    # We report our case count rather than counts from the TCIA wiki pages.
    case_counts = get_cases_per_collection(client, args)

    # Get a list of the licenses used by data collections
    licenses = get_collection_license_info()

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
                collection_data['Description'] = collection_descriptions[collection_id]['description']
                collection_data['Subjects'] = case_counts[collection_id.lower()]
                collection_data['LicenseURL'] = licenses[collection_id]['licenseURL']
                collection_data['LicenseLongName'] = licenses[collection_id]['longName']
                collection_data['LicenseShortName'] = licenses[collection_id]['shortName']
                # collection_data['LicenseURL'] = \
                #     licenses[collection_descriptions[collection_id]['licenseId']-1]['licenseURL']
                # collection_data['LicenseLongName'] = \
                #     licenses[collection_descriptions[collection_id]['licenseId'] - 1]['longName']
                # collection_data['LicenseShortName'] = \
                #     licenses[collection_descriptions[collection_id]['licenseId'] - 1]['shortName']

            elif collection_data['tcia_api_collection_id'] in collection_descriptions:
                collection_data['Description'] = collection_descriptions[collection_data['tcia_api_collection_id']]['description']
                collection_data['Subjects'] = case_counts[collection_data['tcia_api_collection_id'].lower()]
                collection_data['LicenseURL'] = \
                    licenses[collection_data['tcia_api_collection_id']]['licenseURL']
                collection_data['LicenseLongName'] = \
                    licenses[collection_data['tcia_api_collection_id']]['longName']
                collection_data['LicenseShortName'] = \
                    licenses[collection_data['tcia_api_collection_id']]['shortName']
                # collection_data['LicenseURL'] = \
                #     licenses[collection_descriptions[collection_data['tcia_api_collection_id']]['licenseId'] - 1][
                #         'licenseURL']
                # collection_data['LicenseLongName'] = \
                #     licenses[collection_descriptions[collection_data['tcia_api_collection_id']]['licenseId'] - 1][
                #         'longName']
                # collection_data['LicenseShortName'] = \
                #     licenses[collection_descriptions[collection_data['tcia_api_collection_id']]['licenseId'] - 1][
                #         'shortName']
            else:
                collection_data['Description'] = ""
                collection_data['LicenseURL'] = ""
                collection_data['LicenseLongName'] = ""
                collection_data['LicenseShortName'] = ""

            rows.append(json.dumps(collection_data))
        else:
            print(f'{collection_id} not in IDC collections')

    # Make sure we found metadata for all out collections
    for collection in collection_ids:
        if not collection in found_ids:
            print(f'****No metadata for {collection}')

    metadata = '\n'.join(rows)
    return metadata

def gen_excluded_collections_table(args):
    BQ_client = bigquery.Client(project=args.src_project)
    collection_ids = get_collections_in_version(BQ_client, args)

    metadata = build_metadata(BQ_client, args, collection_ids)
    job = load_BQ_from_json(BQ_client, args.dst_project, args.bqdataset_name, args.bqtable_name, metadata,
                            data_collections_metadata_schema, write_disposition='WRITE_TRUNCATE')
    while not job.state == 'DONE':
        print('Status: {}'.format(job.state))
        time.sleep(args.period * 60)
    print("{}: Completed collections metatdata upload \n".format(time.asctime()))

if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--version', default=3, help='IDC version for which to build the table')
    args = parser.parse_args()
    parser.add_argument('--src_project', default='idc-dev-etl')
    parser.add_argument('--dst_project', default='idc-dev-etl')
    parser.add_argument('--bqdataset_name', default=f'idc_v{args.version}', help='BQ dataset name')
    parser.add_argument('--bqtable_name', default='excluded_collections_metadata', help='BQ table name')
    parser.add_argument('--bq_collection_table', default='collection', help='BQ table from which to get collections in version')
    parser.add_argument('--bq_excluded_collections', default='excluded_collections', help='BQ table from which to get collections to exclude')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    gen_excluded_collections_table(args)