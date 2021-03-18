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
import sys
import os
import json
import time
from google.cloud import bigquery
from utilities.bq_helpers import load_BQ_from_json
from bq.gen_analysis_results_table.schema import analysis_results_metadata_schema
from utilities.tcia_scrapers import scrape_tcia_analysis_collections_page


# # Build a table of all the DOIs in a particular release
# def build_DOI_list(args):
#     with open(args.third_party_DOIs_file) as f:
#         third_party_DOIs = json.load(f)
#     with open(args.exclude) as f:
#         excluded = f.read().splitlines()
#     DOIs = []
#     for collection in third_party_DOIs:
#         for map in third_party_DOIs[collection]:
#             # DOI = map["SourceDOI"].split('doi.org/')[1]
#             DOI = map["SourceDOI"]
#             if not DOI in DOIs:
#                 if not DOI in excluded:
#                     DOIs.append(DOI)
#     return DOIs

# Return a list of the source_dois of analysis results wikis in some version
def get_analysis_results_dois(client, args):
    # Get all the source DOIs in this version
    query = f"""
        SELECT distinct se.source_doi as source_doi
        FROM `{args.project}.{args.bqdataset_name}.{args.bq_version_table}` as v
        JOIN `{args.project}.{args.bqdataset_name}.{args.bq_collection_table}` as c
        ON v.id = c.version_id
        JOIN `{args.project}.{args.bqdataset_name}.{args.bq_patient_table}` as p
        ON c.id = p.collection_id
        JOIN `{args.project}.{args.bqdataset_name}.{args.bq_study_table}` as st
        ON p.id = st.patient_id
        JOIN `{args.project}.{args.bqdataset_name}.{args.bq_series_table}` as se
        ON st.id = se.study_id

        WHERE v.id = {args.version}
        """
    result = client.query(query).result()
    all_source_dois = [series['source_doi'] for series in result]

    # Get the source DOIs of the original data collections
    query = f"""
        SELECT DOI
        FROM `{args.project}.{args.bqdataset_name}.{args.bq_original_collections_metadata_table}`
        """
    result = client.query(query).result()
    original_source_dois = [collection['DOI'] for collection in result]
    analysis_results_dois = set(all_source_dois) - set(original_source_dois)
    return analysis_results_dois

def build_metadata(args, source_dois):
    # Scrape the TCIA Data Collections page for collection metadata
    collection_metadata = scrape_tcia_analysis_collections_page()

    # # Get a list of the DOIs of collections that analyzed some set of data collections
    # DOIs = build_DOI_list(args)

    rows = []
    for collection_id, collection_data in collection_metadata.items():
        # print(collection_id)
        # if collection_data["DOI"].split('doi.org/')[1] in DOIs:
        if collection_data["DOI"] in source_dois:
            collection_data["Collection"] = collection_id
            # collection_data["DOI"] = collection_data["DOI"].split('doi.org/')[1]
            rows.append(json.dumps(collection_data))
    metadata = '\n'.join(rows)
    return metadata

def gen_collections_table(args):
    BQ_client = bigquery.Client()
    source_dois = get_analysis_results_dois(BQ_client, args)

    metadata = build_metadata(args, source_dois)
    job = load_BQ_from_json(BQ_client, args.project, args.bqdataset_name, args.bqtable_name, metadata, analysis_results_metadata_schema)
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
    parser.add_argument('--bqtable_name', default='analysis_results_metadata', help='BQ table name')
    parser.add_argument('--bq_version_table', default='version', help='BQ table from which to get versions')
    parser.add_argument('--bq_collection_table', default='collection', help='BQ table from which to get collections in version')
    parser.add_argument('--bq_patient_table', default='patient', help='BQ table from which to get patients in version')
    parser.add_argument('--bq_study_table', default='study', help='BQ table from which to get study in version')
    parser.add_argument('--bq_series_table', default='series', help='BQ table from which to get series in version')
    parser.add_argument('--bq_original_collections_metadata_table', default='original_collections_metadata',
                        help='BQ original collections metadata table')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    gen_collections_table(args)