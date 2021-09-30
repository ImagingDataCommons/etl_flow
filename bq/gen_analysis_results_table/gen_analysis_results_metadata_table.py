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

# Build the analysis_results_metadata BQ table

# Get all source DOIS and collections they are in
def get_all_idc_dois(client, args):
    query = f"""
        SELECT DISTINCT c.collection_id AS collection_id, se.source_doi AS source_doi 
        FROM `{args.src_project}.{args.bqdataset_name}.{args.bq_collection_table}` AS c
        JOIN `{args.src_project}.{args.bqdataset_name}.{args.bq_patient_table}` AS p
        ON c.collection_id = p.collection_id
        JOIN `{args.src_project}.{args.bqdataset_name}.{args.bq_study_table}` AS st
        ON p.submitter_case_id = st.submitter_case_id
        JOIN `{args.src_project}.{args.bqdataset_name}.{args.bq_series_table}` AS se
        ON st.study_instance_uid = se.study_instance_uid
        LEFT JOIN `{args.src_project}.{args.bqdataset_name}.{args.bq_excluded_collections}` AS ex
        ON LOWER (c.collection_id) = LOWER(ex.tcia_api_collection_id)
        WHERE ex.tcia_api_collection_id IS NULL
        """

    result = client.query(query).result()
    source_dois = {}
    for row in result:
        collection_id = row['collection_id'].lower().replace(' ','_').replace('-','_')
        if row['source_doi'] not in source_dois:
            source_dois[row['source_doi']] = [collection_id]
        else:
            source_dois[row['source_doi']].append(collection_id)

    for doi in source_dois:
        source_dois[doi] = ','.join(source_dois[doi])

    return source_dois


def build_metadata(args, BQ_client):
    # Get all source DOIS and collections they are in
    source_dois = get_all_idc_dois(BQ_client, args)

    # Scrape the TCIA analysis results page for metadata
    analysis_metadata = scrape_tcia_analysis_collections_page()


    rows = []
    for analysis_id, analysis_data in analysis_metadata.items():
        # If the DOI of this analysis result is in source_dois, then it is in the series table
        # and therefore we have a series from this analysis result, and therefor we should include
        # this analysis result in the analysis_results metadata table
        if analysis_data["DOI"] in source_dois:
            # analysis_data["Collection"] = analysis_id
            title_id = analysis_id.rsplit('(',1)
            title = title_id[0]
            if title.endswith(' '):
                title = title[:-1]
            analysis_data['Title'] = title
            analysis_data['ID'] = title_id[1].split(')')[0]
            analysis_data['Collections'] = source_dois[analysis_data['DOI']]
            rows.append(json.dumps(analysis_data))
    metadata = '\n'.join(rows)
    return metadata

def gen_collections_table(args):
    BQ_client = bigquery.Client(project=args.src_project)
    metadata = build_metadata(args, BQ_client)
    job = load_BQ_from_json(BQ_client, args.dst_project, args.bqdataset_name, args.bqtable_name, metadata, analysis_results_metadata_schema,
                            write_disposition='WRITE_TRUNCATE')
    while not job.state == 'DONE':
        print('Status: {}'.format(job.state))
        time.sleep(args.period * 60)
    print("{}: Completed collections metatdata upload \n".format(time.asctime()))

if __name__ == '__main__':
    parser =argparse.ArgumentParser()

    parser.add_argument('--version', default=4, help='IDC version for which to build the table')
    args = parser.parse_args()
    parser.add_argument('--src_project', default='idc-dev-etl')
    parser.add_argument('--dst_project', default='idc-dev-etl')
    parser.add_argument('--bqdataset_name', default=f'idc_v{args.version}_dev', help='BQ dataset name')
    parser.add_argument('--bqtable_name', default='analysis_results_metadata', help='BQ table name')
    parser.add_argument('--bq_collection_table', default='collection', help='BQ table from which to get collections in version')
    parser.add_argument('--bq_patient_table', default='patient', help='BQ table from which to get patients in version')
    parser.add_argument('--bq_study_table', default='study', help='BQ table from which to get study in version')
    parser.add_argument('--bq_series_table', default='series', help='BQ table from which to get series in version')
    parser.add_argument('--bq_excluded_collections', default='excluded_collections', help='BQ table from which to get collections to exclude')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    gen_collections_table(args)