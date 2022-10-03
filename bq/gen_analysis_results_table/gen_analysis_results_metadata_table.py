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
from utilities.logging_config import successlogger, progresslogger
from python_settings import settings

# Build the analysis_results_metadata BQ table

# Get the access status of redactable collections
# Note this assumes that analysis is only against tcia supplied data (radiology) data.
def get_redacted_collections(client,args):
    query = f"""
    SELECT tcia_api_collection_id, tcia_access as access
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.redacted_collections` 
    """
    redacted_collection_access = {c.tcia_api_collection_id.lower().replace(' ','_').replace('-','_'): c.access for c in client.query(query).result()}
    return redacted_collection_access

# Get all source DOIs and the collections which they are in
def get_all_idc_dois(client, args):
    # query = f"""
    #     SELECT DISTINCT c.collection_id AS collection_id, se.source_doi AS source_doi
    #     FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.version` AS v
    #     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.version_collection` as vc
    #     ON v.version = vc.version
    #     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.collection` AS c
    #     ON vc.collection_uuid = c.uuid
    #     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.collection_patient` AS cp
    #     ON c.uuid = cp.collection_uuid
    #     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.patient` AS p
    #     ON cp.patient_uuid = p.uuid
    #     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.patient_study` AS ps
    #     ON p.uuid = ps.patient_uuid
    #     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.study` AS st
    #     ON ps.study_uuid = st.uuid
    #     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.study_series` AS ss
    #     ON st.uuid = ss.study_uuid
    #     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.series` AS se
    #     ON ss.series_uuid = se.uuid
    #     LEFT JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.excluded_collections` AS ex
    #     ON LOWER (c.collection_id) = LOWER(ex.tcia_api_collection_id)
    #     WHERE ex.tcia_api_collection_id IS NULL AND v.version = {settings.CURRENT_VERSION}
    #     """
    query = f"""
        SELECT DISTINCT collection_id AS collection_id, source_doi AS source_doi
        FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_included`
        WHERE idc_version = {settings.CURRENT_VERSION} 
        """
    result = client.query(query).result()

    # Generate a dictionary indexed by doi, and where value is associated collection(s)
    source_dois = {}
    for row in result:
        collection_id = row['collection_id'].lower().replace(' ','_').replace('-','_')
        if row['source_doi'] not in source_dois:
            source_dois[row['source_doi'].lower()] = [collection_id]
        else:
            source_dois[row['source_doi'].lower()].append(collection_id)

    for doi in source_dois:
        source_dois[doi] = ','.join(source_dois[doi])

    return source_dois

def get_descriptions(client,args):
    query = f"""
    SELECT ID, Description
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.analysis_results_descriptions` 
    """
    descriptions = {c.ID: c.Description for c in client.query(query).result()}
    return descriptions

def build_metadata(args, BQ_client):
    # Get analysis results descriptions
    descriptions = get_descriptions(BQ_client, args)

    # # Get access status of potentially redacted collections
    # redacted_collection_access = get_redacted_collections(BQ_client,args)

    # Get all source DOIS and collections they are in
    source_dois = get_all_idc_dois(BQ_client, args)

    # Scrape the TCIA analysis results page for metadata
    analysis_metadata = scrape_tcia_analysis_collections_page()

    rows = []
    for analysis_id, analysis_data in analysis_metadata.items():
        # If the DOI of this analysis result is in source_dois, then it is in the series table
        # and therefore we have a series from this analysis result, and therefor we should include
        # this analysis result in the analysis_results metadata table
        if analysis_data["DOI"].lower() in source_dois:
            # analysis_data["Collection"] = analysis_id
            title_id = analysis_id.rsplit('(',1)
            title = title_id[0]
            if title.endswith(' '):
                title = title[:-1]
            analysis_data['Title'] = title
            analysis_data['ID'] = title_id[1].split(')')[0]
            analysis_data['Collections'] = source_dois[analysis_data['DOI']]
            analysis_data['Access'] = 'Public'
            # for collection in analysis_data["Collections"].split(','):
            #     if collection in redacted_collection_access:
            #         analysis_data['Access'] = redacted_collection_access[collection]
            analysis_data['Description'] = descriptions[analysis_data['ID']]
            rows.append(json.dumps(analysis_data))
    metadata = '\n'.join(rows)
    return metadata

def gen_collections_table(args):
    BQ_client = bigquery.Client(project=settings.DEV_PROJECT)
    metadata = build_metadata(args, BQ_client)
    job = load_BQ_from_json(BQ_client, settings.DEV_PROJECT, settings.BQ_DEV_EXT_DATASET, args.bqtable_name, metadata, analysis_results_metadata_schema,
        write_disposition='WRITE_TRUNCATE',
        table_description='Metadata of Analysis Results. These are the results of analysis performed against Original Collections hosted by IDC.')
    while not job.state == 'DONE':
        progresslogger.info('Status: {}'.format(job.state))
        time.sleep(args.period * 60)
    successlogger.info(f"{time.asctime()}: Completed {args.bqtable_name}")

if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--bqtable_name', default='analysis_results_metadata', help='BQ table name')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    gen_collections_table(args)