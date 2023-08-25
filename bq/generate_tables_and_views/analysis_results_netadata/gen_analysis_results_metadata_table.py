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
from bq.generate_tables_and_views.analysis_results_netadata.schema import analysis_results_metadata_schema
from utilities.tcia_scrapers import scrape_tcia_analysis_collections_page
from utilities.logging_config import successlogger, progresslogger
from python_settings import settings

# Build the analysis_results_metadata BQ table

# # Get the access status of redactable collections
# # Note this assumes that analysis is only against tcia supplied data (radiology) data.
# def get_redacted_collections(client,args):
#     query = f"""
#     SELECT tcia_api_collection_id, tcia_access as access
#     FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.redacted_collections`
#     """
#     redacted_collection_access = {c.tcia_api_collection_id.lower().replace(' ','_').replace('-','_'): c.access for c in client.query(query).result()}
#     return redacted_collection_access
#
# Get the license associated with a particular source DOI
def get_license(client, doi):
    query = f"""
    SELECT distinct license_url, license_long_name, license_short_name
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined`
    WHERE lower(source_doi) = lower('{doi}')
    """
    licenses = [dict(row) for row in client.query(query)]
    try:
        assert len(licenses) == 1
    except Exception as exc:
        exit
    return licenses[0]

# Get all source DOIs, licenses and the collections which they are in
def get_collections_containing_a_doi(client, args):
    query = f"""
        SELECT DISTINCT collection_id AS collection_id, source_doi
        FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined`
        WHERE idc_version = {settings.CURRENT_VERSION} 
        AND license_short_name in ('CC BY 3.0', 'CC BY 4.0', 'CC BY-NC 3.0', 'CC BY-NC 4.0')
        ORDER BY collection_id
        """
    result = client.query(query).result()

    # Generate a dictionary indexed by doi, and where value is associated with collection(s)
    source_dois = {}
    for row in result:
        if row['source_doi']:
            collection_id = row['collection_id'].lower().replace(' ','_').replace('-','_')
            if row['source_doi'].lower() not in source_dois:
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


def get_idc_sourced_analysis_metadata(client):
    query = f"""
    SELECT DISTINCT *
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.analysis_results_metadata_idc_source`
    """

    results = [dict(row) for row in client.query(query).result()]
    metadata = {f'{row["Title"]} ({row["ID"]})':row for row in results}
    return metadata


def get_all_analysis_metadata(client):
    # Scrape the TCIA analysis results page for metadata
    analysis_metadata = get_idc_sourced_analysis_metadata(client)
    analysis_metadata = analysis_metadata | scrape_tcia_analysis_collections_page()

def build_metadata(args, BQ_client):

    all_idc_analysis_metadata = get_idc_sourced_analysis_metadata(BQ_client)
    idc_analysis_metadata = {title: {'DOI':data['DOI'], 'CancerType':data['CancerType'], \
            'Location':data['Location'], 'Subjects':data['Subjects'], 'Collections':data['Collections'], \
            'AnalysisArtifactsonTCIA':data['AnalysisArtifacts'], 'Updated':data['Updated']} \
            for title, data in all_idc_analysis_metadata.items()}
    analysis_metadata = idc_analysis_metadata | scrape_tcia_analysis_collections_page()

    # Get analysis results descriptions
    descriptions = get_descriptions(BQ_client, args)

    # # Get access status of potentially redacted collections
    # redacted_collection_access = get_redacted_collections(BQ_client,args)

    # For each source DOI, which collections contain it
    source_dois_collections = get_collections_containing_a_doi(BQ_client, args)

    rows = []
    for analysis_id, analysis_data in analysis_metadata.items():
        # If the DOI of this analysis result is in source_dois_license, then it is in the series table
        # and therefore we have a series from this analysis result, and therefore we should include
        # this analysis result in the analysis_results metadata table
        if analysis_data["DOI"].lower() in source_dois_collections:
            # analysis_data["Collection"] = analysis_id
            title_id = analysis_id.rsplit('(',1)
            title = title_id[0]
            if title.endswith(' '):
                title = title[:-1]
            analysis_data['Title'] = title
            analysis_data['ID'] = title_id[1].split(')')[0]
            analysis_data['Collections'] = source_dois_collections[analysis_data['DOI']]
            analysis_data['Access'] = 'Public'
            license = get_license(BQ_client, analysis_data["DOI"])
            for key, value in license.items():
                analysis_data[key] = value

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