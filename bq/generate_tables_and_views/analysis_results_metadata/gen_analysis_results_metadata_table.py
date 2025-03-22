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

# Build the analysis_results_metadata BQ table

import argparse
import sys
import os
import json
import time
from re import split as re_split
from google.cloud import bigquery
from utilities.bq_helpers import load_BQ_from_json
from bq.generate_tables_and_views.analysis_results_metadata.schema import analysis_results_metadata_schema
from utilities.tcia_helpers import get_all_tcia_metadata
from utilities.logging_config import successlogger, progresslogger
# from python_settings import settings
import settings
import requests



# Get the licenses associated with each source_doi
def get_licenses(client):
    query = f"""
SELECT *
FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.licenses`
"""
    licenses = {row['source_doi']: dict(row) for row in client.query(query)}
    return licenses


# Get all source_dois and the collections which they are in
def get_collections_containing_a_doi(client, args):
    query = f"""
        SELECT DISTINCT collection_id AS collection_id, source_doi, source_url
        FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_public_and_current`
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
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.analysis_results_descriptions_end_user` 
    """
    descriptions = {c.ID: c.Description for c in client.query(query).result()}
    return descriptions


def get_idc_sourced_analysis_metadata(client):
    query = f"""
--     SELECT DISTINCT ID, Title, Access, DOI as source_doi, CancerType as CancerTypes, Location as CancerLocations, AnalysisArtifacts, Updated 
    SELECT DISTINCT ID, Title, Access, source_doi, CancerType as CancerTypes, Location as TumorLocations, AnalysisArtifacts, Updated
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.analysis_results_metadata_idc_source`
    """
    results = [dict(row) for row in client.query(query).result()]
    metadata = {row["source_doi"]:row for row in results}
    return metadata


# Get a list of subjects per source_doi
def count_subjects(client):
    query = f"""
SELECT source_doi, COUNT (DISTINCT submitter_case_id) cnt
FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_public_and_current`
GROUP BY source_doi
"""

    results = [dict(row) for row in client.query(query).result()]
    counts = {row['source_doi'].lower(): row['cnt'] for row in results}
    return counts


def get_url(url, headers=""):  # , headers):
    result =  requests.get(url, headers=headers)  # , headers=headers)
    if result.status_code != 200:
        raise RuntimeError('In get_url(): status_code=%s; url: %s', result.status_code, url)
    return result


def get_citation(source_doi, source_url):
    if source_doi:
        if 'zenodo' in source_doi:
            breakpoint()
            params = {'access_token': settings.ZENODO_ACCESS_TOKEN}
        else:
            params = {}
        header = {"Accept": "text/x-bibliography; style=apa"}
        # citation = get_url(source_url, header).text
        citation = requests.get(source_url, headers=header, params=params).text
    else:
        citation =source_url

    return citation


def get_tcia_sourced_analysis_metadata(BQ_client):
    tcia_ars = get_all_tcia_metadata('analysis-results')
    ar_metadata = {}
    for ar in tcia_ars:
        ar_metadata[ar['result_doi'].lower()] = dict(
            ID = ar['result_short_title'],
            Title = ar['result_title'],
            Access = ar['result_page_accessibility'],
            source_doi = ar['result_doi'],
            CancerTypes = ', '.join(ar['cancer_types']) if ar['cancer_types'] else "" ,
            TumorLocations = ', '.join(ar['cancer_locations']) if ar['cancer_locations'] else "",
            AnalysisArtifacts = ', '.join(ar['supporting_data']) if ar['supporting_data'] else "",
            Updated = ar['date_updated'],
        )
    return ar_metadata

def build_metadata(args, BQ_client):
    tcia_analysis_metadata = get_tcia_sourced_analysis_metadata(BQ_client)
    idc_analysis_metadata = get_idc_sourced_analysis_metadata(BQ_client)
    analysis_metadata = idc_analysis_metadata | tcia_analysis_metadata

    # Get licenses and analysis results descriptions
    licenses = get_licenses(BQ_client)
    descriptions = get_descriptions(BQ_client, args)

     # For each source DOI, which collections contain it
    source_dois_collections = get_collections_containing_a_doi(BQ_client, args)

    counts_per_doi = count_subjects(BQ_client)

    rows = []
    for source_doi, analysis_data in analysis_metadata.items():
        # If the DOI of this analysis result is in source_dois_collections, then it is in the series table
        # and therefore we have a series from this analysis result, and therefore we should include
        # this analysis result in the analysis_results metadata table
        if source_doi.lower() in source_dois_collections:
            analysis_data['source_url'] =f'https://doi.org/{source_doi}'
            try:
                analysis_data['Collections'] = source_dois_collections[source_doi]
            except Exception as exc:
                print(f"Didn't have counts for source_doi {source_doi}" )
            analysis_data['Subjects'] = counts_per_doi[source_doi]
            license = licenses[source_doi]
            for key, value in license['license'].items():
                analysis_data[key] = value
            analysis_data['Description'] = descriptions[analysis_data['ID']]
            analysis_data['AnalysisArtifactsonTCIA'] = analysis_data['AnalysisArtifacts']
            analysis_data['DOI'] = analysis_data['source_doi']
            analysis_data['CancerType'] = analysis_data['CancerTypes']
            analysis_data['Location'] = analysis_data['TumorLocations']
            analysis_data['Citation'] = get_citation(analysis_data['source_doi'], analysis_data['source_url'])
            rows.append(json.dumps(analysis_data))
    metadata = '\n'.join(rows)
    return metadata

def gen_collections_table(args):
    BQ_client = bigquery.Client(project=settings.DEV_PROJECT)
    metadata = build_metadata(args, BQ_client)
    try:
        job = load_BQ_from_json(BQ_client, settings.DEV_PROJECT, settings.BQ_DEV_EXT_DATASET, args.bqtable_name, metadata, analysis_results_metadata_schema,
            write_disposition='WRITE_TRUNCATE',
            table_description='Metadata of Analysis Results. These are the results of analysis performed against Original Collections hosted by IDC.')
    except Exception as exc:
        print(f'Error {exc}')
        exit
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