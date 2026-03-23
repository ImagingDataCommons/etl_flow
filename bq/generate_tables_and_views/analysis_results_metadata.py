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
from bq.bq_utilities import create_temp_table_from_df
import pandas as pd
from google.cloud import bigquery
from utilities.bq_helpers import load_BQ_from_json
from utilities.tcia_helpers import get_tcia_collection_manager_data
from utilities.logging_config import successlogger, progresslogger, errlogger

import settings
import requests

analysis_results_metadata_schema = [
    bigquery.SchemaField('analysis_result_name', 'STRING', mode='REQUIRED', description='Analysis result name as used externally by IDC webapp'),
    bigquery.SchemaField('analysis_result_id', 'STRING', mode='REQUIRED', description='Analysis result ID as used internally by IDC webapp'),
    bigquery.SchemaField('analysis_result_title', 'STRING', mode='REQUIRED', description='Descriptive title of this analysis result'),
    bigquery.SchemaField('source_doi','STRING', mode='NULLABLE', description='DOI that can be resolved at doi.org to an information page'),
    bigquery.SchemaField('source_url','STRING', mode='REQUIRED', description='URL of a wiki page'),
    bigquery.SchemaField('cancer_types','STRING', mode='REQUIRED', description='Type(s) of cancer analyzed'),
    bigquery.SchemaField('tumor_locations', 'STRING', mode='REQUIRED', description='Body location that was analyzed'),
    bigquery.SchemaField('subjects', 'INTEGER', mode='REQUIRED', description='Number of subjects whose data was analyzed'),
    bigquery.SchemaField('collections', 'STRING', mode='REQUIRED', description='collection_names of original data collections analyzed'),
    bigquery.SchemaField('modalities', 'STRING', mode='REQUIRED', description='Modalities of this analysis result'),
    bigquery.SchemaField('updated', 'DATE', mode='REQUIRED', description='Most recent update reported by TCIA'),
    bigquery.SchemaField('license_url', 'STRING', mode='REQUIRED', description='URL of license of this analysis result'),
    bigquery.SchemaField('license_long_name', 'STRING', mode='REQUIRED', description='Long name of license of this analysis result'),
    bigquery.SchemaField('license_short_name', 'STRING', mode='REQUIRED', description='Short name of license of this analysis result'),
    bigquery.SchemaField('description', 'STRING', mode='REQUIRED',
                         description='Description of this analysis result'),
    bigquery.SchemaField('citation', 'STRING', mode='NULLABLE',
                         description='Citation to be used for this analysis result'),
    # Deprecations
    bigquery.SchemaField('ID', 'STRING', mode='REQUIRED',
                         description='DEPRECATED: Duplicate of analysis_result_name'),
    bigquery.SchemaField('Title', 'STRING', mode='REQUIRED', description='DEPRECATED: Duplicate of analysis_result_title'),
    bigquery.SchemaField('CancerTypes', 'STRING', mode='REQUIRED', description='DEPRECATED: Duplicate of cancer_types'),
    bigquery.SchemaField('TumorLocations', 'STRING', mode='REQUIRED', description='DEPRECATED: Duplicate of tumor_locations'),
    bigquery.SchemaField('Access', 'STRING', mode='REQUIRED', description='DEPRECATED: Access is always Public'),
]


def get_descriptions(client,args):
    query = f"""
    SELECT ID, Description
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.analysis_results_end_user_descriptions` 
    """
    descriptions = {c.ID: c.Description for c in client.query(query).result()}
    return descriptions


def get_idc_sourced_analysis_result_metadata(client):
    query = f"""
--     SELECT DISTINCT ID, Title, Access, source_doi, Updated
    SELECT DISTINCT analysis_result_name, analysis_result_title, source_doi, updated
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.analysis_results_metadata_idc_source`
    """
    metadata = [dict(row) for row in client.query(query).result()]
    # metadata = {row["source_doi"]:row for row in results}
    return metadata


# Get a list of subjects per source_doi
def get_citation(source_url):
    header = {"Accept": "text/x-bibliography; style=elsevier-vancouver-no-et-al"}
    citation = requests.get(source_url, headers=header).text
    if citation.startswith("<!DOCTYPE html>"):
        errlogger.error(f'No citation for {source_url}')
        exit(1)
    return citation


# Get collection-level metadata: CancerTypes, TumorLocations, modalities, Subjects, CollectionsAnalysed, for
# each analysis result
# analysis_metadata includes all TCIA analysis results, not just those that are in IDC. We need to deal with that.
# 1. Load analysis_metadata, loaded from spreadsheet, into a temp BQ table
# 2. Remove analysis results that IDC does not have
# 3. Get cumulative metadata for each remaining AR
def get_analysis_results_metadata(client, analysis_metadata):
    schema = [
        bigquery.SchemaField("analysis_result_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("analysis_result_title", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("source_doi", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("updated", "STRING", mode="REQUIRED"),
    ]
    table_name = 'gen_analysis_results_metadata'
    table_id = f"{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.{table_name}"
    df = pd.DataFrame(analysis_metadata)
    create_temp_table_from_df(client, table_id, schema, df, expire_in_minutes=30)

    query = f"""
WITH ocm AS (
--   Flatten 
  SELECT * except(Updated, sources, CancerTypes, TumorLocations, Subjects)
  FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_EXT_DATASET}.original_collections_metadata` ocm, 
  UNNEST(sources) AS srcs, UNNEST(SPLIT(CancerTypes, ',')) as CTypes, 
  UNNEST(SPLIT(TumorLocations, ',')) TLocations, 
  UNNEST(SPLIT(srcs.ImageTypes, ',')) ITypes
)
,
s1 AS (
      SELECT DISTINCT garm.*, ocm.collection_id, ocm.CTypes, ocm.TLocations, ocm.ITypes 
      FROM ocm
      JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.{table_name}` garm
      ON ocm.source_doi = garm.source_doi
 )

SELECT DISTINCT 
    etl_functions.name_to_id(analysis_result_name) analysis_result_id, 
    analysis_result_name, 
    analysis_result_title, 
    s1.source_doi source_doi, 
    CONCAT("https://doi.org/", s1.source_doi) source_url,  
    STRING_AGG(DISTINCT TRIM(s1.CTypes, ' '), ", " ORDER BY TRIM(CTypes, ' ')) cancer_types,
    STRING_AGG(DISTINCT TRIM(TLocations, ' '), ", " ORDER BY TRIM(TLocations, ' ')) tumor_locations,
    COUNT(DISTINCT ajpac.submitter_case_id) subjects,
    STRING_AGG( DISTINCT TRIM(s1.collection_id, ' '), ", " ORDER BY TRIM(s1.collection_id, ' ')) collections,
    STRING_AGG( DISTINCT TRIM(Modality, ' '), ", " ORDER BY TRIM(Modality, ' ')) modalities,
    updated,
    license_url, license_long_name, license_short_name,
    "Public" Access,
    analysis_result_name ID, 
    analysis_result_title Title, 
    STRING_AGG(DISTINCT TRIM(s1.CTypes, ' '), ", " ORDER BY TRIM(CTypes, ' ')) CancerTypes,
    STRING_AGG(DISTINCT TRIM(TLocations, ' '), ", " ORDER BY TRIM(TLocations, ' ')) TumorLocations,
    FROM s1
    JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_public_and_current` ajpac
    ON s1.source_doi = ajpac.source_doi
    JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_EXT_DATASET}.dicom_metadata` dm
    ON ajpac.sop_instance_uid = dm.SOPInstanceUID
    GROUP BY ID, Title, source_doi, source_url, Updated, license_url, license_long_name, license_short_name
    ORDER BY ID
    """

    results = {row['source_doi']:dict(row) for row in client.query(query)}
    return results
        
def get_tcia_sourced_analysis_result_metadata(BQ_client):
    tcia_ars = get_tcia_collection_manager_data('analysis-results')
    ar_metadata = []
    for ar in tcia_ars:
        ar_metadata.append(
            dict(
            analysis_result_name = ar['result_short_title'],
            analysis_result_title = ar['result_title'],
            # Access = ar['result_page_accessibility'],
            source_doi = ar['result_doi'].lower(),
            updated = ar['date_updated'],
            )
        )
    return ar_metadata

def build_metadata(args, BQ_client):
    # Get some basic metadata for each tcia-sourced and idc-sourced analysis result
    tcia_analysis_metadata = get_tcia_sourced_analysis_result_metadata(BQ_client)
    idc_analysis_metadata = get_idc_sourced_analysis_result_metadata(BQ_client)
    all_analysis_results = idc_analysis_metadata + tcia_analysis_metadata

    analysis_results_metadata = get_analysis_results_metadata(BQ_client, all_analysis_results)
    # Get analysis results descriptions
    descriptions = get_descriptions(BQ_client, args)

    rows = []
    for source_doi, analysis_data in analysis_results_metadata.items():
        try:
            analysis_data['description'] = descriptions[analysis_data['ID']]
        except Exception as exc:
            errlogger.error(f'No description found for {analysis_data["ID"]}')

        analysis_data['Citation'] = get_citation(analysis_data['source_url'])
        rows.append(json.dumps(analysis_data))
    return rows

def gen_table(args):
    BQ_client = bigquery.Client(project=settings.DEV_PROJECT)
    rows = build_metadata(args, BQ_client)
    metadata = '\n'.join(rows)
    try:
        job = load_BQ_from_json(BQ_client, settings.DEV_PROJECT, settings.BQ_DEV_EXT_DATASET, args.bqtable_name, metadata, analysis_results_metadata_schema,
            write_disposition='WRITE_TRUNCATE',
            table_description='Metadata of Analysis Results. These are the results of analysis performed against Original Collections hosted by IDC.')
        while not job.state == 'DONE':
            progresslogger.info('Status: {}'.format(job.state))
            time.sleep(args.period * 60)
        successlogger.info(f"{time.asctime()}: Completed {args.bqtable_name}")
        return
    except Exception as exc:
        print(f'Error {exc}')
    exit(1)

if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--bqtable_name', default='analysis_results_metadata', help='BQ table name')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    gen_table(args)