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

from bq.bq_utilities import get_github_directory_contents_from_comet, \
    get_data_from_comet

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
    bigquery.SchemaField('ImageTypes', 'STRING', mode='REQUIRED', description='Deprecated: Duplicate of modalities'),
]


def generate_analysis_results_metadata():
    all_analysis_metadata = {}
    analysis_files = get_github_directory_contents_from_comet("collections/analysis", args.comet_branch)
    for analysis_file in analysis_files:
        data = get_data_from_comet(f"collections/analysis/{analysis_file}", branch=args.comet_branch)
        analysis_metadata = dict(
            analysis_result_name=data['analysis_result_name'],
            analysis_result_id=data['analysis_result_id'],
            analysis_result_title=data['title'],
            source_doi=data['source_doi'],
            source_url=data['source_url'],
            cancer_types=', '.join(data['cancer_types']),
            tumor_locations=', '.join(data['tumor_locations']),
            subjects=0,
            collections=', '.join(data['collections']),
            modalities="",
            updated=data['updated'],
            license_url=data['license']['url'],
            license_long_name=data['license']['long_name'],
            license_short_name=data['license']['short_name'],
            description="",
            citation=data['citation'],
            ID=data['analysis_result_id'],
            Title=data['title'],
            CancerTypes=', '.join(data['cancer_types']),
            TumorLocations=', '.join(data['tumor_locations']),
            Access="Public"
        )
        all_analysis_metadata[data['analysis_result_id']] = analysis_metadata
    return all_analysis_metadata


def add_descriptions(client, analysis_results_metadata):
    query = f"""
    SELECT * 
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.analysis_results_end_user_descriptions`
    ORDER BY id
    """

    descriptions = {}
    for row in client.query(query).result():
        descriptions[row['id']] = dict(
            description = row['description']
        )
    for result, metadata in analysis_results_metadata.items():
        try:
            metadata['description'] = descriptions[result]['description']
        except Exception as exc:
            errlogger.error(f'No description for {result}: {exc}')
        # collection_metadata[collection]['Description'] = ""
    progresslogger.info('Added descriptions')
    return analysis_results_metadata

# Get the set of modalities per analysis_result
def add_modalities(client, analysis_metadata):
    query = f"""
  SELECT distinct aj.source_doi, string_agg(DISTINCT Modality, ', ' ORDER BY Modality) modalities
  FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_EXT_DATASET}.dicom_metadata` dm
  JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_public_and_current` aj
  ON dm.SOPInstanceUID = aj.sop_instance_uid
  GROUP BY aj.source_doi
  ORDER BY aj.source_doi    
    """

    modalities = {}
    for row in client.query(query).result():
        modalities[row['source_doi']] = row['modalities']

    for analysis, metadata in analysis_metadata.items():
        try:
            metadata['modalities'] = modalities[metadata['source_doi'].lower()]
            metadata['ImageTypes'] = modalities[metadata['source_doi'].lower()]
        except:
            errlogger.error(f'No modality for {metadata["analysis_result_name"]}')
    progresslogger.info('Added modalities')
    return analysis_metadata


# Count the cases (patients) in each collection
def add_case_counts(client, analysis_metadata):
    query = f"""
    SELECT
      source_doi,
      COUNT(DISTINCT submitter_case_id ) as cases,
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_public_and_current`
    GROUP BY source_doi
    """

    patients = {c['source_doi']: c['cases'] for c in client.query(query).result()}
    for analysis, metadata in analysis_metadata.items():
        try:
            metadata['subjects'] = patients[metadata['source_doi'].lower()]
        except:
            errlogger.error(f"No case for {analysis}")
    progresslogger.info('Added modalities')
    return analysis_metadata



def build_metadata(args, BQ_client):
    # Get some basic metadata for each tcia-sourced and idc-sourced analysis result
    analysis_results_metadata = generate_analysis_results_metadata()

    # analysis_results_metadata = get_analysis_results_metadata(BQ_client, all_analysis_results)
    # Get analysis results descriptions
    # descriptions = get_descriptions(BQ_client, args)
    analysis_results_metadata = add_descriptions(BQ_client, analysis_results_metadata)
    analysis_results_metadata = add_modalities(BQ_client, analysis_results_metadata)
    analysis_results_metadata = add_case_counts(BQ_client, analysis_results_metadata)

    return [metadata for id,metadata in analysis_results_metadata.items()]



def gen_table(args):
    BQ_client = bigquery.Client(project=settings.DEV_PROJECT)
    rows = build_metadata(args, BQ_client)
    rows_1 = rows
    metadata = '\n'.join([json.dumps(row) for row in rows_1])
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
        errlogger.error(f'Error {exc}')
    exit(1)

if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--bqtable_name', default='analysis_results_metadata', help='BQ table name')
    parser.add_argument("--comet_branch", default = 'release/v24')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    gen_table(args)