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
import os
import sys
import json
from google.cloud import bigquery
from utilities.bq_helpers import load_BQ_from_json, delete_BQ_Table
from bq.bq_utilities import dataframe_to_bq, get_github_directory_contents_from_comet, \
    get_data_from_comet
from utilities.tcia_helpers import get_tcia_collection_manager_data
from utilities.logging_config import progresslogger, errlogger
from python_settings import settings
from bq.bq_utilities import read_json_to_dataframe
import requests
import pandas as pd

data_collections_metadata_schema = [
    bigquery.SchemaField('collection_name', 'STRING', mode='REQUIRED', description='Collection name as used externally by IDC webapp'),
    bigquery.SchemaField('collection_id', 'STRING', mode='REQUIRED', description='Collection ID as used internally by IDC webapp'),
    # bigquery.SchemaField('collection_title', 'STRING', mode='REQUIRED',
    #                      description='Descriptive title of this collection'),
    bigquery.SchemaField('cancer_types', 'STRING', mode='REQUIRED', description='Cancer types in this collection '),
    bigquery.SchemaField('tumor_locations', 'STRING', mode='REQUIRED',
                         description='Tumor locations in this collection'),
    bigquery.SchemaField('subjects', 'INTEGER', mode='REQUIRED', description='Number of subjects in this collection'),
    bigquery.SchemaField('species', 'STRING', mode='REQUIRED', description="Species of collection subjects"),
    bigquery.SchemaField(
        "sources",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField('source_id', 'STRING', mode='NULLABLE', description='collection_id or analysis_result_id of this source'),
            bigquery.SchemaField('source_title', 'STRING', mode='REQUIRED',
                                 description='Descriptive title of this source'),
            bigquery.SchemaField('source_type', 'STRING', mode='NULLABLE', description='"original collection" or "analysis result"'),
            bigquery.SchemaField('source_doi', 'STRING', mode='NULLABLE',
                                 description='DOI that can be resolved at doi.org to a information page of this source'),
            bigquery.SchemaField('source_url', 'STRING', mode='REQUIRED',
                                 description='URL of the information page of this sourc'),
            bigquery.SchemaField('modalities', 'STRING', mode='NULLABLE',
                                 description='URL of the information page of this source'),
            bigquery.SchemaField(
                "license",
                "RECORD",
                fields=[
                    bigquery.SchemaField('license_url', 'STRING', mode='REQUIRED',
                                         description='URL of license of this (sub)collection'),
                    bigquery.SchemaField('license_long_name', 'STRING', mode='REQUIRED',
                                         description='Long name of license of this (sub)collection'),
                    bigquery.SchemaField('license_short_name', 'STRING', mode='REQUIRED',
                                         description='Short name of license of this (sub)collection')
                ]
            ),
            bigquery.SchemaField('citation', 'STRING', mode='NULLABLE',
                                 description='Citation to be used for this source'),
            bigquery.SchemaField('Access', 'STRING', mode='NULLABLE', description='DEPRECATED: All IDC data is public'),
            bigquery.SchemaField('ImageTypes', 'STRING', mode='NULLABLE',
                                 description='DEPRECATED: Duplicate of modalities'),
        ],
        description='Array of metadata for each source of instance data in this collection'
    ),
    bigquery.SchemaField('supporting_data', 'STRING', mode='NULLABLE', description='Type(s) of addional available data'),
    bigquery.SchemaField('program_id', 'STRING', mode='REQUIRED', description='Program to which this collection belongs'),
    bigquery.SchemaField('status', 'STRING', mode='NULLABLE', description='Collection status: Ongoing or Complete'),
    bigquery.SchemaField('updated', 'DATE', mode='NULLABLE', description='Date of most recent update'),
    bigquery.SchemaField('description', 'STRING', mode='REQUIRED', description='Description of collection (HTML format)'),
    # Deprecations
    # bigquery.SchemaField('Title', 'STRING', mode='REQUIRED',
    #                      description='Deprecated: Duplicate of collection_title'),
    bigquery.SchemaField('CancerTypes', 'STRING', mode='REQUIRED', description='DEPRECATED: Duplicate of cancer_types'),
    bigquery.SchemaField('TumorLocations', 'STRING', mode='REQUIRED',
                         description='DEPRECATED: Duplicate of tumor_locations'),
    bigquery.SchemaField('SupportingData', 'STRING', mode='NULLABLE',
                         description='DEPRECATED: Duplicate of supporting_data'),
    bigquery.SchemaField('Program', 'STRING', mode='REQUIRED',
                         description='DEPRECATED: Duplicate of program_id'),

]

# Count the cases (patients) in each collection
def add_case_counts(client, args, collection_metadata):
    query = f"""
    SELECT
      collection_id collection_name,
      COUNT(DISTINCT submitter_case_id ) as cases,
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_public_and_current`
    GROUP BY
        collection_id
    """

    case_counts = {c['collection_name']: c['cases'] for c in client.query(query).result()}
    for collection in collection_metadata:
        try:
            collection_metadata[collection]['subjects'] = case_counts[collection]
        except Exception as exc:
            errlogger.error(f'No case counts for {collection}')
            collection_metadata[collection]['subjects'] = ""

    return collection_metadata


def add_descriptions(client, args, collection_metadata):
    query = f"""
    SELECT * 
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.original_collections_end_user_descriptions`
    ORDER BY collection_id
    """

    descriptions = {}
    for row in client.query(query).result():
        descriptions[row['collection_id']] = dict(
            description = row['description']
        )
    for collection, metadata in collection_metadata.items():
        try:
            metadata['description'] = descriptions[collection.lower().replace('-','_').replace(' ','_')]['description']
        except Exception as exc:
            errlogger.error(f'No description for {collection}: {exc}')
        # collection_metadata[collection]['Description'] = ""
    progresslogger.info('Added descriptions')
    return collection_metadata


# Get the set of modalities per collection, source
def add_modalities(client, collection_metadata):
    query = f"""
  SELECT distinct aj.collection_id, aj.source_doi, aj.source_url, string_agg(DISTINCT Modality, ', ' ORDER BY Modality) modalities
  FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_EXT_DATASET}.dicom_metadata` dm
  JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_public_and_current` aj
  ON dm.SOPInstanceUID = aj.sop_instance_uid
  GROUP BY aj.source_url, aj.collection_id, aj.source_url, aj.source_doi
  ORDER BY aj.collection_id    
    """

    modalities = {}
    for row in client.query(query).result():
        collection_name = row['collection_id']
        if collection_name in modalities:
            modalities[collection_name][row['source_url']] = row['modalities']
        else:
            modalities[collection_name] = {row['source_url']:  row['modalities']}

    for collection_name, metadata in collection_metadata.items():
        for source in metadata['sources']:
            try:
                source['modalities'] = modalities[collection_name][source['source_url'].lower()]
                source['ImageTypes'] = modalities[collection_name][source['source_url'].lower()]
            except:
                errlogger.error(f'No modality for {collection_name}, {source["source_url"].lower()}')
    progresslogger.info('Added modalities')
    return collection_metadata


def add_analysis_results_sources(client, all_collections_metadata):
    analysis_results_data = {}
    analysis_results_files = get_github_directory_contents_from_comet("collections/analysis", args.comet_branch)
    for file in analysis_results_files:
        data = get_data_from_comet(f"collections/analysis/{file}", branch=args.comet_branch)
        analysis_results_data[data['source_doi']] = data

    # Generate a table of the sources of each collection
    query = f"""
    SELECT DISTINCT collection_id collection_name, source_doi
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_public_and_current` 
"""
    for row in client.query(query).result():
        if row["source_doi"] in analysis_results_data:
            collection_metadata  = all_collections_metadata[row['collection_name']]
            source = analysis_results_data[row['source_doi']]
            collection_metadata['sources'].append(
                dict(
                    source_id=source['analysis_result_id'].lower().replace('-', '_').replace(' ', '_'),
                    source_title=source['title'],
                    source_type='analysis_result',
                    source_doi=source['source_doi'],
                    source_url=source['source_url'],
                    modalities='',
                    license=dict(
                        license_url=source['license']['url'],
                        license_long_name=source['license']['long_name'],
                        license_short_name=source['license']['short_name']
                    ),
                    citation=source['citation'],
                    Access='Public',
                    ImageTypes=''
                )
            )
    return all_collections_metadata


def generate_collection_metadata():
    all_collection_metadata = {}
    collection_files = get_github_directory_contents_from_comet("collections/original", args.comet_branch)
    for collection_file in collection_files:
        data = get_data_from_comet(f"collections/original/{collection_file}", branch=args.comet_branch)
        try:
            collection_metadata = dict(
                collection_name=data['collection_name'],
                collection_id=data['collection_id'],
                cancer_types=', '.join(data['cancer_types']),
                tumor_locations=', '.join(data['tumor_locations']),
                subjects = 0,
                species=', '.join(data['species']) if type(data['species'])==list else data['species'],
                sources=[],
                supporting_data= "" if data['supporting_data'] is None else ', '.join(data['supporting_data']),
                program_id = data['program'],
                status=data['status'],
                updated=data['updated'],
                description = "",
                CancerTypes=', '.join(data['cancer_types']),
                TumorLocations=', '.join(data['tumor_locations']),
                SupportingData="" if data['supporting_data'] is None else ', '.join(data['supporting_data']),
                Program=data['program']
            )
        except Exception as exc:
            pass
        for source in data['sources']:
            collection_metadata['sources'].append(
                dict(
                    source_id = data['collection_id'],
                    source_title = source['title'],
                    source_type = 'original_data',
                    source_doi = source['concept_doi'] if 'concept_doi' in source else source['source_doi'],
                    source_url = f"https://doi.org/{source['concept_doi']}" if 'concept_doi' in source else source['source_url'],
                    modalities = '',
                    license = dict(
                        license_url = source['license']['url'],
                        license_long_name = source['license']['long_name'],
                        license_short_name = source['license']['short_name']
                    ),
                    citation = source['citation'],
                    Access = 'Public',
                    ImageTypes = ''
                )
            )
        all_collection_metadata[data['collection_name']] = collection_metadata
    return all_collection_metadata


def build_metadata(client, args):
    # Get metadata for both collections and analysis results
    # all_metadata = get_all_metadata()
    # Now get most of the metadata for all collections
    collection_metadata = generate_collection_metadata()


    # Add sources dois
    collection_metadata = add_analysis_results_sources(client, collection_metadata)
    collection_metadata = add_descriptions(client, args, collection_metadata)
    collection_metadata = add_case_counts(client, args, collection_metadata)
    collection_metadata = add_modalities(client, collection_metadata)

    # Convert dictionary to list of dicts
    metadata = [value for value in collection_metadata.values()]
    return metadata


def main(args):
    BQ_client = bigquery.Client(project=settings.DEV_PROJECT)

    if args.use_cached_metadata:
        with open(args.cached_metadata_file) as f:
            all_metadata = json.load(f)
    else:
        all_metadata = build_metadata(BQ_client, args)
        with open(args.cached_metadata_file, 'w') as f:
            json.dump(all_metadata, f)

    # Drop any collections that do not have any sources. This is probably only needed during development
    metadata = [row for row in all_metadata if len(row['sources']) > 0]
    metadata_1 = metadata
    pass
    metadata_json = '\n'.join([json.dumps(row) for row in
                        sorted(metadata_1, key=lambda d: d['collection_name'])])
    try:
        pass
        delete_BQ_Table(BQ_client, settings.DEV_PROJECT, settings.BQ_DEV_EXT_DATASET, args.bqtable_name)
        load_BQ_from_json(BQ_client,
                    settings.DEV_PROJECT,
                    settings.BQ_DEV_EXT_DATASET, args.bqtable_name, metadata_json,
                                data_collections_metadata_schema, write_disposition='WRITE_TRUNCATE')
        return
    except Exception as exc:
        errlogger.error(f'Table creation failed: {exc}')
    exit


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bqtable_name', default='original_collections_metadata', help='BQ table name')
    parser.add_argument("--comet_branch", default = 'release/v24')
    parser.add_argument('--use_cached_metadata', default=False)
    parser.add_argument('--cached_metadata_file', default='cached_included_metadata.json', help='Where to cache metadata')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    main(args)