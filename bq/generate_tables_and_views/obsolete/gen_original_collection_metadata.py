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
# from bq.generate_tables_and_views.original_collections_metadata.schema import data_collections_metadata_schema
from utilities.tcia_helpers import get_tcia_collection_manager_data
from utilities.logging_config import progresslogger, errlogger
from python_settings import settings
from bq.bq_utilities import read_json_to_dataframe
import requests
import pandas as pd

data_collections_metadata_schema = [
    bigquery.SchemaField('collection_name', 'STRING', mode='REQUIRED', description='Collection name as used externally by IDC webapp'),
    bigquery.SchemaField('collection_id', 'STRING', mode='REQUIRED', description='Collection ID as used internally by IDC webapp'),
    bigquery.SchemaField('collection_title', 'STRING', mode='REQUIRED',
                         description='Descriptive title of this collection'),
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
            bigquery.SchemaField('access', 'STRING', mode='NULLABLE', description='DEPRECATED: All IDC data is public'),
            bigquery.SchemaField('ImageTypes', 'STRING', mode='NULLABLE',
                                 description='DEPRECATED: Duplicate of modalities'),
        ],
        description='Array of metadata for each source of instance data in this collection'
    ),
    bigquery.SchemaField('supporting_data', 'STRING', mode='NULLABLE', description='Type(s) of addional available data'),
    bigquery.SchemaField('program', 'STRING', mode='REQUIRED', description='Program to which this collection belongs'),
    bigquery.SchemaField('status', 'STRING', mode='NULLABLE', description='Collection status: Ongoing or Complete'),
    bigquery.SchemaField('updated', 'DATE', mode='NULLABLE', description='Date of most recent update'),
    bigquery.SchemaField('description', 'STRING', mode='REQUIRED', description='Description of collection (HTML format)'),
    # Deprecations
    bigquery.SchemaField('Title', 'STRING', mode='REQUIRED',
                         description='Deprecated: Duplicate of collection_title'),
    bigquery.SchemaField('CancerTypes', 'STRING', mode='REQUIRED', description='DEPRECATED: Duplicate of cancer_types'),
    bigquery.SchemaField('TumorLocations', 'STRING', mode='REQUIRED',
                         description='DEPRECATED: Duplicate of tumor_locations'),
    bigquery.SchemaField('SupportingData', 'STRING', mode='NULLABLE',
                         description='DEPRECATED: Duplicate of supporting_data'),
]


def add_programs(client, args, collection_metadata):
    query = f"""
        SELECT *
        FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.collection_program_map`"""
    programs = {row['collection_id'].lower(): row['program'] for row in client.query(query).result()}
    for collection_name, metadata in collection_metadata.items():
        try:
            metadata["program"] = programs[metadata['collection_id']]
        except Exception as exc:
            errlogger.error(f'No program for {collection_name}')
            metadata["program"] = ""

    return collection_metadata

# Generate a dict of collections in the current version,
# indexed by "collection_name". Includes whether collection is sources from tcia,
# or idc or both,
def get_collections_in_version(client, args):
    # Return collections that have specified access
    query = f"""
    SELECT DISTINCT collection_id as collection_name, REPLACE(REPLACE(LOWER(collection_id),'-','_'),' ','_') collection_id, se_sources.tcia tcia_source, se_sources.idc idc_source
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_public_and_current`
    """

    # data = {row.collection_id: dict(row.items()) for row in client.query(query)}
    data = [row for row in client.query(query)]
    collections = {}
    for row in data:
        if not row['collection_name'] in collections:
            collections[row['collection_name']] = dict(row)
        else:
            # If we found a second entry for a collection, it must be because the collection
            # has data from both tcia and idc
            collections[row['collection_name']]['tcia_source'] = True
            collections[row['collection_name']]['idc_source'] = True
    return collections

# Count the cases (patients) in each collection
def add_case_counts(client, args, collection_metadata):
    query = f"""
    SELECT
      collection_id,
      COUNT(DISTINCT submitter_case_id ) as cases,
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_public_and_current`
    GROUP BY
        collection_id
    """

    case_counts = {c['collection_id']: c['cases'] for c in client.query(query).result()}
    for collection in collection_metadata:
        try:
            collection_metadata[collection]['subjects'] = case_counts[collection]
        except Exception as exc:
            errlogger.error(f'No case counts for {collection}')
            collection_metadata[collection]['subjects'] = ""

    return collection_metadata

# # Generate a per-collection list of the modalities across all instances in each collection
# def add_image_modalities(client, args, collection_metadata):
#     query = f"""
#       SELECT DISTINCT
#         REPLACE(REPLACE(LOWER(collection_id),'-','_'),' ','_') AS idc_webapp_collection_id,
#         STRING_AGG(DISTINCT modality, ", " ORDER BY modality) ImageTypes
#       FROM
#         `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.all_joined_public_and_current`
#       JOIN
#         `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_pub.dicom_metadata`
#      ON
#         sop_instance_uid = SOPInstanceUID
#       GROUP BY
#         idc_webapp_collection_id
#       ORDER BY
#         idc_webapp_collection_id  """
#
#     imageTypes = {c['idc_webapp_collection_id'].lower().replace(' ','_').replace('-','_'): c['ImageTypes'] for c in client.query(query).result()}
#     for collection in collection_metadata:
#         collection_metadata[collection]['modalities'] = imageTypes[collection]
#         collection_metadata[collection]['ImageTypes'] = imageTypes[collection]
    return collection_metadata


# Get metadata like that from the TCIA data collections page for collections that TCIA doesn't have
def get_original_collections_metadata_idc_source(client, args):
    query = f"""
    SELECT * 
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.original_collections_metadata_idc_source`
    ORDER BY collection_id
    """
    idc_sourced_original_collections_metadata = read_json_to_dataframe(f'{settings.PROJECT_PATH}/bq/generate_tables_and_views/table_generation_jsons/idc_original_collections_metadata.json5')

    idc_only_metadata = {}
    for index, row in idc_sourced_original_collections_metadata.iterrows():
        if row['idc_only'] == True:
            idc_only_metadata[row['collection_name']] = dict(
                collection_name=row['collection_name'],
                collection_id=row['collection_id'],
                collection_title=row['title'],
                cancer_types=row['CancerTypes'],
                tumor_locations=row['TumorLocations'],
                # Subjects = 0,
                species=row['Species'],
                sources=[],
                supporting_data=row['SupportingData'],
                status=row['Status'],
                updated = None,
                # Deprecations
                Title = row['title'],
                CancerTypes = row['CancerTypes'],
                TumorLocations = row['TumorLocations'],
                SupportingData=row['SupportingData'],
            )
    return idc_only_metadata

def get_url(url, headers=""):  # , headers):
    result =  requests.get(url, headers=headers)  # , headers=headers)
    if result.status_code != 200:
        raise RuntimeError('In get_url(): status_code=%s; url: %s', result.status_code, url)
    return result


def get_citation(source_url):
    header = {"Accept": "text/x-bibliography; style=elsevier-vancouver-no-et-al"}
    citation = requests.get(source_url, headers=header).text
    if 'This DOI cannot be found in the DOI System' in citation:
        errlogger.error(f'Unable to get citation for {source_url}')
        citation = ""

    return citation


def get_original_collections_metadata_tcia_source(client, args, idc_collections):
    tcia_collection_metadata = get_tcia_collection_manager_data('collections')
    metadata = {}
    for collection_name, values  in idc_collections.items():
        # Find the collection manager entry corresponding to a collection that IDC has
        try:
            collection_metadata = next(collection for collection in tcia_collection_metadata \
                        if collection_name == collection['collection_short_title'])
        except Exception as exc:
            errlogger.error(f'No collection manager data for {collection_name}')
            exit(1)

        try:
            metadata[collection_name] = dict(
                collection_name=collection_name,
                collection_id=values['collection_id'],
                collection_title=collection_metadata['collection_title'],
                cancer_types=", ".join(collection_metadata['cancer_types']) \
                    if isinstance(collection_metadata['cancer_types'], list) else '',
                tumor_locations=", ".join(collection_metadata['cancer_locations']) \
                    if isinstance(collection_metadata['cancer_locations'], list) else '',
                subjects=0,
                species=", ".join(collection_metadata['species']) \
                    if isinstance(collection_metadata['species'], list) else '',
                sources = [],
                supporting_data=", ".join(collection_metadata['supporting_data']) \
                    if isinstance(collection_metadata['supporting_data'], list) else '',
                status=collection_metadata['collection_status'],
                updated=None,
                # Deprecations
                Title=collection_metadata['collection_title'],
                CancerTypes=", ".join(collection_metadata['cancer_types']) \
                    if isinstance(collection_metadata['cancer_types'], list) else '',
                TumorLocations=", ".join(collection_metadata['cancer_locations']) \
                    if isinstance(collection_metadata['cancer_locations'], list) else '',
                SupportingData=", ".join(collection_metadata['supporting_data']) \
                    if isinstance(collection_metadata['supporting_data'], list) else '',
            )
        except Exception as exc:
            print(exc)

    return metadata

def get_idc_sourced_original_collections_metadata():
    with open(f'{settings.PROJECT_PATH}/bq/generate_tables_and_views/table_generation_jsons/idc_original_collections_metadata.json') as f:
        json_string = f.read()
    idc_sourced_original_collections_metadata = pd.read_json(json_string)
    return idc_sourced_original_collections_metadata


def get_idc_sourced_analysis_results_metadata():
    with open(f'{settings.PROJECT_PATH}/bq/generate_tables_and_views/table_generation_jsons/idc_analysis_results_metadata.json') as f:
        json_string = f.read()
    idc_sourced_original_collections_metadata = pd.read_json(json_string)
    return idc_sourced_original_collections_metadata


# Get metadata of all the collections in this version.
# For each collection, determine whether collection level metadata (as opposed to per-source metadata) is sourced
# from tcia or idc. We get this collection level metadata from tcia if we get radiology or pathology or both from
# tcia. Otherwise, we get collection level metadata from idc maintained table/file.

def get_collection_metadata(client, args):
    # Get a list of all the collections that we have in this version
    idc_collections = get_collections_in_version(client, args)

    # Get metadata for each collection for which we source radiology data or pathology data or both from TCIA.
    idc_sourced_original_collections_metadata = read_json_to_dataframe(f'{settings.PROJECT_PATH}/bq/generate_tables_and_views/table_generation_jsons/idc_original_collections_metadata.json5')
    # Now remove from idc_collections, any collection for which we do not get any data from TCIA
    tcia_sourced_collections = idc_collections
    for index, row in idc_sourced_original_collections_metadata.iterrows():
        if row['idc_only'] == True:
            try:
                del tcia_sourced_collections[row['collection_name']]
            except:
                errlogger.error(f'Collection {row["collection_name"]} not in idc_collections')
    # Get metadata for each of these
    tcia_sourced_collections = get_original_collections_metadata_tcia_source(client, args, tcia_sourced_collections)

    # Get metadata of two classes of collections: those for which we source data from both tcia and idc,
    # and those for which we source data only from idc.
    idc_only_collections = get_original_collections_metadata_idc_source(client, args)

    # Merge the TCIA collection metadata.
    collection_metadata = tcia_sourced_collections
    collection_metadata |= idc_only_collections

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


# Get a list of the licenses associated with each collection
def add_licenses(client, doi, collection_metadata):
    # Get the licenses associated with a source_url. We assume that there is only one distinct licenses
    query = f"""
    SELECT DISTINCT source_doi, STRUCT(license_url, license_long_name, license_short_name) license
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.licenses`
    """
    licenses = {row['source_doi']: row['license'] for row in client.query(query)}

    for collection, metadata in collection_metadata.items():
        for source in metadata['sources']:
            try:
                source["license"] = licenses[source['source_doi']]
            except Exception as exc:
                errlogger.error(f'No license for {collection}, {source["source_doi"]}: {exc}')
                source["license"] = {"license_doi": "", "license_long_name": "", "license_short_name": ""}
    progresslogger.info('Added licenses')
    return collection_metadata

def add_citations(collection_metadata):
    for collection, data in collection_metadata.items():
        for source in data['sources']:
            if source['source_doi']:
                try:
                    citation = get_citation(source['source_url'])
                except Exception as exc:
                    errlogger.error(f'Error getting citation for {collection},{source["source_url"]}:  {exc}')
                    citation = source['source_url']
            else:
                citation = source['source_url']
            source['citation'] = citation

    progresslogger.info('Added citations')
    return collection_metadata

# Get the set of modalities per collection for each of TCIA and IDC.
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


def add_updates(client,collection_metadata):
    query = f"""
    SELECT c.collection_id collection_name, vm.version_timestamp
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_EXT_DATASET}.version_metadata` vm
    JOIN  `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.collection` c
    ON vm.idc_version = c.rev_idc_version
    WHERE c.final_idc_version=0
    """
    timestamps = {c['collection_name']:c['version_timestamp'] for c in client.query(query).result()}
    for collection_name in collection_metadata:
        try:
            collection_metadata[collection_name]['updated'] = timestamps[collection_name]
        except Exception as exc:
            errlogger.error(f'No timestamp for {collection_name}')
            collection_metadata[collection_name]['updated'] = ""

    progresslogger.info('Added updates')
    return collection_metadata

def add_sources(client, collection_metadata):
    query = f"""
    SELECT DISTINCT collection_id collection_name, source_doi
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_public_and_current` 
"""
    for row in client.query(query).result():
        collection_metadata[row['collection_name']]['sources'].append(
            {
             "source_id": "",
             "source_type": "",
             "source_doi": row["source_doi"],
             "source_url": f'https://doi.org/{row["source_doi"]}',
             "modalities": "",
             "license": {
                 "license_url": "",
                 "license_long_name": "",
                 "license_short_name": ""
             },
             "citation": "",
             "access": "Public",
             "ImageTypes": "",
            }
        )
    return collection_metadata

def add_ids(client, collection_metadata):

    # Build a dictionary of source_id and type across all collections and analysis results
    tcia_collection_metadata = { data['collection_doi'].lower(): {
        "source_id": data['collection_short_title'].lower().replace('-', '_'),
        "source_type": "original data"
    } for data in get_tcia_collection_manager_data('collections')}
    tcia_analysis_results_metadata = {data['result_doi'].lower(): {
        "source_id": data['result_short_title'].lower().replace('-', '_'),
        "source_type": 'analysis result'
    } for data in  get_tcia_collection_manager_data('analysis-results')}
    idc_collection_metadata = {data['source_doi'].lower(): {
        "source_id": data['collection_id'],
        "source_type": "original data"
    } for index, data in read_json_to_dataframe(f'{settings.PROJECT_PATH}/bq/generate_tables_and_views/table_generation_jsons/idc_original_collections_metadata.json5').iterrows()}
    idc_analysis_results_metadata = {data['source_doi'].lower(): {
        "source_id": data['analysis_result_name'].lower().replace('-', '_').replace(' ', '_',),
        "source_type": "analysis result"
    } for index, data in read_json_to_dataframe(f'{settings.PROJECT_PATH}/bq/generate_tables_and_views/table_generation_jsons/idc_analysis_results_metadata.json5').iterrows()}

    source_data = tcia_collection_metadata | tcia_analysis_results_metadata | idc_collection_metadata | idc_analysis_results_metadata
    for collection, metadata in collection_metadata.items():
        for source in metadata['sources']:
            try:
                source['source_id'] = source_data[source['source_doi']]['source_id']
                source['source_type'] = source_data[source['source_doi']]['source_type']
            except:
                errlogger.error(f'No source_id for {collection}:{source["source_doi"]}')
    return collection_metadata

def build_metadata(client, args):
    # Now get most of the metadata for all collections
    collection_metadata = get_collection_metadata(client, args)

    # Add additional metadata that we get separately
    collection_metadata = add_sources(client, collection_metadata)
    collection_metadata = add_ids(client, collection_metadata)
    collection_metadata = add_licenses(client, args, collection_metadata)
    collection_metadata = add_descriptions(client, args, collection_metadata)
    collection_metadata = add_updates(client, collection_metadata)
    collection_metadata = add_case_counts(client, args, collection_metadata)
    collection_metadata = add_programs(client, args, collection_metadata)
    collection_metadata = add_citations(collection_metadata)
    collection_metadata = add_modalities(client, collection_metadata)

    # Convert dictionary to list of dicts
    metadata = [value for value in collection_metadata.values()]
    return metadata


def gen_collections_table(args):
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
                    settings.BQ_DEV_EXT_DATASET if args.access=='Public' else settings.BQ_DEV_INT_DATASET , args.bqtable_name, metadata_json,
                                data_collections_metadata_schema, write_disposition='WRITE_TRUNCATE')
        return
    except Exception as exc:
        errlogger.error(f'Table creation failed: {exc}')
    exit


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bqtable_name', default='original_collections_metadata_of', help='BQ table name')
    parser.add_argument('--access', default='Public', help="Generate original_collections_metadata if True; (deprecated)generate a table of excluded collections if false (deprecated)")
    parser.add_argument('--use_cached_metadata', default=False)
    parser.add_argument('--cached_metadata_file', default='cached_included_metadata.json', help='Where to cache metadata')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    gen_collections_table(args)