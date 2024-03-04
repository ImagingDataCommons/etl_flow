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
from google.cloud import bigquery
from utilities.bq_helpers import load_BQ_from_json, delete_BQ_Table
from bq.generate_tables_and_views.original_collections_metadata.schema import data_collections_metadata_schema
from utilities.tcia_helpers import get_all_tcia_metadata
from utilities.logging_config import errlogger
from python_settings import settings

def add_programs(client, args, collection_metadata):
    query = f"""
        SELECT *
        FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.program`"""
    programs = {row['collection_id'].lower(): row['program'] for row in client.query(query).result()}
    for collection in collection_metadata:
            if collection_metadata[collection]['collection_id']:
                collection_metadata[collection]["Program"] = \
                    programs[collection_metadata[collection]['collection_id']]
    return collection_metadata
    # programs = {collection: program for cur.fetchall()

# Generate a dict of collections in the current version,
# indexed by "idc_webapp_collection_ids" and mapping t
# "tcia_api_collection_id". Includes sources of each collection.
def get_collections_in_version(client, args):
    # Return collections that have specified access
    query = f"""
    SELECT DISTINCT collection_id collection_name, c_sources.tcia tcia_source, c_sources.idc idc_source
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_current`
    """

    collections = {row.collection_name.lower().replace(' ','_').replace('-','_'): dict(row.items())\
        for row in client.query(query)}
    return collections

# Count the cases (patients) in each collection
def add_case_counts(client, args, collection_metadata):
    query = f"""
    SELECT
      collection_id,
      COUNT(DISTINCT submitter_case_id ) as cases,
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_current`
    GROUP BY
        collection_id
    """

    case_counts = {c['collection_id'].lower().replace(' ','_').replace('-','_'): c['cases'] for c in client.query(query).result()}
    for collection in collection_metadata:
        collection_metadata[collection]['Subjects'] = case_counts[collection]
    return collection_metadata

# Generate a per-collection list of the modalities across all instances in each collection
def add_image_modalities(client, args, collection_metadata):
    query = f"""
      SELECT DISTINCT
        REPLACE(REPLACE(LOWER(collection_id),'-','_'),' ','_') AS idc_webapp_collection_id,
        STRING_AGG(DISTINCT modality, ", " ORDER BY modality) ImageTypes
      FROM
        `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.all_joined_current`
      JOIN
        `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_pub.dicom_metadata`
     ON
        sop_instance_uid = SOPInstanceUID
      GROUP BY
        idc_webapp_collection_id
      ORDER BY
        idc_webapp_collection_id  """

    imageTypes = {c['idc_webapp_collection_id'].lower().replace(' ','_').replace('-','_'): c['ImageTypes'] for c in client.query(query).result()}
    for collection in collection_metadata:
        collection_metadata[collection]['ImageTypes'] = imageTypes[collection]
    return collection_metadata

# Get metadata like that on the TCIA data collections page for collections that TCIA doesn't have
def get_original_collections_metadata_idc_source(client, args):
    query = f"""
    SELECT * 
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.original_collections_metadata_idc_source`
    ORDER BY collection_id
    """

    metadata = {}
    for row in client.query(query).result():
        metadata[row['collection_id']] = dict(
            collection_name = row['collection_name'],
            collection_id = row['collection_name'].lower().replace('-','_').replace(' ','_'),
            source_doi = row['DOI'],
            source_url = row['URL'],
            CancerTypes = row['CancerType'],
            TumorLocations = row['Location'],
            Subjects = 0,
            Species = row['Species'],
            ImageTypes = row['ImageTypes'],
            SupportingData = row['SupportingData'],
            Access = ["Public"],
            Status = row['Status'],
            Updated = row['Updated'] if row['Updated'] != 'NA' else None,
            DOI = row['DOI'],
            URL = row['URL'],
            CancerType = row['CancerType'],
            Location = row['Location'],
            # idc_webapp_collection_id = row['collection_name'].lower().replace('-','_').replace(' ','_'),
            # tcia_api_collection_id = row['collection_name'],
            # tcia_wiki_collection_id = ''
        )
    return metadata

def get_original_collections_metadata_tcia_source(client, args, idc_collections):
    tcia_collection_metadata = get_all_tcia_metadata('collections')
    metadata = {}
    for collection_id, values  in idc_collections.items():
        collection_name = values['collection_name']
        try:
            collection_metadata = next(collection for collection in tcia_collection_metadata \
                        if collection_name == collection['collection_short_title'])
        except Exception as exc:
            errlogger.error(f'No collection manager data for {collection_name}')
            id_map = {
                'ACRIN-NSCLC-FDG-PET': 'ACRIN 6668',
                'CT COLONOGRAPHY': 'ACRIN 6664',
                'Prostate-Anatomical-Edge-Cases': 'Prostate Anatomical Edge Cases',
                'QIN-BREAST': 'QIN-Breast'
            }
            collection_metadata = next(collection for collection in tcia_collection_metadata \
                if id_map[collection_name] == collection['collection_short_title'])

        try:
            metadata[collection_id] = dict(
                collection_name=collection_name,
                collection_id=collection_id,
                Status=collection_metadata['collection_status'],
                Updated=collection_metadata['date_updated'].split('T')[0],
                Access=["Public"],
                ImageTypes="",
                Subjects=0,
                source_doi=collection_metadata['collection_doi'],
                source_url=f"https://doi.org/{collection_metadata['collection_doi']}",
                CancerTypes=", ".join(collection_metadata['cancer_types']) \
                    if isinstance(collection_metadata['cancer_types'], list) else '',
                SupportingData=", ".join(collection_metadata['supporting_data']) \
                    if isinstance(collection_metadata['supporting_data'], list) else '',
                Species=", ".join(collection_metadata['species']) \
                    if isinstance(collection_metadata['species'], list) else '',
                TumorLocations=", ".join(collection_metadata['cancer_locations']) \
                    if isinstance(collection_metadata['cancer_locations'], list) else '',
                DOI=collection_metadata['collection_doi'],
                URL=f"https://doi.org/{collection_metadata['collection_doi']}",
                CancerType=", ".join(collection_metadata['cancer_types']) \
                    if isinstance(collection_metadata['cancer_types'], list) else '',
                Location=", ".join(collection_metadata['cancer_locations']) \
                    if isinstance(collection_metadata['cancer_locations'], list) else '',
                # idc_webapp_collection_id=collection_id,
                # tcia_api_collection_id=collection_name,
                # tcia_wiki_collection_id=collection_metadata['collection_browse_title']
            )
        except Exception as exc:
            print(exc)

    return metadata


# Get collection metadata by scraping the TCIA Original Collections page
# and/or from the original_collections_metadata_idc_source table
def get_collection_metadata(client, args):
    # Get a dict indexed by idc_webapp_collection_ids and mapping
    # to "tcia_api_collection_id having access==args.access
    idc_collections = get_collections_in_version(client, args)
    idc_collection_metadata = {collection.lower().replace(' ','_').replace('-','_'): value for collection, value in get_original_collections_metadata_idc_source(client, args).items()}
    tcia_only_collections = {key: val for key, val in idc_collections.items() if key not in idc_collection_metadata.keys()}
    tcia_collection_metadata = get_original_collections_metadata_tcia_source(client, args, tcia_only_collections)

    # Merge the TCia collection metadata.
    collection_metadata = tcia_collection_metadata | idc_collection_metadata
    return collection_metadata

def add_descriptions(client, args, collection_metadata):
    query = f"""
    SELECT * 
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.original_collections_descriptions`
    ORDER BY collection_id
    """

    descriptions = {}
    for row in client.query(query).result():
        descriptions[row['collection_id']] = dict(
            description = row['description']
        )
    for collection in collection_metadata:
        collection_metadata[collection]['Description'] = descriptions[collection]['description']
        # collection_metadata[collection]['Description'] = ""
    return collection_metadata


# Get a list of the licenses associated with each collection
def add_licenses(client, doi, collection_metadata):
    # query = f"""
    # WITH unstruct as(
    # SELECT DISTINCT REPLACE(REPLACE(LOWER(collection_id),'-','_'),' ','_') collection_id, license_url, license_long_name, license_short_name
    # FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_public`
    # WHERE license_url is not null
    # )
    # SELECT collection_id, ARRAY_AGG(STRUCT(license_url, license_long_name, license_short_name)) as licenses
    # FROM unstruct
    # GROUP BY collection_id
    # ORDER BY collection_id
    #  """
    query = f"""
    WITH unstruct as(
    SELECT DISTINCT source_url, license.license_url, license.license_long_name, license.license_short_name
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.licenses`
    WHERE license.license_url is not null
    )
    SELECT source_url, ARRAY_AGG(STRUCT(license_url, license_long_name, license_short_name)) as licenses
    FROM unstruct
    GROUP BY source_url
    ORDER BY source_url
     """

    license_dicts = [dict(row) for row in client.query(query)]

    licenses = {dict(row)['source_url']: dict(row)['licenses'] for row in client.query(query)}

    # Keep only distinct licenses
    for source_url, license_list in licenses.items():
        licenses[source_url] = \
            list({v['license_short_name']: v for v in license_list}.values())

    for collection, metadata in collection_metadata.items():
        collection_metadata[collection]["licenses"] = next(license for  source_url, license in licenses.items() if source_url.lower() == metadata['source_url'].lower())
    return collection_metadata


def build_metadata(client, args):
    # Now get most of the metadata for all collections
    collection_metadata = get_collection_metadata(client, args)

    # Add additional metadata that we get separately
    collection_metadata = add_licenses(client, args, collection_metadata)
    collection_metadata = add_programs(client, args, collection_metadata)
    collection_metadata = add_descriptions(client, args, collection_metadata)
    collection_metadata = add_case_counts(client, args, collection_metadata)
    collection_metadata = add_image_modalities(client, args, collection_metadata)

    # Convert dictionary to list of dicts
    metadata = [value for value in collection_metadata.values()]
    return metadata


def gen_collections_table(args):
    BQ_client = bigquery.Client(project=settings.DEV_PROJECT)

    metadata = build_metadata(BQ_client, args)

    metadata_json = '\n'.join([json.dumps(row) for row in
                        sorted(metadata, key=lambda d: d['collection_name'])])
    try:
        delete_BQ_Table(BQ_client, settings.DEV_PROJECT, settings.BQ_DEV_EXT_DATASET, args.bqtable_name)
        load_BQ_from_json(BQ_client,
                    settings.DEV_PROJECT,
                    settings.BQ_DEV_EXT_DATASET if args.access=='Public' else settings.BQ_DEV_INT_DATASET , args.bqtable_name, metadata_json,
                                data_collections_metadata_schema, write_disposition='WRITE_TRUNCATE')
        pass
    except Exception as exc:
        errlogger.error(f'Table creation failed: {exc}')
        exit


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bqtable_name', default='original_collections_metadata', help='BQ table name')
    parser.add_argument('--access', default='Public', help="Generate original_collections_metadata if True; (deprecated)generate a table of excluded collections if false (deprecated)")
    parser.add_argument('--use_cached_metadata', default=False)
    parser.add_argument('--cached_metadata_file', default='cached_included_metadata.json', help='Where to cache metadata')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    gen_collections_table(args)