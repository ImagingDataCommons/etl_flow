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
import requests

def add_programs(client, args, collection_metadata):
    query = f"""
        SELECT *
        FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.program`"""
    programs = {row['collection_id'].lower(): row['program'] for row in client.query(query).result()}
    for collection_name, metadata in collection_metadata.items():
        metadata["Program"] = \
            programs[metadata['collection_id']]
    return collection_metadata
    # programs = {collection: program for cur.fetchall()

# Generate a dict of collections in the current version,
# indexed by "idc_webapp_collection_ids" and mapping t
# "tcia_api_collection_id". Includes sources of each collection.
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
        collection_metadata[collection]['Subjects'] = case_counts[collection]
    return collection_metadata

# Generate a per-collection list of the modalities across all instances in each collection
def add_image_modalities(client, args, collection_metadata):
    query = f"""
      SELECT DISTINCT
        REPLACE(REPLACE(LOWER(collection_id),'-','_'),' ','_') AS idc_webapp_collection_id,
        STRING_AGG(DISTINCT modality, ", " ORDER BY modality) ImageTypes
      FROM
        `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.all_joined_public_and_current`
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

    idc_only_metadata = {}
    idc_tcia_metadata = {}
    for row in client.query(query).result():
        if row['idc_only'] == 'True':
            idc_only_metadata[row['collection_name']] = dict(
                collection_name = row['collection_name'],
                # collection_id = row['collection_name'].lower().replace('-','_').replace(' ','_'),
                collection_id = row['collection_id'],
                CancerTypes = row['CancerType'],
                TumorLocations = row['Location'],
                Subjects = 0,
                Species = row['Species'],
                Sources = [
                    dict(
                    Access = "Public",
                    source_doi = row['source_doi'].lower() if row['source_doi'] else "",
                    source_url = row['source_url'].lower() if row['source_url'] else "",
                    ImageTypes = "",
                    License = "",
                    Citation = ""
                    )
                ],
                SupportingData = row['SupportingData'],
                Status = row['Status'],
                Updated = row['Updated'] if row['Updated'] != 'NA' else None,
                # Location = row['Location'],
                DOI = row['source_doi'],
                URL = row['source_url'],
                CancerType = row['CancerType'],
                Location =  row['Location']
                # idc_webapp_collection_id = row['collection_name'].lower().replace('-','_').replace(' ','_'),
                # tcia_api_collection_id = row['collection_name'],
                # tcia_wiki_collection_id = ''
            )
        else:
            idc_tcia_metadata[row['collection_name']] = dict(
                collection_name=row['collection_name'],
                # collection_id=row['collection_name'].lower().replace('-', '_').replace(' ', '_'),
                collection_id = row['collection_id'],
                CancerTypes=row['CancerType'],
                TumorLocations=row['Location'],
                Subjects=0,
                Species=row['Species'],
                Sources=[
                    dict(
                        Access="Public",
                        source_doi=row['source_doi'].lower() if row['source_doi'] else "",
                        source_url=row['source_url'].lower() if row['source_url'] else "",
                        ImageTypes="",
                        License="",
                        Citation=""
                    )
                ],
                SupportingData=row['SupportingData'],
                Status=row['Status'],
                Updated=row['Updated'] if row['Updated'] != 'NA' else None,
                # Location = row['Location'],
                DOI=row['source_doi'],
                URL=row['source_url'],
                CancerType=row['CancerType'],
                Location=row['Location']
            # idc_webapp_collection_id = row['collection_name'].lower().replace('-','_').replace(' ','_'),
            # tcia_api_collection_id = row['collection_name'],
            # tcia_wiki_collection_id = ''
            )

    return (idc_only_metadata, idc_tcia_metadata)


def get_url(url, headers=""):  # , headers):
    result =  requests.get(url, headers=headers)  # , headers=headers)
    if result.status_code != 200:
        raise RuntimeError('In get_url(): status_code=%s; url: %s', result.status_code, url)
    return result


# def get_citation(source_url):
#     header = {"Accept": "text/x-bibliography; style=apa"}
#     citation = get_url(source_url, header).text
#     return citation


def get_citation(source_url):
    if source_url:
        if 'zenodo' in source_url:
            params = {'access_token': settings.ZENODO_ACCESS_TOKEN}
        else:
            params = {}
        header = {"Accept": "text/x-bibliography; style=apa"}
        # citation = get_url(source_url, header).text
        citation = requests.get(source_url, headers=header, params=params).text
        if 'This DOI cannot be found in the DOI System' in citation:
            errlogger.error(f'Unable to get citation for {source_url}')
            citation = ""
    else:
        citation = source_url

    return citation


def get_original_collections_metadata_tcia_source(client, args, idc_collections):
    tcia_collection_metadata = get_all_tcia_metadata('collections')
    metadata = {}
    for collection_name, values  in idc_collections.items():
        # Find the collection manager entry corrsponding to a collection that IDC has
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
            # collection_metadata = next(collection for collection in tcia_collection_metadata \
            #     if id_map[collection_name] == collection['collection_short_title'])

        try:
            metadata[collection_name] = dict(
                collection_name=collection_name,
                collection_id=values['collection_id'],
                CancerTypes=", ".join(collection_metadata['cancer_types']) \
                    if isinstance(collection_metadata['cancer_types'], list) else '',
                TumorLocations=", ".join(collection_metadata['cancer_locations']) \
                    if isinstance(collection_metadata['cancer_locations'], list) else '',
                Subjects=0,
                Species=", ".join(collection_metadata['species']) \
                    if isinstance(collection_metadata['species'], list) else '',
                Sources = [
                    dict(
                    Access="Public",
                    source_doi=collection_metadata['collection_doi'].lower(),
                    source_url=f"https://doi.org/{collection_metadata['collection_doi'].lower()}",
                    ImageTypes="",
                    License = "",
                    Citation=""                    )
                ],
                SupportingData=", ".join(collection_metadata['supporting_data']) \
                    if isinstance(collection_metadata['supporting_data'], list) else '',
                Status=collection_metadata['collection_status'],
                Updated=collection_metadata['date_updated'].split('T')[0],
                DOI=collection_metadata['collection_doi'].lower(),
                URL=f"https://doi.org/{collection_metadata['collection_doi'].lower()}",
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


def merge_metadata(tcia_collection_metadata, idc_collection_metadata):
    collection_metadata = {}
    for collection_id, metadata in tcia_collection_metadata.items():
        collection_metadata[collection_id] = metadata
        # If IDC sources data from the same collection add its source data
        if collection_id in idc_collection_metadata:
            metadata['Sources'].extend(idc_collection_metadata[collection_id]['Sources'])
    for collection_id, metadata in idc_collection_metadata.items():
        if not collection_id in tcia_collection_metadata:
            collection_metadata[collection_id] = metadata

    return collection_metadata


# Get basic metadata for both TCIA and IDC sourced collections
def get_collection_metadata(client, args):
    # Get all the collections in this version. For each collection, get whether original collection data
    # is source from tcia and/ot idc
    idc_collections = get_collections_in_version(client, args)

    # Get metadata for each collection which we source from tcia (we also source data for some from idc)
    tcia_collections = {key: val for key, val in idc_collections.items() if val['tcia_source'] == True}
    tcia_collection_metadata = get_original_collections_metadata_tcia_source(client, args, tcia_collections)

    # Get metadata of two classes of collections: those for which we source data from both tcia and idc,
    # and those for which we source data only from idc.
    idc_only_collection_metadata, idc_and_tcia_collection_metadata = get_original_collections_metadata_idc_source(client, args)

    # Merge the TCia collection metadata.
    collection_metadata = tcia_collection_metadata
    for collection_id, metadata in idc_and_tcia_collection_metadata.items():
        collection_metadata[collection_id]['Sources'].extend(metadata['Sources'])
    collection_metadata |= idc_only_collection_metadata

    # We must now merge in source data for analysis results. For this purpose
    query = f"""
    SELECT DISTINCT collection_id, source_doi, source_url 
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_public_and_current`
    ORDER by collection_id
    """

    for row in client.query(query):
        if next((source for source in collection_metadata[row['collection_id']]['Sources'] if source['source_doi'] == row['source_doi']), 0) == 0:
            # This analysis result source is not already in this collections Sources
            collection_metadata[row['collection_id']]['Sources'].append(
                dict(
                    Access="Public",
                    source_doi=row['source_doi'].lower(),
                    source_url=row['source_url'].lower(),
                    ImageTypes="",
                    License="",
                    Citation="")
            )





    # collection_metadata = merge_metadata(tcia_collection_metadata, idc_collection_metadata)
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
    for collection, metadata in collection_metadata.items():
        try:
            metadata['Description'] = descriptions[collection.lower().replace('-','_').replace(' ','_')]['description']
        except Exception as exc:
            errlogger.error(f'No description for {collection}: {exc}')
        # collection_metadata[collection]['Description'] = ""
    return collection_metadata


# Get a list of the licenses associated with each collection
def add_licenses(client, doi, collection_metadata):
    # Get the licenses associated with a source_url. We assume that there is only one distinct licenses

    query = f"""
    WITH dist AS (
        SELECT DISTINCT source_url, license.license_url license_url, license.license_long_name license_long_name, license.license_short_name license_short_name
        FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.licenses`
        WHERE license.license_url is not null)
    SELECT source_url, STRUCT(license_url, license_long_name, license_short_name) license
    FROM dist
    """

    # license_dicts = [dict(row) for row in client.query(query)]
    #
    # licenses = {dict(row)['source_url']: dict(row)['licenses'] for row in client.query(query)}

    licenses = {row['source_url']: row['license'] for row in client.query(query)}

    # # Keep only distinct licenses
    # for source_url, license_list in licenses.items():
    #     licenses[source_url] = \
    #         list({v['license_short_name']: v for v in license_list}.values())

    for collection, metadata in collection_metadata.items():
        for source in metadata['Sources']:
            try:
                source["License"] = licenses[source['source_url']]
            except Exception as exc:
                errlogger.error(f'No license for {collection}, {source["source_url"]}: {exc}')
                source["License"] = {"license_url": "", "license_long_name": "", "license_short_name": ""}
    return collection_metadata

def add_citations(collection_metadata):
    for collection, data in collection_metadata.items():
        for source in data['Sources']:
            if source['source_doi']:
                try:
                    citation = get_citation(source['source_url'])
                except Exception as exc:
                    errlogger.error(f'Error getting citation for {collection},{source["source_url"]}:  {exc}')
                    citation = source['source_url']
            else:
                citation = source['source_url']
            source['Citation'] = citation

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
        # collection_name = row['collection_id'].lower().replace('-', '_').replace(' ','_')
        collection_name = row['collection_id']
        if collection_name in modalities:
            modalities[collection_name][row['source_url']] = row['modalities']
        else:
            modalities[collection_name] = {row['source_url']:  row['modalities']}

    for collection_name, metadata in collection_metadata.items():
        for source in metadata['Sources']:
            try:
                source['ImageTypes'] = modalities[collection_name][source['source_url'].lower()]
            except:
                errlogger.error(f'No modality for {collection_name}, {source["source_url"].lower()}')
    return collection_metadata


def build_metadata(client, args):
    # Now get most of the metadata for all collections
    collection_metadata = get_collection_metadata(client, args)

    # Add additional metadata that we get separately
    collection_metadata = add_citations(collection_metadata)
    collection_metadata = add_case_counts(client, args, collection_metadata)
    collection_metadata = add_programs(client, args, collection_metadata)
    collection_metadata = add_descriptions(client, args, collection_metadata)
    collection_metadata = add_licenses(client, args, collection_metadata)
    collection_metadata = add_modalities(client, collection_metadata)
    # collection_metadata = add_image_modalities(client, args, collection_metadata)

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