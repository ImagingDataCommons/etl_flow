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
import time
from google.cloud import bigquery
from utilities.bq_helpers import load_BQ_from_json
from bq.original_data_collections.schema import data_collections_metadata_schema
from bq.utils.gen_license_table import get_original_collection_licenses
from utilities.tcia_helpers import get_collection_descriptions_and_licenses, get_collection_license_info
from utilities.tcia_scrapers import scrape_tcia_data_collections_page
from utilities.logging_config import successlogger, errlogger, progresslogger
from python_settings import settings

def get_collections_programs(client, args):
    query = f"""
        SELECT *
        FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.program`"""
    programs = {row['tcia_wiki_collection_id'].lower().replace(' ','_').replace('-','_'): row['program'] for row in client.query(query).result()}

    return programs
    # programs = {collection: program for cur.fetchall()



# Generate a dict of included or excluded collections,
# indexed by "idc_webapp_collection_ids" and mapping to
# "tcia_api_collection_id".
def get_collections_in_version(client, args):
    # Return collections that have specified access
    query = f"""
    SELECT tcia_api_collection_id
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_collections`
    WHERE tcia_access='{args.access}' OR idc_access='{args.access}'
    """

    collection_ids = {row.tcia_api_collection_id.lower().replace(' ','_').replace('-','_'): \
        row.tcia_api_collection_id for row in client.query(query)}
    return collection_ids

# Count the cases (patients) in each collection
def get_cases_per_collection(client, args):
    query = f"""
    SELECT
      c.collection_id as collection_id,
      COUNT(DISTINCT p.submitter_case_id ) as cases,
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.version` AS v
    JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.version_collection` as vc
    ON v.version = vc.version
    JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.collection` AS c
    ON vc.collection_uuid = c.uuid
    JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.collection_patient` AS cp
    ON c.uuid = cp.collection_uuid
    JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.patient` AS p
    ON cp.patient_uuid = p.uuid
    WHERE v.version = {settings.CURRENT_VERSION}
    GROUP BY
        c.collection_id
    """

    case_counts = {c['collection_id'].lower().replace(' ','_').replace('-','_'): c['cases'] for c in client.query(query).result()}
    return case_counts

# Generate a per-collection list of the modalities across all instances in each collection
def get_image_modalities(client, args):
    query = f"""
    WITH
      siis AS (
      SELECT
        REPLACE(REPLACE(LOWER(c.collection_id),'-','_'),' ','_') AS idc_webapp_collection_id,
        i.sop_instance_uid,
      FROM
        `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.version` v
      JOIN
        `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.version_collection` vc
      ON
        v.version = vc.version
      JOIN
        `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.collection` c
      ON
        vc.collection_uuid = c.uuid
      JOIN
        `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.collection_patient` cp
      ON
        c.uuid = cp.collection_uuid
      JOIN
        `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.patient` p
      ON
        cp.patient_uuid = p.uuid
      JOIN
        `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.patient_study` ps
      ON
        p.uuid = ps.patient_uuid
      JOIN
        `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.study` st
      ON
        ps.study_uuid = st.uuid
      JOIN
        `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.study_series` ss
      ON
        st.uuid = ss.study_uuid
      JOIN
        `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.series` se
      ON
        ss.series_uuid = se.uuid
      JOIN
        `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.series_instance` si
      ON
        se.uuid = si.series_uuid
      JOIN
        `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.instance` i
      ON
        si.instance_uuid = i.uuid
      WHERE
        v.version = {settings.CURRENT_VERSION}),
      pre_mods AS (
      SELECT
        siis.idc_webapp_collection_id,
        da.Modality
      FROM
        siis
      JOIN
        `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_pub.dicom_metadata` da
      ON
        siis.sop_instance_uid = da.SOPInstanceUID),
      mods AS (
      SELECT
        DISTINCT idc_webapp_collection_id,
        modality
      FROM
        pre_mods
      ORDER BY
        Modality)
    SELECT
      idc_webapp_collection_id,
      ARRAY_TO_STRING(ARRAY_AGG(modality),", ") ImageTypes
    FROM
      mods
    GROUP BY
      idc_webapp_collection_id
    ORDER BY
      idc_webapp_collection_id
  """

    imageTypes = {c['idc_webapp_collection_id'].lower().replace(' ','_').replace('-','_'): c['ImageTypes'] for c in client.query(query).result()}
    return imageTypes


def get_access_status(client, args):
    query = f"""
        SELECT tcia_api_collection_id as collection_id, tcia_access, idc_access
        FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_collections`
        """
    access_status = {c['collection_id'].lower().replace(' ','_').replace('-','_'): \
        {'tcia_access':c['tcia_access'], 'idc_access': c['idc_access']}\
        for c in client.query(query).result()}
    return access_status

# Get metadata like that on the TCIA data collections page for collections that TCIA doesn't have
def get_original_collections_metadata_idc_source(client, args):
    query = f"""
    SELECT * 
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.original_collections_metadata_idc_source`
    ORDER BY idc_webapp_collection_id
    """

    metadata = {}
    for row in client.query(query).result():
        metadata[row['idc_webapp_collection_id']] = dict(
            tcia_wiki_collection_id = "",
            DOI = row['DOI'],
            URL = row['URL'],
            CancerType = row['CancerType'],
            Location = row['Location'],
            Subjects = 0,
            Species = row['Species'],
            ImageTypes = row['ImageTypes'],
            SupportingData = row['SupportingData'],
            Access = "",
            Status = row['Status'],
            Updated = row['Updated'] if row['Updated'] != 'NA' else None
        )
    return metadata

# Get collection metadata by scraping the TCIA Original Collections page
# and/or from the original_collections_metadata_idc_source table
def get_collection_metadata(client, args):
    # Start with metadata for collections that TCIA doesn't have
    collection_metadata = {collection.lower().replace(' ','_').replace('-','_'): value for collection, value in get_original_collections_metadata_idc_source(client, args).items()}

    # Get the metadata for TCIA hosted collections
    # Collecting metadata takes a long time, so we sometimes cache it when
    # doing development on other parts of this code. Normally it should not
    # be cached.
    if args.use_cached_metadata:
        with open(args.cached_metadata_file) as f:
            tcia_collection_metadata = json.load(f)
    else:
        tcia_collection_metadata = {collection.lower().replace(' ', '_').replace('-', '_'): value for collection, value in
                                scrape_tcia_data_collections_page().items()}
    with open(args.cached_metadata_file, 'w') as f:
        json.dump(tcia_collection_metadata,f)

    # Merge the TCia collection metadata.
    collection_metadata |= tcia_collection_metadata

    # Replace ImageTypes values with actual image types in IDC data
    imageTypes = get_image_modalities(client, args)

    for id, data in collection_metadata.items():
        if id in imageTypes:
            data['ImageTypes'] = imageTypes[id]
    return collection_metadata


def get_non_tcia_descriptions(client, args):
    query = f"""
    SELECT * 
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.original_collections_metadata_idc_source`
    ORDER BY idc_webapp_collection_id
    """

    descriptions = {}
    for row in client.query(query).result():
        # row['idc_webapp_collection_id'] = dict(
        #     description = row['Description']
        # )
        descriptions[row['idc_webapp_collection_id']] = dict(
            description = row['Description']
        )
    return descriptions


def get_all_descriptions(client, args):
    collection_descriptions = {collection.lower().replace(' ','_').replace('-','_'): value for collection, value in get_collection_descriptions_and_licenses().items()}
    collection_descriptions |=get_non_tcia_descriptions(client, args)
    return collection_descriptions


# Get a list of the licenses associated with each collection
def get_licenses(client, doi):
    query = f"""
    WITH unstruct as(
    SELECT DISTINCT REPLACE(REPLACE(LOWER(collection_id),'-','_'),' ','_') collection_id, license_url, license_long_name, license_short_name
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined`
    WHERE license_url is not null
    )
    SELECT collection_id, ARRAY_AGG(STRUCT(license_url, license_long_name, license_short_name)) as licenses
    FROM unstruct
    GROUP BY collection_id
    ORDER BY collection_id
     """
    license_dicts = [dict(row) for row in client.query(query)]
    licenses = {row['collection_id']: row['licenses'] for row in license_dicts}
    return licenses


def build_metadata(client, args):
    BQ_client = bigquery.Client(project=settings.DEV_PROJECT)

    # Get a list of the licenses associated with each collection
    licenses = get_licenses(BQ_client, args)
    # licenses = get_original_collection_licenses(args)

    # Now get most of the medadata for all collections
    collection_metadata = get_collection_metadata(client, args)

    programs = get_collections_programs(BQ_client, args)

    # Get a dict indexed by idc_webapp_collection_ids and mapping
    # to "tcia_api_collection_id having access==args.access
    idc_collection_ids = get_collections_in_version(BQ_client, args)

    # Get collection descriptions for all collections
    collection_descriptions = get_all_descriptions(client, args)

    # We report our case count rather than counts from the TCIA wiki pages.
    case_counts = get_cases_per_collection(client, args)

    # # Get the access status of each collection
    # access_status = get_access_status(client,args)

    rows = []
    json_rows = []
    found_ids = []

    for idc_collection_id, tcia_api_collection_id in idc_collection_ids.items():
        # if idc_collection_id.lower().replace(' ','_').replace('-','_') in common_collection_metadata_ids:
        if idc_collection_id in collection_metadata:
                try:
                    # tcia_collection_id = common_collection_metadata_ids[
                    #     idc_collection_id.lower().replace(' ', '_').replace('-', '_')]
                    collection_data = collection_metadata[idc_collection_id]
                    if not 'URL' in collection_data:
                        # TCIA collections have an empty URL
                        collection_data['URL'] = f"https://doi.org/{collection_data['DOI']}"
                    if collection_data['tcia_wiki_collection_id']:
                        # Only tcia collections have a tcia_api_collection_id
                        collection_data['tcia_api_collection_id'] = tcia_api_collection_id
                    else:
                        collection_data['tcia_api_collection_id'] = ""
                    collection_data['idc_webapp_collection_id'] = idc_collection_id
                    if collection_data['tcia_wiki_collection_id']:
                        collection_data['Program'] = programs[collection_data['tcia_wiki_collection_id'].lower().replace(' ','_').replace('-','_')]
                    else:
                        collection_data['Program'] = programs[idc_collection_id]
                    try:
                        collection_data['Description'] = collection_descriptions[idc_collection_id][
                            'description']
                    except:
                        collection_data['Description'] = ""
                    collection_data['Subjects'] = case_counts[idc_collection_id]
                    try:
                        # collection_data['licenses'] = [dict(row)['license'] for row in licenses if \
                        #     row['collection_id'].lower().replace('-', '_').replace(' ', '_')  == idc_collection_id]
                        collection_data['licenses'] = licenses[idc_collection_id]

                        # collection_data['licenses'] = []
                        # if 'tcia' in licenses[idc_collection_id]:
                        #     collection_data['licenses'].append(licenses[idc_collection_id]['tcia'])
                        #     if 'idc' in licenses[idc_collection_id] and \
                        #             licenses[idc_collection_id]['tcia'] != licenses[idc_collection_id]['idc']:
                        #                 collection_data['licenses'].append(licenses[idc_collection_id]['idc'])
                        # else:
                        #     collection_data['licenses'].append(licenses[idc_collection_id]['idc'])
                    except:
                        collection_data['licenses'] = {}
                    collection_data['Access'] = [args.access]
                    rows.append(collection_data)
                    json_rows.append(json.dumps(collection_data))
                    found_ids.append(idc_collection_id)
                except Exception as exc:
                    errlogger.error(f'Exception building metadata {exc}')

        else:
            errlogger.error(f'{idc_collection_id} not in collection metadata')

    # Make sure we found metadata for all our collections
    for idc_collection in idc_collection_ids:
        if not idc_collection in found_ids:
            errlogger.error(f'****No metadata for {idc_collection}')
            if idc_collection == 'apollo':
                collection_data = {
                    "tcia_wiki_collection_id": "APOLLO-1-VA",
                    "DOI": "",
                    "CancerType": "Non-small Cell Lung Cancer",
                    "Location": "Lung",
                    "Species": "Human",
                    "Subjects": 7,
                    "ImageTypes": "CT, PT",
                    "SupportingData": "",
                    "Access": "Public",
                    "Status": "Complete",
                    "Updated": "2018-03-08",
                    "tcia_api_collection_id": "APOLLO",
                    "idc_webapp_collection_id": "apollo",
                    "Program": "APOLLO",
                    "Description": \
"""<p>	This data collection consists of images and associated data acquired from the <a class=""external-link"" href=""https://proteomics.cancer.gov/programs/apollo-network""> APOLLO Network</a> and may be subject to usage restrictions.</p>
<p>	The <strong><em>A</em></strong>pplied <strong><em>P</em></strong>roteogenomics <strong><em>O</em></strong>rganizationa<strong><em>L</em></strong> <strong><em>L</em></strong>earning and <strong><em>O</em></strong>utcomes (APOLLO) network is a collaboration between NCI, the Department of Defense (DoD), and the Department of Veterans Affairs (VA) to incorporate proteogenomics into patient care as a way of looking beyond the genome, to the activity and expression of the proteins that the genome encodes. The emerging field of proteogenomics aims to better predict how patients will respond to therapy by screening their tumors for both genetic abnormalities and protein information, an approach that has been made possible in recent years due to advances in proteomic technology.</p>
<p>	Please see the <a href=""https://wiki.cancerimagingarchive.net/display/Public/APOLLO-1-VA"">APOLLO</a> wiki page to learn more.</p>""",
                    "license_url": "https://proteomics.cancer.gov/data-portal",
                    "license_long_name": "APOLLO Network Data Use Agreement",
                    "license_short_name": "APOLLO"}
                # collection_data["Description"] = collection_descriptions['APOLLO']['description']
                rows.append(json.dumps(collection_data))
            elif args.access != 'Public' and idc_collection.lower().startswith('apollo'):
                # APOLLO-* collections are not on the TCIA data collections page so can't scrape metadata.
                continue
            else:
                errlogger.error(f'No metadata for {idc_collection}')

    metadata = '\n'.join(json_rows)
    return metadata

def gen_collections_table(args):
    BQ_client = bigquery.Client(project=settings.DEV_PROJECT)

    metadata = build_metadata(BQ_client, args)
    try:
        job = load_BQ_from_json(BQ_client,
                    settings.DEV_PROJECT,
                    settings.BQ_DEV_EXT_DATASET if args.access=='Public' else settings.BQ_DEV_INT_DATASET , args.bqtable_name, metadata,
                                data_collections_metadata_schema, write_disposition='WRITE_TRUNCATE')
    except Exception as exc:
        errlogger.error(f'Table creation failed: {exc}')
        exit

    while not job.state == 'DONE':
        progresslogger.info('Status: {}'.format(job.state))
        time.sleep(args.period * 60)
    successlogger.info(f"{time.asctime()}: Completed collections metatdata upload")

# if __name__ == '__main__':
#     parser = argparse.ArgumentParser()
#     parser.add_argument('--version', default=8, help='IDC version for which to build the table')
#     args = parser.parse_args()
#     parser.add_argument('--src_project', default='idc-dev-etl')
#     parser.add_argument('--dst_project', default='idc-dev-etl')
#     parser.add_argument('--dev_bqdataset_name', default=f'idc_v{settings.CURRENT_VERSION}_dev', help='BQ dataset of dev tables')
#     parser.add_argument('--pub_bqdataset_name', default=f'idc_v{settings.CURRENT_VERSION}_pub', help='BQ dataset of public tables')
#     parser.add_argument('--bqtable_name', default='original_collections_metadata', help='BQ table name')
#     parser.add_argument('--gen_excluded', default=False, help="Generate excluded_original_collections_metadata if True")
#     parser.add_argument('--use_cached_metadata', default=True)
#     parser.add_argument('--cached_metadata_file', default='cached_metadata.json', help='Where to cache metadata')
#
#     args = parser.parse_args()
#     print("{}".format(args), file=sys.stdout)
#     gen_collections_table(args)