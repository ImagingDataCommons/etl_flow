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

# Generate a table of license info for each collection
import argparse
import sys
from google.cloud import bigquery
from utilities.tcia_helpers import get_collection_license_info
from ingestion.utilities.utils import to_webapp

from python_settings import settings


# def get_collections_in_version(client, args):
#     query = f"""
#     SELECT DISTINCT c.collection_id
#     FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.version` AS v
#     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.version_collection` as vc
#     ON v.version = vc.version
#     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.collection` AS c
#     ON vc.collection_uuid = c.uuid
#     WHERE v.version = {settings.CURRENT_VERSION}
#     ORDER BY c.collection_id
#     """
#
#     result = client.query(query).result()
#     collection_ids = sorted([to_webapp(collection['collection_id']) for collection in result])
#     return collection_ids

def get_collections_in_version(client, args):
    if args.gen_excluded:
        # Only excluded collections
        query = f"""
        SELECT DISTINCT c.collection_id
        FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.version` AS v
        JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.version_collection` as vc
        ON v.version = vc.version
        JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.collection` AS c
        ON vc.collection_uuid = c.uuid
        JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.excluded_collections` as ex
        ON LOWER(c.collection_id) = LOWER(ex.tcia_api_collection_id)
        WHERE v.version = {settings.CURRENT_VERSION}
        ORDER BY c.collection_id
        """
    else:
        # Only included collections
        query = f"""
        SELECT DISTINCT c.collection_id
        FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.version` AS v
        JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.version_collection` as vc
        ON v.version = vc.version
        JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.collection` AS c
        ON vc.collection_uuid = c.uuid
        LEFT JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.excluded_collections` as ex
        ON LOWER(c.collection_id) = LOWER(ex.tcia_api_collection_id)
        WHERE ex.tcia_api_collection_id IS NULL
        AND v.version = {settings.CURRENT_VERSION}
        ORDER BY c.collection_id
        """
    result = client.query(query).result()
    collection_ids = sorted([to_webapp(collection['collection_id']) for collection in result])
    return collection_ids

# Get license info for collections that are not available from TCIA
# We currently assume this is pathology data
def get_non_tcia_license_info(client, args):
    query = f"""
    SELECT * 
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.non_tcia_collection_metadata`
    ORDER BY idc_webapp_collection_id
    """
    licenses = {}
    for row in client.query(query).result():
        licenses[row['idc_webapp_collection_id'].lower().replace(' ','_').replace('-','_')] = dict(
            licenseURL = row['license_url'],
            longName = row['license_long_name'],
            shortName = row['license_short_name']
        )
    return licenses


# def get_all_license_info(client, args):
#     licenses = {collection.lower().replace(' ','_').replace('-','_'): value for collection, value in get_collection_license_info().items()}
#     licenses |= get_non_tcia_license_info(client, args)
#     return licenses

# Get license data for all TCIA collections. Includes redacted collections.
# We assume the same license applies to both radiology and pathology except
# for redacted collections.
def get_tcia_license_info(client, args):
    licenses = {collection.lower().replace(' ','_').replace('-','_'): value for collection, value in get_collection_license_info().items()}
    return licenses

def get_original_collection_licenses(args):
    client = bigquery.Client()
    licenses = {}
    collection_ids = get_collections_in_version(client, args)
    tcia_licenses = get_tcia_license_info(client, args)
    non_tcia_licenses = get_non_tcia_license_info(client, args)

    # Add licenses of TCIA collections to table
    for collection, license in tcia_licenses.items():
        if collection in collection_ids:
            licenses[collection] = {
                "tcia":
                    {
                        'license_url': license['licenseURL'],
                        'license_long_name': license['longName'],
                        'license_short_name': license['shortName'].strip()
                    }
            }
            if collection.startswith('cptac') or collection.startswith('tcga'):
                # All cptac and tcga pathology is CC by 3.0
                licenses[collection]['path'] = {
                    'license_url': 'https://creativecommons.org/licenses/by/3.0/',
                    'license_long_name': 'Creative Commons Attribution 3.0 Unported License',
                    'license_short_name': 'CC BY 3.0'
                }
            elif collection == 'nlst':
                # nlst pathology is CC by 4.0
                licenses[collection]['path'] = {
                    'license_url': 'https://creativecommons.org/licenses/by/4.0/',
                    'license_long_name': 'Creative Commons Attribution 4.0 International License',
                    'license_short_name': 'CC BY 4.0'
                }

    if not args.gen_excluded:
        # Add licenses of non-TCIA collections (certain TCGA collections)
        for collection, license in non_tcia_licenses.items():
            licenses[collection] = {
                "path":
                    {
                        'license_url': license['licenseURL'],
                        'license_long_name': license['longName'],
                        'license_short_name': license['shortName'].strip()
                    }
                }
    return licenses



if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    # parser.add_argument('--version', default=8, help='IDC version for which to build the table')
    # args = parser.parse_args()
    # parser.add_argument('--target', default='dev', help="dev or prod")
    # parser.add_argument('--merged', default=True, help='True if premerge buckets have been merged in dev buckets')
    # parser.add_argument('--src_project', default='idc-dev-etl')
    # parser.add_argument('--dst_project', default='idc-dev-etl')
    # parser.add_argument('--dev_bqdataset_name', default=f'idc_v{args.version}_dev', help='BQ dataset containing development tables')
    # parser.add_argument('--pub_bqdataset_name', default=f'idc_v{args.version}_pub', help='BQ dataset containing public tables')
    # parser.add_argument('--trg_bqdataset_name', default=f'idc_v{args.version}_pub', help='BQ dataset of resulting table')
    # parser.add_argument('--bqtable_name', default='auxiliary_metadata', help='BQ table name')
    args = parser.parse_args()

    print("{}".format(args), file=sys.stdout)

    get_original_collection_licenses(args)