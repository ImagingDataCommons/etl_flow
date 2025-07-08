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


# Generate a list of included or excluded collections
def get_collections_in_version(client, args):
    # Return collections that have specified access
    # query = f"""
    # SELECT replace(replace(lower(tcia_api_collection_id),'-','_'),' ','_') collection_id
    # FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_collections`
    # WHERE tcia_access='{args.access}' OR idc_access='{args.access}'
    # """
    # collection_ids = dict(client.query(query))
    query = f"""
    SELECT replace(replace(lower(tcia_api_collection_id),'-','_'),' ','_') collection_id
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_collections`
    WHERE access='{args.access}'
    """
    collection_ids = [row.collection_id for row in client.query(query)]
    return collection_ids


def get_non_tcia_license_info(client, args):
    query = f"""
    SELECT * 
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.original_collections_metadata_idc_source`
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


# Get license data for all TCIA collections. Includes redacted collections.
# We assume the same license applies to both radiology and pathology except
# for redacted collections.
def get_tcia_license_info(client, args):
    licenses = {collection.lower().replace(' ','_').replace('-','_'): value for collection, value in get_collection_license_info().items()}
    return licenses

def get_original_collection_licenses(args):
    client = bigquery.Client()
    licenses = {}
    idc_collection_ids = get_collections_in_version(client, args)
    tcia_licenses = get_tcia_license_info(client, args)
    non_tcia_licenses = get_non_tcia_license_info(client, args)

    for collection in idc_collection_ids:
        if collection in tcia_licenses:
            license = tcia_licenses[collection]
            if not license['shortName'].startswith('TCIA') or args.access != 'Public':
                licenses[collection] = {
                    "tcia":
                        {
                            'license_url': license['licenseURL'],
                            'license_long_name': license['longName'],
                            'license_short_name': license['shortName'].strip()
                        }
                }
        # If this is a Public access list, add licenses for pathology
        if args.access == 'Public':
            if collection.startswith('cptac') or collection.startswith('tcga'):
                if collection in licenses:
                    # All cptac and tcga pathology is CC by 3.0
                    licenses[collection]['idc'] = {
                        'license_url': 'https://creativecommons.org/licenses/by/3.0/',
                        'license_long_name': 'Creative Commons Attribution 3.0 Unported License',
                        'license_short_name': 'CC BY 3.0'
                    }
                else:
                    licenses[collection] = {
                        'idc': {
                            'license_url': 'https://creativecommons.org/licenses/by/3.0/',
                            'license_long_name': 'Creative Commons Attribution 3.0 Unported License',
                            'license_short_name': 'CC BY 3.0'
                        }
                    }

            elif collection == 'nlst':
                # nlst pathology is CC by 4.0
                licenses[collection]['idc'] = {
                    'license_url': 'https://creativecommons.org/licenses/by/4.0/',
                    'license_long_name': 'Creative Commons Attribution 4.0 International License',
                    'license_short_name': 'CC BY 4.0'
                }

    # If this is a Public list, add licenses for the non_tcia (pathology) data for non-TCIA collections
    if args.access == 'Public':
        for collection, license in non_tcia_licenses.items():
            licenses[collection] = {
                "idc":
                    {
                        'license_url': license['licenseURL'],
                        'license_long_name': license['longName'],
                        'license_short_name': license['shortName'].strip()
                    }
            }
    return licenses


if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--access', default='Public', help='Public, Limited, or Excluded')
    args = parser.parse_args()

    print("{}".format(args), file=sys.stdout)

    get_original_collection_licenses(args)