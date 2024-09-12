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

# Generate a table of current licenses for all collections
# Note that this includes the license of the collection itself
# as well as the license(s) of any analysis results in the
# collection

import argparse
import sys
import json
import settings
from google.cloud import bigquery
from utilities.bq_helpers import load_BQ_from_json, delete_BQ_Table
from utilities.tcia_helpers import get_all_tcia_metadata
from utilities.logging_config import progresslogger, errlogger

LICENSE_NAME_MAP = {
    'CC BY 3.0': 'Creative Commons Attribution 3.0 Unported License',
    'CC BY 4.0': 'Creative Commons Attribution 4.0 International License',
    'CC BY-NC 3.0': 'Creative Commons Attribution-NonCommercial 3.0 Unported License',
    'CC BY-NC 4.0': 'Creative Commons Attribution-NonCommercial 4.0 International License'
    }


licenses_schema = [
    bigquery.SchemaField('collection_name', 'STRING', mode='NULLABLE', description='Collection name as used externally by IDC webapp'),
    bigquery.SchemaField('source_doi','STRING', mode='NULLABLE', description='DOI that can be resolved at doi.org to a wiki page'),
    bigquery.SchemaField('source_url','STRING', mode='NULLABLE', description='URL of collection information page'),
    bigquery.SchemaField('source','STRING', mode='NULLABLE', description='Source of thise subcollection, "tcia" or "idc"'),
    bigquery.SchemaField(
        "license",
        "RECORD",
        mode="NULLABLE",
        fields=[
            bigquery.SchemaField('license_url', 'STRING', mode='NULLABLE',
                                 description='URL of license of this analysis result'),
            bigquery.SchemaField('license_long_name', 'STRING', mode='NULLABLE',
                                 description='Long name of license of this analysis result'),
            bigquery.SchemaField('license_short_name', 'STRING', mode='NULLABLE',
                                 description='Short name of license of this analysis result')
        ]
    )
]


# Generate a dict of tcia-sourced collections in the current version,
# indexed by "idc_webapp_collection_ids" and mapping to
# "tcia_api_collection_id". Includes sources of each collection.
def get_all_tcia_collections_in_version(client, args):
    # Return collections that have specified access
    query = f"""
    SELECT DISTINCT tcia_api_collection_id collection_name
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_collections`
    WHERE tcia_access='Public' and (tcia_metadata_sunset=0 or tcia_metadata_sunset>={settings.CURRENT_VERSION})
    ORDER BY collection_name
    """
    collections = {row.collection_name.lower().replace(' ','_').replace('-','_'): dict(row.items())\
        for row in client.query(query)}
    return collections

# These are licenses of IDC sourced subcollections. This includes pathology data which IDC has converted to DICOM
# and which we therefore consider to be the source. It also includes IDC source analysis results. Those results
# may be against a TCIA sourced radiology collection. In such a case the analysis result license may differ from
# the original data license.
def get_idc_sourced_collection_licenses(client):
    query = f"""
WITH res AS (
  SELECT
    DISTINCT collection_id AS collection_name, 
    source_doi,
    source_url,
    license_url,
    license_long_name,
    license_short_name
  FROM
    {settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.idc_all_joined )
SELECT
  collection_name, 
  lower(source_doi) source_doi,
  lower(source_url) source_url,
  'idc' source,
  STRUCT(license_url,
    license_long_name,
    license_short_name) license
FROM
  res
ORDER BY
  collection_name
    """
    # license_dicts = {dict(row)['source_doi']: dict(row) for row in client.query(query)}
    license_dicts = [dict(row) for row in client.query(query)]
    return license_dicts


# These are licenses of (sub)collections for which the data originates in TCIA and therefore TCIA sets the licenses
def get_tcia_original_collection_licenses(client, args, tcia_sourced_subcollections):
    # Get all the collection manager collections data:
    tcia_collection_metadata = {row['collection_short_title']:row for row in get_all_tcia_metadata('collections')}
    tcia_downloads_metadata = {row['id']:row for row in get_all_tcia_metadata('downloads')}
    tcia_licese_metadata = {row['license_label']:row for row in get_all_tcia_metadata('licenses')}

    tcia_licenses = []
    for collection_id, values in tcia_sourced_subcollections.items():
        collection_name = values['collection_name']
        try:
            collection_metadata = tcia_collection_metadata[collection_name]
            if collection_metadata['collection_page_accessibility'] == 'Limited':
                # print(f'Skipping collection {download_metadata["slug"]}')
                continue

        except Exception as exc:
            errlogger.error(f'No collection manager data for {collection_name}')
            id_map = {
                'ACRIN-NSCLC-FDG-PET': 'ACRIN 6668',
                'CT COLONOGRAPHY': 'ACRIN 6664',
                'Prostate-Anatomical-Edge-Cases': 'Prostate Anatomical Edge Cases',
                'QIN-BREAST': 'QIN-Breast'
            }
            try:
                collection_metadata = tcia_collection_metadata[id_map[collection_name]]
            except Exception as exc:
                errlogger.error(f'{collection_id} not in tcia_collection_metadat')
                continue

        try:
            for id in collection_metadata['collection_downloads']:
                download_metadata = tcia_downloads_metadata[id]
                if not download_metadata['slug'].startswith(collection_metadata['slug']):
                    errlogger.error(f'Slug mismatch for {collection_id}')
                    target_slug = {
                        'acrin_nsclc_fdg_pet': 'acrin-6668-da-rad',
                        'ct_colonography': 'acrin-6664-da-rad'
                    }[collection_id]
                else:
                    target_slug = collection_metadata['slug']

                if download_metadata['slug'].startswith(target_slug) and \
                        ('-rad-' in download_metadata['slug'] or download_metadata['slug'].endswith('-rad')) and\
                        download_metadata['download_access'] == 'Public':
                    license_short_name = download_metadata['data_license']
                    tcia_licenses.append(
                        {
                            "collection_name": collection_name,
                            "source_doi": collection_metadata['collection_doi'].lower(),
                            "source_url": f'https://doi.org/{collection_metadata["collection_doi"].lower()}',
                            "source": 'tcia',
                            "license": {
                                "license_url": tcia_licese_metadata[license_short_name]['license_url'],
                                "license_long_name": LICENSE_NAME_MAP[license_short_name],
                                "license_short_name": license_short_name
                            }
                        }
                    )
                    break
                # else:
                # # elif download_metadata['slug'].startswith(target_slug):
                #     errlogger.error(f'Target slug: {target_slug}; Found slug: {download_metadata["slug"]} ')
            if not next((collection for collection in tcia_licenses if collection['collection_name'] == collection_name),0):
                errlogger.error(f'No licenses found for TCIA collection {collection_name}')
                # license_short_name = 'CC BY 4.0'
                # tcia_licenses.append(
                #     {
                #         "collection_name": collection_name,
                #         "source_doi": collection_metadata['collection_doi'].lower(),
                #         "source_url": f'https://doi.org/{collection_metadata["collection_doi"].lower()}',
                #         "source": 'tcia',
                #         "license": {
                #             "license_url": tcia_licese_metadata[license_short_name]['license_url'],
                #             "license_long_name": LICENSE_NAME_MAP[license_short_name],
                #             "license_short_name": license_short_name
                #         }
                #     }
                # )

        except Exception as exc:
            errlogger.error(exc)

    return tcia_licenses

# Get the source DOIs in all TCIA sourced data. This includes the DOIs of analysis results.
def get_tcia_dois(client, args):
    # Return collections that have specified access
    query = f"""
    SELECT DISTINCT lower(source_doi) source_doi, lower(source_url)  source_url
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_public_and_current`
    WHERE se_sources.tcia = True
    """
    dois = {row.source_doi: row for row in client.query(query)}
    return dois


# These are licenses of analysis results sourced from TCIA and therefore TCIA sets the licenses
def get_tcia_analysis_results_licenses(client, args):
    # Get a list of all the source_dois in the current IDC data sourced from TCIA
    idc_ar_dois = get_tcia_dois(client, args)

    # Get TCIA Collection Manager analysis-results metadata of all TCIA analysis results
    all_tcia_analysis_results_metadata = get_all_tcia_metadata('analysis-results')
    # Keep only the metadata of analysis results which IDC has
    # These are the only ARs for which we need licenses
    tcia_ar_metadata = {row['result_short_title']:row for row in all_tcia_analysis_results_metadata \
                        if row['result_doi'].lower() in idc_ar_dois}

    # Get all the download and license info from the collection manager.
    tcia_downloads_metadata = {row['id']:row for row in get_all_tcia_metadata('downloads')}
    tcia_license_metadata = {row['license_label']:row for row in get_all_tcia_metadata('licenses')}

    tcia_licenses = []
    # Get the license for each AR that IDC has.
    for result_short_title, ar_metadata in tcia_ar_metadata.items():
        try:
            for id in ar_metadata['result_downloads']:
                download_metadata = tcia_downloads_metadata[id]
                if not download_metadata['slug'].startswith(ar_metadata['slug']):
                    errlogger.error(f'Slug mismatch for {result_short_title}')
                else:
                    target_slug = ar_metadata['slug'] + '-da-rad'

                if download_metadata['slug'] == target_slug:
                    license_short_name = download_metadata['data_license']
                    tcia_licenses.append(
                        {
                            "collection_name": result_short_title,
                            "source_doi": ar_metadata['result_doi'].lower(),
                            "source_url": f'https://doi.org/{ar_metadata["result_doi"].lower()}',
                            "source": 'tcia',
                            "license": {
                                "license_url": tcia_license_metadata[license_short_name]['license_url'],
                                "license_long_name": LICENSE_NAME_MAP[license_short_name],
                                "license_short_name": license_short_name
                            }
                        }
                    )
                    break
            if not next((collection for collection in tcia_licenses if collection['collection_name'] == result_short_title),0):
                errlogger.error(f'No licenses found for TCIA analysis result {result_short_title}')
        except Exception as exc:
            errlogger.error(exc)
    return tcia_licenses


def gen_licenses_table(args):
    client = bigquery.Client()

    tcia_sourced_subcollections = get_all_tcia_collections_in_version(client, args)

    # Get the licenses of subcollections sourced from TCIA
    tcia_sourced_licenses = get_tcia_original_collection_licenses(client, args, tcia_sourced_subcollections)

    # Get the licenses of analysis results sourced from TCIA
    tcia_ar_licenses = get_tcia_analysis_results_licenses(client, args)

    # Get the licenses of subcollections sourced from IDC
    idc_sourced_licenses = get_idc_sourced_collection_licenses(client)


    all_licenses = idc_sourced_licenses
    all_licenses.extend(tcia_sourced_licenses)
    all_licenses.extend(tcia_ar_licenses)
    licenses = '\n'.join([json.dumps(row) for row in
                          sorted(all_licenses, key=lambda m: m['collection_name'])])

    try:
        delete_BQ_Table(client, settings.DEV_PROJECT, settings.BQ_DEV_INT_DATASET, args.bqtable_name)
        load_BQ_from_json(client,
                    settings.DEV_PROJECT,
                    settings.BQ_DEV_INT_DATASET , args.bqtable_name, licenses,
                                aschema=licenses_schema, write_disposition='WRITE_TRUNCATE')
        pass
    except Exception as exc:
        errlogger.error(f'Table creation failed: {exc}')
        exit


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version for which to build the table')
    parser.add_argument('--project', default=f'{settings.DEV_PROJECT}')
    parser.add_argument('--bqdataset_name', default=f'idc_v{settings.CURRENT_VERSION}_dev', help='BQ dataset name')
    parser.add_argument('--bqtable_name', default=f'licenses', help='BQ table name')
    args = parser.parse_args()

    print("{}".format(args), file=sys.stdout)

    # args.sql = open(args.sql).read()

    gen_licenses_table(args)
