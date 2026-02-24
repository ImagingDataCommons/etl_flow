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

# Generate a table of current licenses for all collections in the current version, indexed by source_doi.
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
import pandas as pd

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


def get_tcia_original_collection_licenses(client, args, tcia_downloads_metadata):
    # Get all the collection manager collections data:
    try:
        tcia_collection_metadata = {row['collection_short_title']:row for row in get_all_tcia_metadata('collections')}
    except Exception as exc:
        pass
    tcia_license_metadata = {row['license_label']:row for row in get_all_tcia_metadata('licenses')}

    tcia_licenses = []
    for collection_name, collection_metadata in tcia_collection_metadata.items():
        if collection_metadata['collection_page_accessibility'] == 'Limited':
            # print(f'Skipping collection {download_metadata["slug"]}')
            continue

        try:
            for id in collection_metadata['collection_downloads']:
                download_metadata = tcia_downloads_metadata[id]
                if not download_metadata['slug'].startswith(collection_metadata['slug']):
                    errlogger.error(f'Slug mismatch for {collection_metadata["slug"]}')
                    target_slug = {
                        'acrin_nsclc_fdg_pet': 'acrin-6668-da-rad',
                        'ct_colonography': 'acrin-6664-da-rad',
                        'vestibular-schwannoma-mc-rc2': 'vestibular-schwannoma-da-rad'
                    }[collection_metadata['slug']]
                else:
                    target_slug = collection_metadata['slug']

                if "Radiology Images" in str(download_metadata["download_type"]) and\
                        download_metadata['download_access'] == 'Public':
                    license_short_name = download_metadata['data_license']
                    tcia_licenses.append(
                        {
                            "collection_name": collection_name,
                            "source_doi": collection_metadata['collection_doi'].lower(),
                            "license_url": tcia_license_metadata[license_short_name]['license_url'],
                            "license_long_name": LICENSE_NAME_MAP[license_short_name],
                            "license_short_name": license_short_name,
                            "source": 'tcia'
                        }
                    )
                    break

        except Exception as exc:
            errlogger.error(exc)

    return pd.DataFrame(tcia_licenses)

# Get the source DOIs in all TCIA sourced data. This includes the DOIs of analysis results.
def get_tcia_dois(client, args):
    # Return collections that have specified access
    query = f"""
    SELECT DISTINCT lower(source_doi) source_doi, lower(source_url)  source_url
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_public`
    WHERE idc_version={settings.CURRENT_VERSION} 
    AND metadata_sunset = 0
    AND se_sources.tcia = True

    """
    dois = {row.source_doi: row for row in client.query(query)}
    return dois


# These are licenses of analysis results sourced from TCIA and therefore TCIA sets the licenses
def get_tcia_analysis_results_licenses(client, args, tcia_downloads_metadata):
    # Get TCIA Collection Manager analysis-results metadata of all TCIA analysis results
    all_tcia_analysis_results_metadata = {row['result_short_title']: row for row in get_all_tcia_metadata('analysis-results')}
    # Get all the download and license info from the collection manager.
    tcia_license_metadata = {row['license_label']:row for row in get_all_tcia_metadata('licenses')}

    tcia_licenses = []
    # Get the license for each AR that IDC has.
    for result_short_title, ar_metadata in all_tcia_analysis_results_metadata.items():
        try:
            for id in ar_metadata['result_downloads']:
                download_metadata = tcia_downloads_metadata[id]
                if "Radiology Images" in str(download_metadata["download_type"]) and \
                    "DICOM" in str(download_metadata["file_type"]) and \
                    download_metadata["download_access"] == "Public":
                    license_short_name = download_metadata['data_license']

                    tcia_licenses.append(
                        {
                            "collection_name": result_short_title,
                            "source_doi": ar_metadata['result_doi'].lower(),
                            "license_url": tcia_license_metadata[license_short_name]['license_url'],
                            "license_long_name": LICENSE_NAME_MAP[license_short_name],
                            "license_short_name": license_short_name,
                            "source": 'tcia'
                        }
                    )
                    break
        except Exception as exc:
            errlogger.error(exc)
    return pd.DataFrame(tcia_licenses)


# Get a dataframe of the licenses in all idc sourced collections indexed by source_doi
def get_idc_collection_licences(args):
    df = pd.read_json('table_generation_jsons/idc_original_collections_metadata.json5')
    df = df[['collection_name', 'source_doi', 'license_url', 'license_long_name', 'license_short_name']]
    df['source'] = 'idc'
    return df


# Get a dataframe of the licenses in all idc sourced analysis results, indexed by source_doi
def get_idc_analysis_results_licences(args):
    df = pd.read_json('table_generation_jsons/idc_analysis_results_metadata.json')
    df = df[['ID', 'source_doi', 'license_url', 'license_long_name', 'license_short_name']]
    df.rename(columns={'ID': 'collection_name'}, inplace=True),
    df['source'] = 'idc'
    return df


def source_dois(args, client):
    query = f"""
SELECT DISTINCT se.source_doi
FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.version` v
JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.version_collection` vc ON v.version = vc.version
JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.collection` c ON vc.collection_uuid = c.uuid
JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.collection_patient` cp ON c.uuid = cp.collection_uuid
JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.patient` p ON cp.patient_uuid = p.uuid
JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.patient_study` ps ON p.uuid = ps.patient_uuid
JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.study` st ON ps.study_uuid = st.uuid
JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.study_series` ss ON st.uuid = ss.study_uuid
JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.series` se ON ss.series_uuid = se.uuid
LEFT JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.doi_to_access` dtc ON se.source_doi = dtc.source_doi
LEFT JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.metadata_sunset` ms ON se.source_doi = ms.source_doi
WHERE se.excluded=FALSE AND se.redacted=FALSE
AND v.version=23
AND ms.metadata_sunset is NULL
AND (dtc.access IS NULL or dtc.access='Public')
"""
    query_job = client.query(query)
    source_dois = query_job.to_dataframe()
    return source_dois

def construct_licenses_table(args):
    client = bigquery.Client()

    idc_collection_licenses = get_idc_collection_licences(args)
    idc_analysis_results_licenses = get_idc_analysis_results_licences(args)
    tcia_downloads_metadata = {row['id']:row for row in get_all_tcia_metadata('downloads')}
    tcia_analysis_results_licenses = get_tcia_analysis_results_licenses(client, args, tcia_downloads_metadata)
    tcia_collection_licenses = get_tcia_original_collection_licenses(client, args, tcia_downloads_metadata)
    all_licenses = pd.concat([idc_collection_licenses, idc_analysis_results_licenses, tcia_collection_licenses, tcia_analysis_results_licenses])

    # Get a dataframe of all source_dois in the idc current version
    idc_source_dois = source_dois(args, client)
    licenses = idc_source_dois.merge(all_licenses, how='left', on='source_doi', sort='source_doi' )
    # Drop the collection_id and source columns. Only useful for debugging
    licenses = licenses[['source_doi', 'license_url', 'license_long_name', 'license_short_name']]
    # licenses = '\n'.join([json.dumps(row) for row in
    #                       sorted(all_licenses, key=lambda m: m['collection_name'])])

    try:

        job_config = bigquery.LoadJobConfig(
            # Specify a (partial) schema. All columns are always written to the
            # table. The schema is used to assist in data type definitions.
            # schema=[
            #     # Specify the type of columns whose type cannot be auto-detected. For
            #     # example the "title" column uses pandas dtype "object", so its
            #     # data type is ambiguous.
            #     bigquery.SchemaField("title", bigquery.enums.SqlTypeNames.STRING),
            #     # Indexes are written if included in the schema by name.
            #     bigquery.SchemaField("wikidata_id", bigquery.enums.SqlTypeNames.STRING),
            # ],
            # Optionally, set the write disposition. BigQuery appends loaded rows
            # to an existing table by default, but with WRITE_TRUNCATE write
            # disposition it replaces the table with the loaded data.
            write_disposition="WRITE_TRUNCATE",
        )
        table_id = f'idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.licenses'
        job = client.load_table_from_dataframe(
            licenses, table_id, job_config=job_config
        )  # Make an API request.
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )

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

    construct_licenses_table(args)
