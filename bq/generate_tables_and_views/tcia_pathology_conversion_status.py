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
# limitations under the License.https://docs.google.com/spreadsheets/d/1LYHDCg_YUYfOKtFwgpSsaNDm_8GLIQO4v475IsfAn7Q/edit?gid=1410925382#gid=1410925382
#

# This script generates a BQ table that documents the conversion status of available TCIA pathology downloads.

import os
import sys
import json
import argparse
import requests
import settings
from google.cloud import bigquery
from python_settings import settings
from utilities.bq_helpers import load_BQ_from_json
from utilities.logging_config import errlogger
import requests
import pandas as pd
import gspread
import gspread_formatting
from oauth2client.service_account import ServiceAccountCredentials

schema = [
    bigquery.SchemaField('download_slug', 'STRING', mode='NULLABLE', description='TCIA collection manager slug of this download'),
    bigquery.SchemaField('TCIA_parent_collection', 'STRING', mode='NULLABLE', description='ID of parent tcia collection'),
    bigquery.SchemaField('TCIA_download_title', 'STRING', mode='NULLABLE', description='Title of download'),
    bigquery.SchemaField('TCIA_download_type', 'STRING', mode='NULLABLE', description='Type of download'),
    bigquery.SchemaField('TCIA_file_types', 'STRING', mode='NULLABLE', description='File types in download'),
    bigquery.SchemaField('Download_size_GB', 'STRING', mode='NULLABLE', description='Download size'),
    bigquery.SchemaField('Download_url', 'STRING', mode='NULLABLE', description='Download url'),
    bigquery.SchemaField('TCIA_revision_date', 'Date', mode='NULLABLE', description='WHen was download last revised'),
    bigquery.SchemaField('IDC_collection_id', 'STRING', mode='NULLABLE', description='Corresponding IDC collection'),
    bigquery.SchemaField('IDC_revision_date', 'Date', mode='NULLABLE', description='WHen was IDC conversion last revised'),
    bigquery.SchemaField('IDC_is_current', 'Boolean', mode='NULLABLE', description='True if IDC revision is current with TCIA revision'),
    bigquery.SchemaField('IDC_version', 'Integer', mode='NULLABLE', description='IDC version of pathology data')]


def query_collection_manager(type, query_param=''):
    if query_param:
        url = f"https://cancerimagingarchive.net/api/v1/{type}/?per_page=100&{query_param}"
        # url = f"https://cancerimagingarchive.net/api/v1/{type}/?{query_param}"
    else:
        url = f"https://cancerimagingarchive.net/api/v1/{type}/?per_page=100"
        # url = f"https://cancerimagingarchive.net/api/v1/{type}/"
    response = requests.get(url)
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        while 'next' in response.links.keys():
            next_url = response.links['next']['url']
            response = requests.get(next_url)
            if response.status_code == 200:
                next_data = response.json()
                data.extend(next_data)
            else:
                print('Error accessing the API:', response.status_code)
                exit

        return data
    else:
        print('Error accessing the API:', response.status_code)
        exit


def export_to_bq(args, metadata):
    metadata_json = '\n'.join([json.dumps(row) for row in
                        sorted(metadata, key=lambda d: d['download_slug'])])
    try:
        BQ_client = bigquery.Client(project=settings.DEV_PROJECT)
        load_BQ_from_json(BQ_client,
                    settings.DEV_PROJECT,
                    settings.BQ_DEV_INT_DATASET , args.bqtable_name, metadata_json,
                                schema, write_disposition='WRITE_TRUNCATE')
        pass
    except Exception as exc:
        errlogger.error(f'Table creation failed: {exc}')
        exit

    return

def export_to_sheets(args, metadata):
    df = pd.DataFrame(metadata)

    # Define the scope
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']

    # Authenticate using the credentials file
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        f'{os.getenv("HOME")}/.config/gcloud/idc-etl-processing-e691db5d4745.json', scope)
    client = gspread.authorize(creds)

    # Open the Google Sheet (by title or URL key)
    sheets = client.open_by_key(args.sheets_key) # or client.open_by_key("your_sheet_key").sheet1

    try:
        sheet = sheets.worksheet(f"v{args.version}")
    except:
        sheet = sheets.add_worksheet(title=f"v{args.version}", rows=df.shape[0], cols=df.shape[1])
    sheet.clear()

    sheet.update([df.columns.values.tolist()] + df.values.tolist())

    sheet.columns_auto_resize(0, df.shape[1])
    sheet.format(["1"], {"textFormat": {"bold": True}})
    size_col =chr(65 + df.columns.get_loc("Download_size_GB"))
    sheet.format([size_col], {"horizontalAlignment": "RIGHT"})
    url_col =chr(65 + df.columns.get_loc("Download_url"))
    sheet.format([url_col], {"wrapStrategy": "CLIP"})
    gspread_formatting.set_column_width(sheet, url_col, 400)
    print("DataFrame exported to Google Sheets successfully.")

def main():
    client = bigquery.Client()

    downloads = query_collection_manager(type="downloads")
    pathology_downloads = {k['slug']: k for k in downloads if
                           (type(k['download_type']) == str and k['download_type'] == 'Pathology Images') or
                           ('Pathology Images' in k['download_type'])}
    collections = query_collection_manager(type="collections")

    # Find the collection that owns each pathology download
    for p, pdata in pathology_downloads.items():
        for c in collections:
            if pdata['id'] in c['collection_downloads']:
                pdata['parent_id'] = c['id']
                pdata['parent_slug'] = c['slug']
                pdata['parent'] = c
                break
        if 'parent' not in pdata:
            print(f'Did not find parent of {pdata["slug"]}')
            pdata['parent_id'] = ""
            pdata['parent_slug'] = ""
            pdata['parent'] = ""

    # Get all IDC collections
    query = f"""
    WITH versioned_collections AS (
        SELECT DISTINCT collection_id, max(instance_revised_idc_version) idc_version
        FROM `bigquery-public-data.idc_current.dicom_all` 
        GROUP BY collection_id, Modality, instance_revised_idc_version HAVING Modality='SM'
    )
    SELECT collection_id, vc.idc_version, vm.version_timestamp
    FROM versioned_collections vc
    JOIN `bigquery-public-data.idc_current.version_metadata` vm
    ON vc.idc_version = vm.idc_version
    """

    # idc_collections = dict(client.query(query).result())
    idc_collections = {row['collection_id']: {
        'idc_version': row['idc_version'],
        'version_timestamp': row['version_timestamp']
    } for row in client.query(query).result()}
    # Add the corresponding IDC collection if it exists
    for p, pdata in pathology_downloads.items():
        if pdata['parent_slug'].replace('-', '_') in idc_collections:
            pdata['idc_collection'] = pdata['parent_slug'].replace('-', '_')
        else:
            pdata['idc_collection'] = ""

    # Get all IDC collections that have pathology data
    query = f"""
    SELECT DISTINCT collection_id
    FROM `bigquery-public-data.idc_current.dicom_all`
    WHERE Modality='SM'
    """
    idc_pathology_collection_ids = [row['collection_id'] for row in client.query(query).result()]

    # Add the corresponding IDC collection if it exists
    for p, pdata in pathology_downloads.items():
        pdata['pathology'] = pdata['parent_slug'].replace('-', '_') in idc_pathology_collection_ids

    metadata = []
    for p, v in sorted(pathology_downloads.items()):
        if v["parent_slug"]:
            data = {
                'download_slug': p,
                'TCIA_parent_collection':v["parent_slug"],
                'TCIA_download_title':v["download_title"],
                'TCIA_download_type': str(v["download_type"]).replace("'", '"'),
                'TCIA_file_types': str(v["file_type"]).replace("'", '"'),
                'Download_size_GB': "",
                'Download_url': v['download_url'],
                'TCIA_revision_date':v["date_updated"],
                'IDC_collection_id':v["idc_collection"],
                'IDC_revision_date': "",
                'IDC_is_current': "",
                'IDC_version': ""
            }
            if v["download_size_unit"] == 'mb':
                data['Download_size_GB'] = f'{float(v["download_size"]) / 1024:.2f}'
            elif v["download_size_unit"] == 'tb':
                data['Download_size_GB'] = f'{float(v["download_size"]) * 1024:.2f}'
            else:
                data['Download_size_GB'] = f'{float(v["download_size"]):.2f}'

            if v["idc_collection"]:
                data['IDC_version'] = idc_collections[ data['IDC_collection_id'] ]['idc_version']
                data['IDC_revision_date'] = idc_collections[ data['IDC_collection_id'] ]['version_timestamp']
                data['IDC_is_current'] = 'True' if data['IDC_revision_date'] >= data['TCIA_revision_date'] else 'False'
            metadata.append(data)
    export_to_sheets(args, metadata)

    export_to_bq(args, metadata)

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='Version of the table')
    parser.add_argument('--bqtable_name', default='tcia_pathology_conversion_status', help='BQ table name')
    parser.add_argument('--sheets_key', default='1CcuidHbu7QbP43OxzURPuIwoZdTamg_4urYoE5z05wA', help='Google Sheets key')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    main()


