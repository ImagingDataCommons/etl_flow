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

# Identify all TCIA radiology downloads which are not DICOM encoded.
# Results are written to a BQ table

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
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials


schema = [
    bigquery.SchemaField('download_slug', 'STRING', mode='NULLABLE', description='TCIA collection manager slug of this download'),
    bigquery.SchemaField('tcia_parent_collection', 'STRING', mode='NULLABLE', description='ID of parent tcia collection'),
    bigquery.SchemaField('file_type', 'STRING', mode='NULLABLE', description='Comma separated list of file types')
]


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
    sheet = client.open_by_key(args.sheets_key) # or client.open_by_key("your_sheet_key").sheet1

    try:
        worksheet = sheet.worksheet(f"v{args.version}")
    except:
        worksheet = sheet.add_worksheet(title=f"v{args.version}", rows=df.shape[0], cols=df.shape[1])
    worksheet.clear()

    worksheet.update([df.columns.values.tolist()] + df.values.tolist())

    worksheet.columns_auto_resize(0, df.shape[1])

    print("DataFrame exported to Google Sheets successfully.")


def main():
    client = bigquery.Client()

    downloads = query_collection_manager(type="downloads")
    radiology_downloads = {k['slug']: k for k in downloads if k['download_type'] == 'Radiology Images'}
    non_dicom_radiology_downloads = {k:v for k,v in radiology_downloads.items() if v['file_type'] != ['DICOM']}
    collections = query_collection_manager(type="collections")

    # Find the collection that owns each radiology download
    for p, pdata in non_dicom_radiology_downloads.items():
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

    metadata = []
    for p in sorted(non_dicom_radiology_downloads):
        if non_dicom_radiology_downloads[p]["parent_slug"]:
            metadata.append({
                'download_slug': p,
                'tcia_parent_collection': non_dicom_radiology_downloads[p]["parent_slug"],
                'file_type': ', '.join(non_dicom_radiology_downloads[p]["file_type"])
            })

    export_to_sheets(args, metadata)

    export_to_bq(args, metadata)

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='Version of the table')
    parser.add_argument('--bqtable_name', default='tcia_non_dicom_radiology_downloads', help='BQ table name')
    parser.add_argument('--sheets_key', default='1-suKZbBQd8Q957hzN6YlCp3s5-_SH5BQxjjrwcQihFk', help='Google Sheets key')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    main()


