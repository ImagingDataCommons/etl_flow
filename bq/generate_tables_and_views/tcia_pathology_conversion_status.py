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

import sys
import json
import argparse
import requests
from google.cloud import bigquery
from python_settings import settings
from utilities.bq_helpers import load_BQ_from_json
from utilities.logging_config import errlogger
import requests
import logging
from google.cloud import bigquery

schema = [
    bigquery.SchemaField('download_slug', 'STRING', mode='NULLABLE', description='TCIA collection manager slug of this download'),
    bigquery.SchemaField('TCIA_parent_collection', 'STRING', mode='NULLABLE', description='ID of parent tcia collection'),
    bigquery.SchemaField('IDC_collection_id', 'STRING', mode='NULLABLE', description='Corresponding IDC collection'),
    bigquery.SchemaField('Have_pathology', 'STRING', mode='NULLABLE', description='True if IDC has this pathology download')
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


def main():
    client = bigquery.Client()

    downloads = query_collection_manager(type="downloads")
    pathology_downloads = {k['slug']: k for k in downloads if 'da-path' in k['slug']}
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
    SELECT DISTINCT collection_id
    FROM `bigquery-public-data.idc_current.dicom_all`
    """
    idc_collection_ids = [row['collection_id'] for row in client.query(query).result()]

    # Add the corresponding IDC collection if it exists
    for p, pdata in pathology_downloads.items():
        if pdata['parent_slug'].replace('-', '_') in idc_collection_ids:
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
    for p in sorted(pathology_downloads):
        metadata.append({
            'download_slug': p,
            'TCIA_parent_collection': pathology_downloads[p]["parent_slug"],
            'IDC_collection_id': pathology_downloads[p]["idc_collection"],
            'Have_pathology': pathology_downloads[p]["pathology"]
        })

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

    # with open('./path_downloads.csv', "w") as f:
    #     cnt = f.write(f'download slug,TCIA parent collection,IDC collection,Have pathology\n')
    #     for p in sorted(pathology_downloads):
    #         cnt = f.write(f'{p},{pathology_downloads[p]["parent_slug"]},{pathology_downloads[p]["idc_collection"]},{pathology_downloads[p]["pathology"]}\n')
    #     pass

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--bqtable_name', default='tcia_pathology_conversion_status', help='BQ table name')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    main()


