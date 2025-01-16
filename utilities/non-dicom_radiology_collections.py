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

import os

import json
from subprocess import run, PIPE
from time import sleep
import requests
import logging
from google.cloud import bigquery


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
    radiology_downloads = {k['slug']: k for k in downloads if 'da-rad' in k['slug']}
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

    # Get all public IDC collections
    query = f"""
    SELECT DISTINCT collection_id
    FROM `bigquery-public-data.idc_current.dicom_all`
    """
    idc_collection_ids = [row['collection_id'] for row in client.query(query).result()]

    # Add the corresponding IDC collection if it exists
    for p, pdata in radiology_downloads.items():
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
    for p, pdata in radiology_downloads.items():
        pdata['pathology'] = pdata['parent_slug'].replace('-', '_') in idc_pathology_collection_ids

    with open('./path_downloads.csv', "w") as f:
        cnt = f.write(f'download slug,TCIA parent collection,IDC collection,Have pathology\n')
        for p in sorted(radiology_downloads):
            cnt = f.write(f'{p},{radiology_downloads[p]["parent_slug"]},{radiology_downloads[p]["idc_collection"]},{radiology_downloads[p]["pathology"]}\n')
        pass



    return


if __name__ == "__main__":
    main()


