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

import json
import sys
import os
import requests
import backoff
import logging
from google.cloud import bigquery
from utilities.tcia_scrapers import scrape_tcia_data_collections_page
from utilities.tcia_helpers import get_TCIA_collections
from utilities.bq_helpers import load_BQ_from_json

NBIA_AUTH_URL = "https://public.cancerimagingarchive.net/nbia-api/oauth/token"
NBIA_INTERNAL_URL = "https://public.cancerimagingarchive.net/nbia-api/services"
NBIA_PUBLIC_URL = "https://services.cancerimagingarchive.net/nbia-api/services"

test_token = "7b374c1c-d5a8-4f63-8447-90c620a7fc9f"

@backoff.on_exception(backoff.expo,
                      requests.exceptions.RequestException,
                      max_tries=3)
def get_url(url):  # , headers):
    return requests.get(url)  # , headers=headers)

@backoff.on_exception(backoff.expo,
                      requests.exceptions.RequestException,
                      max_tries=3)
def nbia_get_inner(url):  # , headers):
    headers = dict(
        Authorization= f'Bearer {os.environ["NBIA_ACCESS_TOKEN"] if "NBIA_ACCESS_TOKEN" in os.environ else ""}')
    return requests.get(url, headers=headers)

def nbia_get(url):
    results = nbia_get_inner(url)
    if results.status_code == 401:
        nbia_refresh_access_token()
        results = nbia_get_inner(url)
    return results

@backoff.on_exception(backoff.expo,
                      requests.exceptions.RequestException,
                      max_tries=3)
def nbia_post_inner(url, data):  # , headers):
    headers = dict(
        Authorization= f'Bearer {os.environ["NBIA_ACCESS_TOKEN"] if "NBIA_ACCESS_TOKEN" in os.environ else ""}')
    return requests.post(url, headers=headers, data=data)  # , headers=headers)

def nbia_post(url, data):  # , headers):
    results = nbia_post_inner(url, data)
    if results.status_code == 401:
        nbia_refresh_access_token()
        results = nbia_post_inner(url, data)

    return  results

def nbia_refresh_access_token():
    data = dict(
        refresh_token=os.environ["NBIA_ACCESS_TOKEN"] if "NBIA_ACCESS_TOKEN" in os.environ else "",
        client_id="nbiaRestAPIClient",
        client_secret="ItsBetweenUAndMe",
        grant_type = "refresh_token")
    result = requests.post(NBIA_AUTH_URL, data)
    if result.status_code != 200:
        nbia_access_token()
    else:
        os.environ['NBIA_ACCESS_TOKEN'] = result.json()['access_token']
        os.environ['NBIA_REFRESH_TOKEN'] = result.json()['refresh_token']


def nbia_access_token():
    data = dict(
        username="nbia_guest",
        password="",
        client_id="nbiaRestAPIClient",
        client_secret="ItsBetweenUAndMe",
        grant_type="password")
    result = requests.post(NBIA_AUTH_URL, data)
    if result.status_code != 200:
        logging.error("Failed to get  NBIA access token")
        raise
    else:
        os.environ['NBIA_ACCESS_TOKEN'] = result.json()['access_token']
        os.environ['NBIA_REFRESH_TOKEN'] = result.json()['refresh_token']


# Get collection descriptions using internal TCIA endpoint
def get_collection_descriptions():
    url = f'{NBIA_INTERNAL_URL}/getCollectionDescriptions'
    results = nbia_get(url).json()
    return results


# Build a new table of collection metadata that includes descriptions and NBIA collection IDs as
# keys.
def add_descriptions_to_data_collections_metadata(descriptions_table, data_collections_table):
    for collection_id, data in sorted(data_collections_table.items()):
        try:
            data['description'] = descriptions_table[collection_id]
        except:
            data['description'] = ""
            logging.info(f'{collection_id} not in descriptions')
    return(data_collections_table)


def reindex_data_collections_metadata(data_collections_table, nbia_collection_ids):
    table = {}
    for metadata_collection, metadata_data in sorted(data_collections_table.items()):
        collection_id = metadata_data['nbia_collection_id']
        # if we found an nbia collection_id then metadata collection is a public radiology collection
        if collection_id != "":
            found = False
            # But collection_id might not be quite correct. nbia_collection_ids has correct nbia ids.
            # If there is a difference, it is just in capitalization
            for nbia_collection_id in nbia_collection_ids:
                if collection_id.lower() == nbia_collection_id.lower():
                    table[nbia_collection_id] = metadata_data
                    found = True
                    break
            if not found:
                table[collection_id] = metadata_data
                logging.warning(f'{collection_id}: No corresponding description. Using resolved nbia_collection_id')
    return(table)


def resolve_data_collections_metadata_ids(data_collections_metadata):
    for key,data in sorted(data_collections_metadata.items()):

        try:
            if data['DOI'].startswith('http'):
                result = get_url(data['DOI'])
            else:
                result = get_url(f"https://doi.org/{data['DOI']}")
            html = result.content.decode()
            data['nbia_collection_id'] = html.split('CollectionCriteria=',1)[1].split('"')[0].split('&')[0].replace('%20',' ')
        except:
            data['nbia_collection_id'] = ""
            logging.info(f'{key}: Could not resolve {data["DOI"]}')

    return data_collections_metadata

def build_data_collections_metadata_table():
    results = get_collection_descriptions()
    descriptions = {description['collectionName']:description['description'] for description in results}
    # "Sorting" the dictionary aids debugging
    descriptions = dict(sorted(descriptions.items()))

    # The keys of the descriptions dict are the collection_ids that NBIA APIs recognize.
    nbia_collection_ids = list(descriptions.keys())

    # Get data collection metadata by scraping TCIA data collections page
    data_collections_metadata = scrape_tcia_data_collections_page()
    for data in data_collections_metadata.values():
        data['Subjects'] = int(data['Subjects'])
    # "Sorting" the dictionary aids debugging
    data_collections_metadata = dict(sorted(data_collections_metadata.items()))
    # Follow the DOI in collection metadata to the corresponding wiki page, where the nbia collection ID is found
    data_collections_metadata = resolve_data_collections_metadata_ids(data_collections_metadata)

    # Reindex the data_collections_metadata table using the nbia_collections_ids as keys
    data_collections_metadata = reindex_data_collections_metadata(data_collections_metadata, nbia_collection_ids)

    collections_data = add_descriptions_to_data_collections_metadata(descriptions, data_collections_metadata)

    return collections_data


def load_data_collections_metadata_to_BQ(project, collections_data, dataset, table):
    BQ_client = bigquery.Client(project)
    schema = [
        bigquery.SchemaField('nbia_collection_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('IDC_Versions', 'STRING', mode='REPEATED'),
        bigquery.SchemaField('DOI', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('CancerType', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('Location', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('Species', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('Subjects', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('ImageTypes', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('SupportingData', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('Access', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('Status', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('Updated', 'DATE', mode='REQUIRED'),
        bigquery.SchemaField('description', 'STRING', mode='REQUIRED')
    ]

    # n = 1
    # while True:
    #     items = {item[0]:item[1] for item in list(collections_data.items())[0:n]}
    #     rows = [json.dumps(dict(IDC_Versions=['1'], **data)) for data in items.values()]
    #     json_rows = '\n'.join(rows)
    #     results = load_BQ_from_json(BQ_client, project, dataset, table, json_rows, schema, 'WRITE_TRUNCATE')
    #     n+=1

    rows = [json.dumps(dict(IDC_Versions=['1'],**data)) for data in collections_data.values()]
    json_rows = '\n'.join(rows)

    results = load_BQ_from_json(BQ_client, project, dataset, table, json_rows, schema, 'WRITE_TRUNCATE')



def compare_tcia_and_nbia_collection_ids():
    tcia_collections = get_TCIA_collections()
    nbia_collections = get_collection_values()
    tcia_ids = [c for c in tcia_collections]
    nbia_ids = [c['criteria'].replace(' ','_') for c in nbia_collections]
    tcia_ids.sort()
    nbia_ids.sort()

    for id in nbia_ids:
        if not id in tcia_ids:
            print(f'{id} is in nbia but not tcia')

    print("")

    for id in tcia_ids:
        if not id in nbia_ids:
            print (f'{id} is in tcia but not nbia')
    pass



# For each collection, collection ID and number of subjects
def get_collection_values():
    url = f'{NBIA_PUBLIC_URL}/getCollectionValuesAndCounts'
    results = nbia_get(url)
    return results.json()

def get_collection_data(collection_id):
    url = f'{NBIA_PUBLIC_URL}/getSimpleSearchWithModalityAndBodyPartPaged'
    data = dict(
        criteriaType0 = "CollectionCriteria",
        value0 = collection_id,
        sortField = "subject",
        sortDirection="descending",
        start=0,
        size=100000)
    results = nbia_post(url, data=data)
    return results.json()

def get_collection_patient_data(collection_id, subjectID):
    url = f'{NBIA_PUBLIC_URL}/getSimpleSearchWithModalityAndBodyPartPaged'
    data = dict(
        criteriaType0 = "CollectionCriteria",
        value0 = collection_id,
        criteriaType1 = "PatientCriteria",
        value1 = subjectID,
        sortField = "subject",
        sortDirection="descending",
        start=0,
        size=100000)
    results = nbia_post(url, data=data)
    return results.json()

def get_study_data(study_object):
    # subjectId = study_object['subjectId']
    url = f'{NBIA_PUBLIC_URL}/getStudyDrillDown'
    data = dict(
        list=study_object['seriesIdentifiers'],
        sortField="subject",
        sortDirection="descending",
        start=0,
        size=100000)
    results = nbia_post(url, data=data)
    return results.json()


if __name__ == "__main__":
    logging.basicConfig(filename='log.log'.format(os.environ['PWD']), filemode='w', level=logging.INFO)

    # access_token, refresh_token, expires_in = nbia_access_token()
    # access_token, refresh_token, expires_in = nbia_refresh_access_token(refresh_token)
    # results = compare_tcia_and_nbia_collection_ids()
    # compare_tcia_and_nbia_collection_ids()

    # collections_metadata = build_data_collections_metadata_table()
    # with open("data", 'w') as f:
    #      json.dump(collections_metadata,f)

    with open("data") as f:
        collections_metadata = json.load(f)
    result = load_data_collections_metadata_to_BQ('idc_peewee-dev-etl', collections_metadata, 'whc_dev',
                                                  'data_collections_metadata')
    collection_descriptions = get_collection_descriptions()
    collection_values = get_collection_values()

    collection_data = get_collection_data(collection_values[0]['criteria'])
    study_data = get_study_data(collection_data['resultSet'][0]['studyIdentifiers'][0])
    # data = get_collection_data(access_token, collections[1]['criteria'])
    pass
