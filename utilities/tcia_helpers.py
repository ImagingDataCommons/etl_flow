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

import json
import sys
import pycurl
import inspect
import zipfile
from subprocess import run, PIPE
import time, datetime
import random
from io import BytesIO, StringIO
from google.cloud import storage
import requests
import backoff
import logging

TCIA_URL = 'https://services.cancerimagingarchive.net/services/v4/TCIA/query'
NBIA_URL = 'https://services.cancerimagingarchive.net/nbia-api/services/v1'
# TCIA_URL = 'https://services.cancerimagingarchive.net/nbia-api/services/v2'


# @backoff.on_exception(backoff.expo,
#                       requests.exceptions.RequestException,
#                       max_tries=3)
def get_url(url):  # , headers):
    result =  requests.get(url)  # , headers=headers)
    if result.status_code != 200:
        raise RuntimeError('In get_url(): status_code=%s; url: %s', result.status_code, url)
    return result

def TCIA_API_request(endpoint, parameters="", nbia_server=True):
    server_url = NBIA_URL if nbia_server else TCIA_URL
    url = f'{server_url}/{endpoint}?{parameters}'
    results = get_url(url)
    results.raise_for_status()
    return results.json()


def TCIA_API_request_to_file(filename, endpoint, parameters="", nbia_server=True):
    server_url = NBIA_URL if nbia_server else TCIA_URL
    url = f'{server_url}/{endpoint}?{parameters}'
    begin = time.time()
    results = get_url(url)
    results.raise_for_status()
    with open(filename, 'wb') as f:
        f.write(results.content)
    duration = str(datetime.timedelta(seconds=(time.time() - begin)))
    logging.debug('File %s downloaded in %s',filename, duration)
    return 0


def get_collections(nbia_server=True):
    server_url = NBIA_URL if nbia_server else TCIA_URL
    url = f'{server_url}/getCollectionValues'
    results = get_url(url)
    collections = results.json()
    # collections = [collection['Collection'].replace(' ', '_') for collection in results.json()]
    return collections


def get_TCIA_patients_per_collection(collection_id, nbia_server=True):
    server_url = NBIA_URL if nbia_server else TCIA_URL
    url = f'{server_url}/getPatient?Collection={collection_id}'
    results = get_url(url)
    patients = results.json()
    return patients


def get_TCIA_studies_per_patient(collection, patientID, nbia_server=True):
    server_url = NBIA_URL if nbia_server else TCIA_URL
    url = f'{server_url}/getPatientStudy?Collection={collection}&PatientID={patientID}'
    results = get_url(url)
    studies = results.json()
    return studies


def get_TCIA_studies_per_collection(collection, nbia_server=True):
    server_url = NBIA_URL if nbia_server else TCIA_URL
    url = f'{server_url}/getPatientStudy?Collection={collection}'
    results = get_url(url)
    studies = results.json()
    return studies


def get_TCIA_series_per_study(collection, patientID, studyInstanceUID, nbia_server=True):
    server_url = NBIA_URL if nbia_server else TCIA_URL
    url = f'{server_url}/getSeries?Collection ={collection}&PatientID={patientID}&StudyInstanceUID={studyInstanceUID}'
    results = get_url(url)
    series = results.json()
    return series

def get_TCIA_instance_uids_per_series(seriesInstanceUID, nbia_server=True):
    server_url = NBIA_URL if nbia_server else TCIA_URL
    url = f'{server_url}/getSOPInstanceUIDs?SeriesInstanceUID={seriesInstanceUID}'
    results = get_url(url)
    instance_uids = results.json()
    return instance_uids

def get_TCIA_instance(seriesInstanceUID, sopInstanceUID, nbia_server=True):
    server_url = NBIA_URL if nbia_server else TCIA_URL
    url = f'{server_url}/getSingleImage?SeriesInstanceUID={seriesInstanceUID}&SOPInstanceUID={sopInstanceUID}'
    results = get_url(url)
    instances = results.json()
    return instances

# def get_TCIA_series_per_collection(collection):
#     results = TCIA_API_request('getSeries')
#     SeriesInstanceUIDs = [SeriesInstanceUID['SeriesInstanceUID'] for SeriesInstanceUID in results]
#     return SeriesInstanceUIDs

def get_TCIA_series_per_collection(collection, nbia_server=True):
    server_url = NBIA_URL if nbia_server else TCIA_URL
    url = f'{server_url}/getSeries?Collection={collection}'
    results = get_url(url)
    series = results.json()
    return series

def get_TCIA_series(nbia_server=True):
    results = TCIA_API_request('getSeries', nbia_server)
    # We only need a few values
    # We create a revision date field, filled with today's date (UTC +0), until TCIA returns a revision date 
    # in the response to getSeries
    today = datetime.date.today().isoformat()
    data = [{'CollectionID':result['Collection'],
          'StudyInstanceUID':result['StudyInstanceUID'],
          'SeriesInstanceUID':result['SeriesInstanceUID'],
          "SeriesInstanceUID_RevisionDate":today}
           for result in results]
    
    return data

def get_TCIA_instances_per_series(series_instance_uid, nbia_server=True):
    # Get a zip of the instances in this series to a file and unzip it
    result = TCIA_API_request_to_file("{}/{}.zip".format("dicom", series_instance_uid),
                "getImage", parameters="SeriesInstanceUID={}".format(series_instance_uid),
                nbia_server=nbia_server)

    # Now try to extract the instances to a directory DICOM/<series_instance_uid>
    try:
        with zipfile.ZipFile("{}/{}.zip".format("dicom", series_instance_uid)) as zip_ref:
            zip_ref.extractall("{}/{}".format("dicom", series_instance_uid))
        return
    except :
        logging.error("\tZip extract failed for series %s with error %s,%s,%s ", series_instance_uid,
                      sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        raise


def create_jsonlines_from_list(original):
    in_json = StringIO(json.dumps(original)) 
    result = [json.dumps(record) for record in json.load(in_json)]
    result = '\n'.join(result)
    return result


def get_collection_size(collection, nbia_server=True):
    size = 0
    serieses=TCIA_API_request('getSeries', parameters="Collection={}".format(collection.replace(' ','_')),
                              nbia_server=nbia_server)
    print("{} series in {}".format(len(serieses), collection), flush=True)
    for aseries in serieses:
        seriesSize=TCIA_API_request('getSeriesSize', parameters="SeriesInstanceUID={}".format(aseries['SeriesInstanceUID']),
                            nbia_server=nbia_server)
#             print(seriesSize)
        size += int(float(seriesSize[0]['TotalSizeInBytes']))
        print("{} {}\r".format(aseries['SeriesInstanceUID'], size),end="")
    return size


def get_collection_sizes_in_bytes(nbia_server=True):
    sizes = {}
    collections = get_collections(nbia_server)
    collections.sort(reverse=True)
    for collection in collections:
        sizes[collection] = get_collection_size(collection)
    return sizes


def get_collection_sizes(nbia_server=True):
    collections = get_collections(nbia_server)
    counts = {collection:0 for collection in collections}
    serieses=TCIA_API_request('getSeries', nbia_server)
    for aseries in serieses:
        counts[aseries['Collection']] += int(aseries['ImageCount'])
    sorted_counts = [(k, v) for k, v in sorted(counts.items(), key=lambda item: item[1])]
    return sorted_counts

def get_access_token():
    # data = "username=nbia_guest&password=&client_id=nbiaRestAPIClient&client_secret=ItsBetweenUAndMe&grant_type=password"
    data = dict(
        username="nbia_guest",
        password="",
        client_id="nbiaRestAPIClient",
        client_secret="ItsBetweenUAndMe",
        grant_type="password")
    url = "https://public.cancerimagingarchive.net/nbia-api/oauth/token"
    result = requests.post(url, data = data)
    access_token = result.json()
    return access_token

def get_collection_values_and_counts():
    access_token = get_access_token()['access_token']
    headers = dict(
        Authorization = f'Bearer {access_token}'
    )
    url = 'https://services.cancerimagingarchive.net/nbia-api/services/getCollectionValuesAndCounts'
    result = requests.get(url, headers=headers)
    collections = [collection['criteria'] for collection in result.json()]
    return collections


def get_collection_descriptions():
    # Get access token for the guest account
    result = run([
        'curl',
        '-v',
        '-d',
        "username=nbia_guest&password=&client_id=nbiaRestAPIClient&client_secret=ItsBetweenUAndMe&grant_type=password",
        '-X',
        'POST',
        '-k',
        "https://public.cancerimagingarchive.net/nbia-api/oauth/token"
        ], stdout=PIPE, stderr=PIPE)
    access_token = json.loads(result.stdout.decode())['access_token']
    result = run([
        'curl',
        '-H',
        "Authorization:Bearer {}".format(access_token),
        '-k',
        'https://public.cancerimagingarchive.net/nbia-api/services/getCollectionDescriptions'
        ], stdout=PIPE, stderr=PIPE)
    descriptions = json.loads(result.stdout.decode())
    collection_descriptions = {description['collectionName']: description['description'] for description in descriptions}

    return collection_descriptions


def get_series_info(storage_client, project, bucket_name):
    series_info = {}
    blobs = storage_client.bucket(bucket_name, user_project=project).list_blobs()
    series_info = {blob.name.rsplit('.dcm',1)[0]: {"md5_hash":blob.md5_hash, "size":blob.size} for blob in blobs}
    return series_info

def get_updated_series(date):
    access_token = get_access_token()['access_token']
    headers = dict(
        Authorization = f'Bearer {access_token}'
    )
    url = f'https://services.cancerimagingarchive.net/nbia-api/services/v2/getUpdatedSeries?fromDate={date}'
    result = requests.get(url, headers=headers)
    series = result.json()
    return series



if __name__ == "__main__":
    # patients = get_TCIA_patients_per_collection('ACRIN-FLT-Breast')
    # get_collection_descriptions()
    # series = get_TCIA_series_per_collection('TCGA-BRCA')
    # series = get_updated_series('06/06/2020')
    # print(time.asctime());studies = get_TCIA_studies_per_collection('BREAST-DIAGNOSIS', nbia_server=False);print(time.asctime())
    # studies = get_TCIA_studies_per_patient(collection.tcia_api_collection_id, patient.submitter_case_id)
    patients=get_TCIA_patients_per_collection('CBIS-DDSM')

    collection = get_collection_values_and_counts()
    collections = get_collections()
    for collection in collections:
        patients = get_TCIA_patients_per_collection(collection['Collection'])
        for patient in patients:
            studies = get_TCIA_studies_per_patient(collection['Collection'], patient['PatientId'])
            for study in studies:
                seriess = get_TCIA_series_per_study(collection['Collection'], patient['PatientId'], study['StudyInstanceUID'])
                for series in seriess:
                    instanceUIDs = get_TCIA_instance_uids_per_series(series['SeriesInstanceUID'])
                    for instanceUID in instanceUIDs:
                        instance = get_TCIA_instance(series['SeriesInstanceUID'], instanceUID['SOPInstanceUID'])


