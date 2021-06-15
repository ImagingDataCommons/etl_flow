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
from subprocess import run, PIPE
import time, datetime
from io import StringIO
import requests
import logging

from python_settings import settings


TIMEOUT=60
CHUNK_SIZE=1024*1024

TCIA_URL = 'https://services.cancerimagingarchive.net/services/v4/TCIA/query'
NBIA_URL = 'https://services.cancerimagingarchive.net/nbia-api/services/v1'
NBIA_AUTH_URL = "https://public.cancerimagingarchive.net/nbia-api/oauth/token"
NLST_URL = 'https://nlst.cancerimagingarchive.net/nbia-api/services'
NLST_V2_URL = 'https://nlst.cancerimagingarchive.net/nbia-api/services/v2'
NLST_AUTH_URL = 'https://nlst.cancerimagingarchive.net/nbia-api/oauth/token'


# @backoff.on_exception(backoff.expo,
#                       requests.exceptions.RequestException,
#                       max_tries=3)
def get_url(url, headers=""):  # , headers):
    result =  requests.get(url, headers=headers, timeout=TIMEOUT)  # , headers=headers)
    if result.status_code != 200:
        raise RuntimeError('In get_url(): status_code=%s; url: %s', result.status_code, url)
    return result

def get_access_token(auth_server = NBIA_AUTH_URL):
    if auth_server == NLST_AUTH_URL:
        data = dict(
            username=settings.TCIA_ID,
            password=settings.TCIA_PASSWORD,
            client_id=settings.TCIA_CLIENT_ID,
            client_secret=settings.TCIA_CLIENT_SECRET,
            grant_type="password")
    else:
        data = dict(
            username="nbia_guest",
            password="",
            client_id=settings.TCIA_CLIENT_ID,
            client_secret=settings.TCIA_CLIENT_SECRET,
            grant_type="password")

    result = requests.post(auth_server, data = data)
    access_token = result.json()['access_token']
    return access_token


# def get_access_token(url="https://public.cancerimagingarchive.net/nbia-api/oauth/token"):
#     data = dict(
#         username="nbia_guest",
#         password="",
#         client_id=settings.TCIA_CLIENT_ID,
#         client_secret=settings.TCIA_CLIENT_SECRET,
#         grant_type="password")
#     # url = "https://public.cancerimagingarchive.net/nbia-api/oauth/token"
#     result = requests.post(url, data = data)
#     access_token = result.json()
#     return access_token


def get_collection_values_and_counts(server=NBIA_URL):
    if server == "NLST":
        server_url = NLST_URL
        access_token = get_access_token(NLST_AUTH_URL)
    else:
        server_url = NBIA_URL
        access_token = get_access_token()
    headers = dict(
        Authorization = f'Bearer {access_token}'
    )
    url = f'{server_url}/getCollectionValuesAndCounts'
    result = requests.get(url, headers=headers)
    collections = [collection['criteria'] for collection in result.json()]
    return collections


def get_TCIA_patients_per_collection(collection_id, server=NBIA_URL):
    if server == "NLST":
        server_url = NLST_V2_URL
        access_token = get_access_token(NLST_AUTH_URL)
        headers = dict(
            Authorization=f'Bearer {access_token}'
        )

    else:
        server_url = NBIA_URL
        headers = ''
    url = f'{server_url}/getPatient?Collection={collection_id}'
    results = get_url(url, headers)
    patients = results.json()
    return patients


def get_TCIA_studies_per_patient(collection, patientID, server=NBIA_URL):
    if server == "NLST":
        server_url = NLST_V2_URL
        access_token = get_access_token(NLST_AUTH_URL)
        headers = dict(
            Authorization=f'Bearer {access_token}'
        )

    else:
        server_url = NBIA_URL
        headers = ''
    url = f'{server_url}/getPatientStudy?Collection={collection}&PatientID={patientID}'
    results = get_url(url, headers)
    studies = results.json()
    return studies


def get_TCIA_studies_per_collection(collection, nbia_server=True):
    server_url = NBIA_URL if nbia_server else TCIA_URL
    url = f'{server_url}/getPatientStudy?Collection={collection}'
    results = get_url(url)
    studies = results.json()
    return studies


def get_TCIA_series_per_study(collection, patientID, studyInstanceUID, server=NBIA_URL):
    if server == "NLST":
        server_url = NLST_V2_URL
        access_token = get_access_token(NLST_AUTH_URL)
        headers = dict(
            Authorization=f'Bearer {access_token}'
        )

    else:
        server_url = NBIA_URL
        headers = ''
    url = f'{server_url}/getSeries?Collection ={collection}&PatientID={patientID}&StudyInstanceUID={studyInstanceUID}'
    results = get_url(url, headers)
    series = results.json()
    return series

def get_TCIA_instance_uids_per_series(seriesInstanceUID, server='NBIA'):
    if server == "NLST":
        server_url = NLST_V2_URL
        access_token = get_access_token(NLST_AUTH_URL)
        headers = dict(
            Authorization=f'Bearer {access_token}'
        )
    else:
        server_url = NBIA_URL
        headers = ''
    url = f'{server_url}/getSOPInstanceUIDs?SeriesInstanceUID={seriesInstanceUID}'
    results = get_url(url, headers)
    instance_uids = results.json()
    return instance_uids

def get_TCIA_instances_per_series(dicom, series_instance_uid, server="NBIA"):
    filename = "{}/{}.zip".format(dicom, series_instance_uid)
    if server == "NLST":
        server_url = NLST_V2_URL
        access_token = get_access_token(NLST_AUTH_URL)
        url = f'{server_url}/getImage?SeriesInstanceUID={series_instance_uid}'
        headers = f'Authorization:Bearer {access_token}'
        result = run([
            'curl',
            '-o',
            filename,
            '-H',
            headers,
            '-k',
            url
        ], stdout=PIPE, stderr=PIPE)

    else:
        if server == 'TCIA':
            server_url = TCIA_URL
        else:
            server_url = NBIA_URL
        url = f'{server_url}/getImage?SeriesInstanceUID={series_instance_uid}'

        result = run([
            'curl',
            '--max-time',
            600,
            '-o',
            filename,
            url
        ], stdout=PIPE, stderr=PIPE)
    # result = json.loads(result.stdout.decode())['access_token']

    # Now try to extract the instances to a directory DICOM/<series_instance_uid>
    try:
        # with zipfile.ZipFile("{}/{}.zip".format(dicom, series_instance_uid)) as zip_ref:
        #     zip_ref.extractall("{}/{}".format(dicom, series_instance_uid))
        result = run([
            'unzip',
            "{}/{}.zip".format(dicom, series_instance_uid),
            '-d',
            "{}/{}".format(dicom, series_instance_uid)
        ], stdout=PIPE, stderr=PIPE)

        return
    except :
        logging.error("\tZip extract failed for series %s with error %s,%s,%s ", series_instance_uid,
                      sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        raise


def get_TCIA_series_per_collection(collection, nbia_server=True):
    server_url = NBIA_URL if nbia_server else TCIA_URL
    url = f'{server_url}/getSeries?Collection={collection}'
    results = get_url(url)
    series = results.json()
    return series


# Get NBIAs internal ID for all the series in a collection/patient
def get_internal_series_ids(collection, patient, third_party="yes", size=100000, server="" ):
    if server == "NLST":
        server_url = NLST_URL
        access_token = get_access_token(NLST_AUTH_URL)
    else:
        server_url = NBIA_URL
        access_token = get_access_token(NBIA_AUTH_URL)
    headers = dict(
        Authorization=f'Bearer {access_token}'
    )
    url = f'{server_url}/getSimpleSearchWithModalityAndBodyPartPaged'
    if not patient=="":
        data = dict(
            criteriaType0="ThirdPartyAnalysis",
            value0=third_party,
            criteriaType1="CollectionCriteria",
            value1=collection,
            criteriaType2="PatientCriteria",
            value2=patient,
            sortField="subject",
            sortDirection="descending",
            start=0,
            size=size)
    else:
        data = dict(
            criteriaType0="ThirdPartyAnalysis",
            value0=third_party,
            criteriaType1="CollectionCriteria",
            value1=collection,
            sortField="subject",
            sortDirection="descending",
            start=0,
            size=size)

    result = requests.post(
        url,
        headers=headers,
        data=data
    )
    return result.json()


def series_drill_down(series_ids, server="" ):
    if server == "NLST":
        server_url = NLST_URL
        access_token = get_access_token(NLST_AUTH_URL)
    else:
        server_url = NBIA_URL
        access_token = get_access_token(NBIA_AUTH_URL)
    url = f'{server_url}/getStudyDrillDown'
    data = "&".join(['list={}'.format(id) for id in series_ids])

    try:
        result = run(['curl', '-v', '-H', "Authorization:Bearer {}".format(access_token), '-k',  url, '-d', data],
                     stdout=PIPE, stderr=PIPE)
    except:
        pass
    return json.loads(result.stdout.decode())



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


def get_collection_descriptions():
    # Get access token for the guest account
    access_token = get_access_token()
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


def get_updated_series(date):
    access_token = get_access_token()
    headers = dict(
        Authorization = f'Bearer {access_token}'
    )
    url = f'https://services.cancerimagingarchive.net/nbia-api/services/v2/getUpdatedSeries?fromDate={date}'
    result = requests.get(url, headers=headers)
    if result.status_code == 500 and result.text == 'No data found.':
        series = []
    else:
        series = result.json()
    return series


def get_hash(request_data, access_token=None):
    if not access_token:
        access_token = get_access_token(url = "https://public-dev.cancerimagingarchive.net/nbia-api/oauth/token")
    headers = dict(
        Authorization = f'Bearer {access_token}'
    )
    url = "https://public-dev.cancerimagingarchive.net/nbia-api/services/getMD5Hierarchy"
    result = requests.post(url, headers=headers, data=request_data)

    return result

def get_images_with_md5_hash(SeriesInstanceUID, access_token=None):
    if not access_token:
        access_token = get_access_token(auth_server = "https://public-dev.cancerimagingarchive.net/nbia-api/oauth/token")
    headers = dict(
        Authorization = f'Bearer {access_token}'
    )
    server_url = "https://public-dev.cancerimagingarchive.net/nbia-api/services/v1"
    # server_url = "https://tracker.nci.nih.gov/browse/NBIA-1478"
    url = f'{server_url}/getImageWithMD5Hash?SeriesInstanceUID={SeriesInstanceUID}'
    result = requests.get(url, headers=headers)

    return result


def get_access_token_dev(url="https://public.cancerimagingarchive.net/nbia-api/oauth/token"):
    data = dict(
        username=settings.TCIA_ID,
        password=settings.TCIA_PASSWORD,
        client_id=settings.TCIA_CLIENT_ID,
        client_secret=settings.TCIA_CLIENT_SECRET,
        grant_type="password")
    # url = "https://public.cancerimagingarchive.net/nbia-api/oauth/token"
    result = requests.post(url, data = data)
    access_token = result.json()
    return access_token


def get_patients_per_collection_dev(collection_id):
    access_token = get_access_token_dev(url = "https://nlst.cancerimagingarchive.net/nbia-api/oauth/token")
    headers = dict(
        Authorization = f'Bearer {access_token}'
    )

    server_url = 'https://nlst.cancerimagingarchive.net/nbia-api/services/v2'
    url = f'{server_url}/getPatient?Collection={collection_id}'
    results = requests.get(url, headers=headers)
    collections = results.json()
    # collections = [collection['Collection'].replace(' ', '_') for collection in results.json()]
    return collections

def get_collections_dev():
    access_token = get_access_token_dev(url = "https://nlst.cancerimagingarchive.net/nbia-api/oauth/token")
    headers = dict(
        Authorization = f'Bearer {access_token}'
    )

    server_url = 'https://nlst.cancerimagingarchive.net/nbia-api/services/v2'
    url = f'{server_url}/getCollectionValues'
    results = requests.get(url, headers=headers)
    collections = results.json()
    # collections = [collection['Collection'].replace(' ', '_') for collection in results.json()]
    return collections

def get_collection_values_and_counts_dev():
    access_token = get_access_token(url = "https://nlst.cancerimagingarchive.net/nbia-api/oauth/token")
    headers = dict(
        Authorization = f'Bearer {access_token}'
    )
    url = 'https://nlst.cancerimagingarchive.net/nbia-api/services/v2/getSimpleSearchCriteriaValues'
    result = requests.get(url, headers=headers)
    collections = [collection['criteria'] for collection in result.json()]
    return collections

def v2_api(endpoint, data):
    access_token = get_access_token(url = "https://services.cancerimagingarchive.net/nbia-api/oauth/token")
    headers = dict(
        Authorization = f'Bearer {access_token}'
    )
    url = f'https://services.cancerimagingarchive.net/nbia-api/services/{endpoint}'
    result = requests.get(url, headers=headers, data=data)
    # collections = [collection['criteria'] for collection in result.json()]
    return result






if __name__ == "__main__":
    if not settings.configured:
        from python_settings import settings
        import settings as etl_settings

        settings.configure(etl_settings)
        assert settings.configured

    get_TCIA_instances_per_series("temp", '1.2.840.113654.2.55.262421043240525317038356381369289737801', server="NLST")
    # results = get_collection_values_and_counts()
    # results = v2_api('getCollectionValuesAndCounts', data="")
    # results = v2_api('getSimpleSearchCriteriaValues', data="")
    # results = get_collection_values_and_counts()
    # results = get_collection_values_and_counts_dev()
    results = get_patients_per_collection_dev('NLST')
    results = get_collections_dev()
    # hash = get_hash({"SeriesInstanceUID":'1.3.6.1.4.1.14519.5.2.1.1706.6003.183542674700655712034736428353'})
    # result = get_images_with_md5_hash('1.3.6.1.4.1.14519.5.2.1.1706.6003.183542674700655712034736428353')
    # with open('/home/bcliffor/temp/1.3.6.1.4.1.14519.5.2.1.1706.6003.183542674700655712034736428353.zip', 'wb') as f:
    #     f.write(result.content)

    # series = get_updated_series('20/02/2021')
    # hash = get_hash({"Collection":'TCGA-ESCA'})
    # instances = get_TCIA_instances_per_series('/mnt/disks/idc-etl/temp', '1.2.840.113713.4.2.165042455211102753703326913551133262099', nbia_server=True)
    # print(instances)
    # patients = get_TCIA_patients_per_collection('LDCT-and-Projection-data')
    get_collection_descriptions()
    # series = get_TCIA_series_per_collection('TCGA-BRCA')
    # series = get_updated_series('23/03/2021')
    # print(time.asctime());studies = get_TCIA_studies_per_collection('BREAST-DIAGNOSIS', nbia_server=False);print(time.asctime())
    # studies = get_TCIA_studies_per_patient(collection.tcia_api_collection_id, patient.submitter_case_id)
    # patients=get_TCIA_patients_per_collection('CBIS-DDSM')
    #
    # # collection = get_collection_values_and_counts()
    # nbia_collections = [c['Collection'] for c in get_collections(nbia_server=True)]
    # nbia_collections.sort()
    # nbia_collections = [c['Collection'] for c in get_collections(nbia_server=True)]
    # nbia_collections.sort()
    # tcia_collections = [c['Collection'] for c in get_collections(nbia_server=False)]
    # tcia_collections.sort()
    # pass
    # for collection in collections:
    #     patients = get_TCIA_patients_per_collection(collection['Collection'])
    #     for patient in patients:
    #         studies = get_TCIA_studies_per_patient(collection['Collection'], patient['PatientId'])
    #         for study in studies:
    #             seriess = get_TCIA_series_per_study(collection['Collection'], patient['PatientId'], study['StudyInstanceUID'])
    #             for series in seriess:
    #                 instanceUIDs = get_TCIA_instance_uids_per_series(series['SeriesInstanceUID'])
    #                 for instanceUID in instanceUIDs:
    #                     instance = get_TCIA_instance(series['SeriesInstanceUID'], instanceUID['SOPInstanceUID'])


