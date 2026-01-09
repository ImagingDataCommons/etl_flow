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

import os

import json
from subprocess import run, PIPE
from time import sleep
import requests
import logging
import zipfile
import pandas as pd
from tcia_utils import datacite
from utilities.logging_config import errlogger

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


# from python_settings import settings
import settings
import logging
logging.getLogger("requests").setLevel(logging.WARNING)


TIMEOUT=10.0
CHUNK_SIZE=1024*1024

TCIA_URL = 'https://services.cancerimagingarchive.net/services/v4/TCIA/query'
NBIA_URL = 'https://services.cancerimagingarchive.net/nbia-api/services'
NBIA_V1_URL = 'https://services.cancerimagingarchive.net/nbia-api/services/v1'
NBIA_V2_URL = 'https://services.cancerimagingarchive.net/nbia-api/services/v2'
# NBIA_AUTH_URL = "https://public.cancerimagingarchive.net/nbia-api/oauth/token"
NBIA_AUTH_URL = "https://services.cancerimagingarchive.net/nbia-api/oauth/token"
NBIA_DEV_URL = 'https://public-dev.cancerimagingarchive.net/nbia-api/services'
NBIA_DEV_AUTH_URL = "https://public-dev.cancerimagingarchive.net/nbia-api/oauth/token"
NLST_URL = 'https://nlst.cancerimagingarchive.net/nbia-api/services'
NLST_V1_URL = 'https://nlst.cancerimagingarchive.net/nbia-api/services/v1'
NLST_V2_URL = 'https://nlst.cancerimagingarchive.net/nbia-api/services/v2'
NLST_AUTH_URL = 'https://nlst.cancerimagingarchive.net/nbia-api/oauth/token'


# @backoff.on_exception(backoff.expo,
#                       requests.exceptions.RequestException,
#                       max_tries=3)
def get_url(url, headers="", timeout=TIMEOUT):  # , headers):
    try:
        result =  requests.get(url, headers=headers, timeout=timeout)  # , headers=headers)
    except Exception as exc:
        logging.error(f'In get_url: {exc}', exc_info=True)
        raise
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
    return (result.json()['access_token'], result.json()['refresh_token'])



def get_tcia_instance_hash(sop_instance_uid, access_token=None):
    # if not access_token:
    #     access_token, refresh_token = get_access_token(NBIA_AUTH_URL)
    headers = dict(
        Authorization=f'Bearer {access_token}'
    )
    url = f"{NBIA_V2_URL}/getM5HashForImage?SOPInstanceUid={sop_instance_uid}"
    result = requests.get(url, headers=headers)
    return result


def get_hash_nlst(request_data, access_token=''):
    access_token, refresh_token = get_access_token(NLST_AUTH_URL)
    retries = 4
    while retries:
        headers = dict(
            Authorization=f'Bearer {access_token}'
        )
        url = f"{NLST_URL}/getMD5Hierarchy"
        result = requests.post(url, headers=headers, data=request_data)
        if result.status_code == 200:
            break
        else:
            sleep( 2**(5-retries))
            retries -= 1
    return result


def get_hash(request_data, access_token=None):
    if not access_token:
        access_token, refresh_token = get_access_token(NBIA_AUTH_URL)
    retries = 4
    while retries:
        headers = dict(
            Authorization=f'Bearer {access_token}'
        )
        url = f"{NBIA_URL}/getMD5Hierarchy"
        result = requests.post(url, headers=headers, data=request_data)
        if result.status_code == 200:
            break
        else:
            sleep( 2**(5-retries))
            retries -= 1
    return result


def get_images_with_md5_hash_nlst(SeriesInstanceUID, access_token=None):
    headers = dict(
        Authorization=f'Bearer {access_token}'
    )
    server_url = NLST_V1_URL
    url = f'{server_url}/getImageWithMD5Hash?SeriesInstanceUID={SeriesInstanceUID}'
    result = requests.get(url, headers=headers)
    return result


def get_images_with_md5_hash(SeriesInstanceUID, access_token=None):
    server_url = NBIA_V1_URL
    url = f'{server_url}/getImageWithMD5Hash?SeriesInstanceUID={SeriesInstanceUID}'
    result = requests.get(url)
    return result


# Get a list of the (public) collections that TCIA knows about. and count of the subjects in each
def get_collection_values_and_counts(server=NBIA_URL):
    if server == "NLST":
        server_url = NLST_URL
        access_token, refresh_token = get_access_token(NLST_AUTH_URL)
    elif server == "NBIA":
        server_url = NBIA_URL
        access_token, refresh_token = get_access_token()
    else:
        server_url = server
        access_token, refresh_token = get_access_token()
    headers = dict(
        Authorization = f'Bearer {access_token}'
    )
    url = f'{server_url}/getCollectionValuesAndCounts'
    result = requests.get(url, headers=headers)
    collections = [collection['criteria'] for collection in result.json()]
    return collections


def get_TCIA_patients_per_collection(collection_id, server=NBIA_V1_URL):
    if collection_id == "NLST":
        server_url = NLST_V2_URL
        access_token, refresh_token = get_access_token(NLST_AUTH_URL)
        headers = dict(
            Authorization=f'Bearer {access_token}'
        )
    elif server == "NBIA":
        server_url = NBIA_V1_URL
        headers = ''
    else:
        server_url = server
        headers = ''
    url = f'{server_url}/getPatient?Collection={collection_id}'
    results = get_url(url, headers)
    patients = results.json() if results.content else []
    return patients


def get_TCIA_studies_per_patient(collection_id, patientID, server=NBIA_V1_URL):
    if collection_id == "NLST":
        server_url = NLST_V2_URL
        access_token, refresh_token = get_access_token(NLST_AUTH_URL)
        headers = dict(
            Authorization=f'Bearer {access_token}'
        )
    elif server == "NBIA":
        server_url = NBIA_V1_URL
        headers = ''
    else:
        server_url = server
        headers = ''
    url = f'{server_url}/getPatientStudy?Collection={collection_id}&PatientID={patientID}'
    results = get_url(url, headers)
    studies = results.json() if results.content else []
    return studies


def get_TCIA_series_per_study(collection_id, patientID, studyInstanceUID, server=NBIA_V1_URL):
    if collection_id == "NLST":
        server_url = NLST_V2_URL
        access_token, refresh_token = get_access_token(NLST_AUTH_URL)
        headers = dict(
            Authorization=f'Bearer {access_token}'
        )
    elif server == "NBIA":
        server_url = NBIA_V1_URL
        headers = ''
    else:
        server_url = server
        headers = ''
    url = f'{server_url}/getSeries?Collection ={collection_id}&PatientID={patientID}&StudyInstanceUID={studyInstanceUID}'
    results = get_url(url, headers)
    series = results.json() if results.content else []
    return series


def get_TCIA_series_metadata(seriesInstanceUID, server=NBIA_V1_URL):
    server_url = server
    headers = ''
    url = f'{server_url}/getSeriesMetaData?SeriesInstanceUID={seriesInstanceUID}'
    results = get_url(url, headers)
    series = results.json() if results.content else {}
    return series[0]


def get_TCIA_instance_uids_per_series(collection_id, seriesInstanceUID, server=NBIA_V1_URL):
    if collection_id == "NLST":
        server_url = NLST_V2_URL
        access_token, refresh_token = get_access_token(NLST_AUTH_URL)
        headers = dict(
            Authorization=f'Bearer {access_token}'
        )
    elif server == "NBIA":
        server_url = NBIA_V1_URL
        headers = ''
    else:
        server_url = server
        headers = ''
    url = f'{server_url}/getSOPInstanceUIDs?SeriesInstanceUID={seriesInstanceUID}'
    results = get_url(url, headers)
    instance_uids = results.json() if results.content else []
    return instance_uids


def get_TCIA_instances_per_series_with_hashes_nlst(dicom, series, access_token ):
    filename = "{}/{}.zip".format(dicom, series.uuid)
    dirname = "{}/{}".format(dicom, series.uuid)

    headers = headers = dict(
        Authorization=f'Bearer {access_token}'
    )

    url = f'{NLST_V1_URL}/getImageWithMD5Hash?SeriesInstanceUID={series.series_instance_uid}'
    with requests.get(url, headers=headers, stream=True) as r:
        r.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk:
                f.write(chunk)

    # Now try to extract the instances to a directory DICOM/<series_instance_uid>
    os.makedirs(f"{dirname}", exist_ok=True )
    with zipfile.ZipFile(filename, "r") as zip_ref:
        zip_ref.extractall(f'{dirname}')

    hashes = open(f'{dirname}/md5hashes.csv').read().splitlines()[1:]
    os.remove(f'{dirname}/md5hashes.csv')

    return hashes




def get_TCIA_instances_per_series_with_hashes(dicom, series):
    filename = "{}/{}.zip".format(dicom, series.uuid)
    dirname = "{}/{}".format(dicom, series.uuid)

    url = f'{NBIA_V1_URL}/getImageWithMD5Hash?SeriesInstanceUID={series.series_instance_uid}'
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk:
                f.write(chunk)

    # Now try to extract the instances to a directory DICOM/<series_instance_uid>
    os.makedirs(f"{dirname}", exist_ok=True)
    with zipfile.ZipFile(filename, "r") as zip_ref:
        zip_ref.extractall(f'{dirname}')

    hashes = open(f'{dirname}/md5hashes.csv').read().splitlines()[1:]
    os.remove(f'{dirname}/md5hashes.csv')

    return hashes


# Get NBIAs internal ID for all the series in a collection/patient
def get_internal_series_ids(collection, patient, third_party="yes", size=100000, server="" ):
    if server == "NLST":
        server_url = NLST_URL
        access_token, refresh_token = get_access_token(NLST_AUTH_URL)
    else:
        server_url = NBIA_URL
        access_token, refresh_token = get_access_token(NBIA_AUTH_URL)
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
            sortDirection="ascending",
            start=0,
            size=size)
    else:
        data = dict(
            criteriaType0="ThirdPartyAnalysis",
            value0=third_party,
            criteriaType1="CollectionCriteria",
            value1=collection,
            sortField="subject",
            sortDirection="ascending",
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
        access_token, refresh_token = get_access_token(NLST_AUTH_URL)
    else:
        server_url = NBIA_URL
        access_token, refresh_token = get_access_token(NBIA_AUTH_URL)
    # url = f'{server_url}/getStudyDrillDown'
    url = f'{server_url}/getStudyDrillDownWithSeriesIds'
    data = "&".join(['list={}'.format(id) for id in series_ids])

    try:
        result = run(['curl', '-v', '-H', "Authorization:Bearer {}".format(access_token), '-k',  url, '-d', data],
                     stdout=PIPE, stderr=PIPE)
    except:
        pass
    return json.loads(result.stdout.decode())


def get_collection_descriptions_and_licenses(collection=None):
    if collection == 'NLST':
        access_token, refresh_token = get_access_token(NLST_AUTH_URL)
        headers = dict(
            Authorization=f'Bearer {access_token}'
        )
        url = f'https://nlst.cancerimagingarchive.net/nbia-api/services/getCollectionDescriptions?collectionName=NLST'
    else:

        access_token, refresh_token = get_access_token()
        if collection:
            url = f'https://public.cancerimagingarchive.net/nbia-api/services/getCollectionDescriptions?collectionName={collection}'
        else:
            url = 'https://public.cancerimagingarchive.net/nbia-api/services/getCollectionDescriptions'
        headers = dict(
            Authorization=f'Bearer {access_token}'
        )

    result = requests.get(
        url,
        headers=headers
    )
    descriptions = result.json()

    collection_descriptions = {description['collectionName']: description for description in descriptions}

    if not collection:
        # Now, if we are getting descriptions of all collections, get the NLST description
        nlst_description = get_collection_descriptions_and_licenses('NLST')
        collection_descriptions['NLST'] = nlst_description['NLST']

    return collection_descriptions


def get_license_info():
    access_token, refresh_token = get_access_token()
    headers = dict(
        Authorization=f'Bearer {access_token}'
    )
    result = requests.get(
        url='https://public.cancerimagingarchive.net/nbia-api/services/getLicenses',
        headers=headers
    )
    licenses = {license['longName']: license for license in result.json()}

    return licenses


def get_collection_license_info():
    table = get_collection_descriptions_and_licenses()
    license_info = {license['id']: license for license in get_license_info()}
    for license in license_info:
        if license_info[license]['licenseURL'].split(':')[0] == 'http':
            license_info[license]['licenseURL'] = f'https:{license_info[license]["licenseURL"].split(":",1)[1]}'
    licenses = {}
    for collection_id, data in table.items():
        # print(collection_id, data['licenseId'])
        try:
            licenseId = data['licenseId']
        except Exception as exc:
            print(exc)
        if licenseId:
            licenses[collection_id] = dict(
                licenseURL = license_info[licenseId]["licenseURL"],
                longName = license_info[licenseId]["longName"],
                shortName = license_info[licenseId]["shortName"]
            )
        elif collection_id == 'Pediatric-CT-SEG':
                licenses[collection_id] = dict(
                    licenseURL="https://creativecommons.org/licenses/by-nc/4.0/",
                    longName="Creative Commons Attribution-NonCommercial 4.0 International License",
                    shortName="CC BY-NC 4.0"
                )
        else:
            licenses[collection_id] = dict(
                licenseURL = "None",
                longName = "None",
                shortName = "None"
            )

    return licenses


def get_all_tcia_metadata(type, query_param=''):
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

def xget_TCIA_instances_per_series_with_hashes_nlst(dicom, series_instance_uid, access_token, uuid=None ):
    filename = "{}/{}.zip".format(dicom, series_instance_uid)
    dirname = "{}/{}".format(dicom, series_instance_uid)

    headers = headers = dict(
        Authorization=f'Bearer {access_token}'
    )

    url = f'{NLST_V2_URL}/getImageWithMD5Hash?SeriesInstanceUID={series_instance_uid}'
    with requests.get(url, headers=headers, stream=True) as r:
        r.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk:
                f.write(chunk)

    # Now try to extract the instances to a directory DICOM/<series_instance_uid>
    os.makedirs(f"{dirname}", exist_ok=True )
    with zipfile.ZipFile(filename, "r") as zip_ref:
        zip_ref.extractall(f'{dirname}')

    hashes = open(f'{dirname}/md5hashes.csv').read().splitlines()[1:]
    # os.remove(f'{dirname}/md5hashes.csv')

    return hashes
# if __name__ == "__main__":
#
#     access_token = get_access_token(auth_server=NLST_AUTH_URL)[0]
#     for patientID in ('126153', '215303'):
#         print("Patient: ", patientID)
#         studies = get_TCIA_studies_per_patient('NLST', patientID)
#         for study in studies:
#             # print("  ", "Study: ", study)
#             print("  ", "Study: ", study['StudyInstanceUID'])
#
#             seriess = get_TCIA_series_per_study('NLST', patientID, study['StudyInstanceUID'])
#             for series in seriess:
#                 # print("      ", "Series: ", series)
#                 print("      ", "Series: ", series['SeriesInstanceUID'])
#
#                 instances=get_TCIA_instance_uids_per_series('NLST', series['SeriesInstanceUID'])
#                 # print("        ", "Instances: ", instances)
#                 os.makedirs(f'/mnt/disks/idc-etl/tmp/{patientID}/{study["StudyInstanceUID"]}', exist_ok=True)
#                 hashes = xget_TCIA_instances_per_series_with_hashes_nlst(f'/mnt/disks/idc-etl/tmp/{patientID}/{study["StudyInstanceUID"]}', series['SeriesInstanceUID'], access_token)
#                 # print("        ", "Hashes: ", hashes)
#                 print("        ", "Instances: ", len(instances))
#                 count = len(os.listdir(f'/mnt/disks/idc-etl/tmp/{patientID}/{study["StudyInstanceUID"]}/{series["SeriesInstanceUID"]}'))
#                 if len(instances) != count -1:
#                     pass
#
#                 pass
#     pass

