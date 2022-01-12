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
import sys
from subprocess import run, PIPE
from time import sleep
import requests
import logging
import zipfile

from http.client import HTTPConnection
HTTPConnection.debuglevel = 0

# rootlogger = logging.getLogger('root')
# errlogger = logging.getLogger('root.err')

from python_settings import settings


TIMEOUT=60
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
    result =  requests.get(url, headers=headers, timeout=timeout)  # , headers=headers)
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


def refresh_access_token(refresh_token, auth_server = NBIA_AUTH_URL):
    data = dict(
        refresh_token=refresh_token,
        client_id=settings.TCIA_CLIENT_ID,
        client_secret=settings.TCIA_CLIENT_SECRET,
        grant_type="refresh_token")

    result = requests.post(auth_server, data = data)
    access_token = result.json()['access_token']
    return (access_token, refresh_token)

def get_instance_hash_nlst(sop_instance_uid, access_token=None):
    # if not access_token:
    #     access_token, refresh_token = get_access_token(NBIA_AUTH_URL)
    headers = dict(
        Authorization=f'Bearer {access_token}'
    )
    url = f"{NLST_V1_URL}/getM5HashForImage?SOPInstanceUid={sop_instance_uid}"
    result = requests.get(url, headers=headers)
    return result

def get_instance_hash(sop_instance_uid, access_token=None):
    # if not access_token:
    #     access_token, refresh_token = get_access_token(NBIA_AUTH_URL)
    headers = dict(
        Authorization=f'Bearer {access_token}'
    )
    url = f"{NBIA_V2_URL}/getM5HashForImage?SOPInstanceUid={sop_instance_uid}"
    result = requests.get(url, headers=headers)
    return result

def get_hash_nlst(request_data, access_token=None):
    # if not access_token:
    #     access_token, refresh_token = get_access_token(NBIA_AUTH_URL)
    headers = dict(
        Authorization=f'Bearer {access_token}'
    )
    url = f"{NLST_URL}/getMD5Hierarchy"
    result = requests.post(url, headers=headers, data=request_data)
    return result

def get_hash(request_data, access_token=None):
    # if not access_token:
    #     access_token, refresh_token = get_access_token(NBIA_AUTH_URL)
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
    if server == "NLST":
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


def get_TCIA_studies_per_patient(collection, patientID, server=NBIA_V1_URL):
    if server == "NLST":
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
    url = f'{server_url}/getPatientStudy?Collection={collection}&PatientID={patientID}'
    results = get_url(url, headers)
    studies = results.json() if results.content else []
    return studies


def get_TCIA_studies_per_collection(collection, server=NBIA_V1_URL):
    # server_url = NBIA_V1_URL if nbia_server else TCIA_URL
    # url = f'{server_url}/getPatientStudy?Collection={collection}'
    # results = get_url(url)
    # studies = results.json()
    # return studies
    if server == "NLST":
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
    url = f'{server_url}/getPatientStudy?Collection={collection}'
    results = get_url(url)
    studies = results.json()
    return studies


def get_TCIA_series_per_study(collection, patientID, studyInstanceUID, server=NBIA_V1_URL):
    if server == "NLST":
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
    url = f'{server_url}/getSeries?Collection ={collection}&PatientID={patientID}&StudyInstanceUID={studyInstanceUID}'
    results = get_url(url, headers)
    series = results.json() if results.content else []
    return series

def get_TCIA_instance_uids_per_series(seriesInstanceUID, server=NBIA_V1_URL):
    if server == "NLST":
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


def get_TCIA_single_instance(seriesInstanceUID, sopInstanceUID, server=NBIA_V1_URL):
    if server == "NLST":
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
    url = f'{server_url}/getSingleImage?SeriesInstanceUID={seriesInstanceUID}&SOPInstanceUID={sopInstanceUID}'
    results = get_url(url, headers)
    return results


def get_TCIA_instances_per_series_with_hashes(dicom, series_instance_uid):
    filename = "{}/{}.zip".format(dicom, series_instance_uid)
    dirname = "{}/{}".format(dicom, series_instance_uid)

    url = f'{NBIA_V1_URL}/getImageWithMD5Hash?SeriesInstanceUID={series_instance_uid}'
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk:
                f.write(chunk)

    # Now try to extract the instances to a directory DICOM/<series_instance_uid>
    os.mkdir(f"{dirname}")
    with zipfile.ZipFile(filename, "r") as zip_ref:
        zip_ref.extractall(f'{dirname}')

    hashes = open(f'{dirname}/md5hashes.csv').read().splitlines()[1:]
    os.remove(f'{dirname}/md5hashes.csv')

    return hashes

def get_TCIA_instances_per_series(dicom, series_instance_uid, server=NBIA_V1_URL):
    filename = "{}/{}.zip".format(dicom, series_instance_uid)
    if server == "NLST":
        server_url = NLST_V2_URL
        access_token, refresh_token = get_access_token(NLST_AUTH_URL)
        url = f'{server_url}/getImage?SeriesInstanceUID={series_instance_uid}'
        headers = f'Authorization:Bearer {access_token}'
        result = run(
            [
                'curl',
                '-o',
                filename,
                '-H',
                headers,
                '-k',
                url
            ],
            stdout=PIPE,
            stderr=PIPE
        )
        # s = f'curl -o {filename} -H {headers} -k {url}'
        pass

    else:
        if server == "":
            server = NBIA_V1_URL
        server_url = server
        url = f'{server_url}/getImage?SeriesInstanceUID={series_instance_uid}'
        result = run(
            [
                'curl',
                '-o',
                filename,
                url
            ],
            stdout=PIPE,
            stderr=PIPE
        )

    # Now try to extract the instances to a directory DICOM/<series_instance_uid>
    try:
         result = run(
             [
                'unzip',
                "{}/{}.zip".format(dicom, series_instance_uid),
                '-d',
                "{}/{}".format(dicom, series_instance_uid)
             ],
             stdout=PIPE,
             stderr=PIPE
         )
         return
    except:
        errlogger.error("\tZip extract failed for series %s with error %s,%s,%s ", series_instance_uid,
                      sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        raise


def get_TCIA_series_per_collection(collection, nbia_server=True):
    server_url = NBIA_V1_URL if nbia_server else TCIA_URL
    url = f'{server_url}/getSeries?Collection={collection}'
    results = get_url(url)
    series = results.json()
    return series


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
        access_token, refresh_token = get_access_token(NLST_AUTH_URL)
    else:
        server_url = NBIA_URL
        access_token, refresh_token = get_access_token(NBIA_AUTH_URL)
    url = f'{server_url}/getStudyDrillDown'
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
    # collection_descriptions = {description['collectionName']: {'description': description['description'], 'licenseId':description['licenseId']} for description in descriptions}
    collection_descriptions = {description['collectionName']: description for description in descriptions}
    # Now, if we are getting descriptions of all collections, get the NLST description
    if not collection:
        nlst_description = get_collection_descriptions_and_licenses('NLST')
        collection_descriptions['NLST'] = nlst_description['NLST']

    return collection_descriptions


def get_collection_licenses():
    access_token, refresh_token = get_access_token()
    headers = dict(
        Authorization=f'Bearer {access_token}'
    )
    result = requests.get(
        url='https://public.cancerimagingarchive.net/nbia-api/services/getLicenses',
        headers=headers
    )
    licenses = result.json()

    return licenses


def get_collection_license_info():
    table = get_collection_descriptions_and_licenses()
    license_info = {license['id']: license for license in get_collection_licenses()}
    licenses = {}
    for collection_id, data in table.items():
        print(collection_id, data['licenseId'])
        licenseId = data['licenseId']
        if licenseId:
            licenses[collection_id] = dict(
                licenseURL = license_info[licenseId]["licenseURL"],
                longName = license_info[licenseId]["longName"],
                shortName = license_info[licenseId]["shortName"]
            )
        else:
            licenses[collection_id] = dict(
                licenseURL = "None",
                longName = "None",
                shortName = "None"
            )

    return licenses


def get_updated_series(date):
    # access_token, refresh_token = get_access_token()
    # headers = dict(
    #     Authorization = f'Bearer {access_token}'
    # )
    # url = f'https://services.cancerimagingarchive.net/nbia-api/services/v2/getUpdatedSeries?fromDate={date}'
    url = f'https://services.cancerimagingarchive.net/nbia-api/services/v1/getUpdatedSeries?fromDate={date}'
    # result = requests.get(url, headers=headers)
    result = requests.get(url)
    if result.status_code == 500 and result.text == 'No data found.':
        series = []
    else:
        series = result.json()
    return series


if __name__ == "__main__":
    if not settings.configured:
        from python_settings import settings
        import settings as etl_settings

        settings.configure(etl_settings)
        assert settings.configured


    # es = get_TCIA_instances_per_series_with_hashes('./temp', '1.3.6.1.4.1.14519.5.2.1.2452.1800.989133494427522093545007937296')

    # p = get_TCIA_patients_per_collection('Training-Pseudo')
    # p = get_TCIA_patients_per_collection('NLST', server="NLST")
    # token= get_access_token()[0]
    # hash = get_hash({'Collection':'LDCT-and-Projection-data'}, token)
    # hash=get_instance_hash('1.3.6.1.4.1.14519.5.2.1.1239.1759.816520824397947445116098544516', access_token=token)
    # token= get_access_token(auth_server=NLST_AUTH_URL)[0]
    # hash=get_instance_hash_nlst('1.2.840.113654.2.55.41416806874377832798981746093165948327', access_token=token)
    #
    # hash = get_hash_nlst({'SeriesInstanceUID':f'1.2.840.113654.2.55.97114726565566537928831413367474015470'}, token)
    # hash = get_images_with_md5_hash_nlst('1.2.840.113654.2.55.97114726565566537928831413367474015470', token)
    # d = get_collection_descriptions_and_licenses()
    # c = get_collection_values_and_counts()
    s = get_updated_series('13/01/2021')
    # studies = get_TCIA_studies_per_patient("PROSTATE-DIAGNOSIS", "ProstateDx-01-0035", server=NBIA_V1_URL)
    # result = get_access_token()
    # access_token, refresh_token = get_access_token()
    # access_token, refresh_token = refresh_access_token(refresh_token)
    # result=get_TCIA_instances_per_series('.', '1.3.6.1.4.1.14519.5.2.1.7009.9004.180224303090109944523368212991', 'NLST')
    # table = get_collection_license_info()
    results = get_collection_values_and_counts()
    # result = get_TCIA_series_per_study('TCGA-GBM', 'TCGA-02-0006', '1.3.6.1.4.1.14519.5.2.1.1706.4001.149500105036523046215258942545' )
    # result = get_TCIA_patients_per_collection('APOLLO-5-LSCC', server=NBIA_V1_URL)
    collections = [key for key in table.keys()]


