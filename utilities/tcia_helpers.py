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
import requests
import backoff
import logging

# TCIA_URL = 'https://services.cancerimagingarchive.net/services/v4/TCIA/query'
TCIA_URL = 'https://services.cancerimagingarchive.net/nbia-api/services/v1'

@backoff.on_exception(backoff.expo,
                      requests.exceptions.RequestException,
                      max_tries=3)
def get_url(url):  # , headers):
    return requests.get(url)  # , headers=headers)


MAX_RETRIES=3

def TCIA_API_request(endpoint, parameters=""):  
    retry = 0
    buffer = BytesIO()
    c = pycurl.Curl()
    url = 'https://services.cancerimagingarchive.net/services/v3/TCIA/query/{}?{}'.format(endpoint,parameters)
    while retry < MAX_RETRIES:
        try:
            c.setopt(c.URL, url)
            c.setopt(c.WRITEDATA,buffer)
            c.perform()
            data = buffer.getvalue().decode('iso-8859-1')
#            print('Raw TCIA data: {}'.format(data),file=sys.stderr)
            results = json.loads(data)
            c.close()
            if retry > 1:
                print("TCIA_API_request successful on retry {}".format(retry))
            return results

        except:
            # print("Error {}; {} in TCIA_API_request".format(e[0],e[1]), file=sys.stderr, flush=True)
            logging.error("Error in TCIA_API_request")
            rand = random.randint(1,10)
            logging.info("Retrying in TCIA_API_request from %s",inspect.stack()[1])
            # print("Retry {}, sleeping {} seconds".format(retry, rand), file=sys.stderr, flush=True)
            logging.info("Retrying in TCIA_API_request from %s",inspect.stack()[1])
            logging.info("Retry %s, sleeping %s seconds", retry, rand)
            time.sleep(rand)
            retry += 1
            
    c.close()
    # print("TCIA_API_request failed in call from {}".format(inspect.stack()[1]), file=sys.stderr, flush=True)
    logging.warning("TCIA_API_request failed in call from %s", inspect.stack()[1])
    raise RuntimeError (inspect.stack()[0:2])


def TCIA_API_request_to_file(filename, endpoint, parameters=""):
    retry = 0
    c = pycurl.Curl()
    url = 'https://services.cancerimagingarchive.net/services/v3/TCIA/query/{}?{}'.format(endpoint,parameters)
    while retry < MAX_RETRIES:
        try:
            with open(filename, 'wb') as f:
                c.setopt(c.URL, url)
                c.setopt(c.WRITEDATA, f)
                c.perform()
                c.close()
            if retry > 1:
                print("TCIA_API_request_to_file successful on retry {}".format(retry))
            return 0

        except:
            # print("Error {}; {} in TCIA_API_request_to_file".format(e[0],e[1]), file=sys.stderr, flush=True)
            logging.info("Error in TCIA_API_request_to_file: %s,%s,%s",sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2])
            rand = random.randint(1,10)
            logging.info("Retrying in TCIA_API_request_to_file from %s",inspect.stack()[1])
            logging.info("Retry %s, sleeping %s seconds", retry, rand)
            time.sleep(rand)
            retry += 1
            
    c.close()
    logging.error("TCIA_API_request_to_file failed in call from %s", inspect.stack()[1])
    # return -1
    raise RuntimeError (inspect.stack()[0:2])


def get_TCIA_collections():
    results = TCIA_API_request('getCollectionValues')
    url = f'{TCIA_URL}/getCollectionValues'
    results = get_url(url)
    collections = results.json()
    # collections = [collection['Collection'].replace(' ', '_') for collection in results.json()]
    return collections


def get_TCIA_patients_per_collection(collection_id):

    url = f'{TCIA_URL}/getPatient?Collection={collection_id}'
    results = get_url(url)
    patients = results.json()
    return patients


def get_TCIA_studies_per_patient(collection, patientID):
    url = f'{TCIA_URL}/getPatientStudy?Collection={collection}&PatientID={patientID}'
    results = get_url(url)
    studies = results.json()
    return studies


def get_TCIA_series_per_study(collection, patientID, studyInstanceUID):
    url = f'{TCIA_URL}/getSeries?Collection ={collection}&PatientID={patientID}&StudyInstanceUID={studyInstanceUID}'
    results = get_url(url)
    series = results.json()
    return series

def get_TCIA_instance_uids_per_series(seriesInstanceUID):
    url = f'{TCIA_URL}/getSOPInstanceUIDs?SeriesInstanceUID={seriesInstanceUID}'
    results = get_url(url)
    instance_uids = results.json()
    return instance_uids

def get_TCIA_instance(seriesInstanceUID, sopInstanceUID):
    url = f'{TCIA_URL}/getSingleImage?SeriesInstanceUID={seriesInstanceUID}&SOPInstanceUID={sopInstanceUID}'
    results = get_url(url)
    instances = results.json()
    return instances

def get_TCIA_series_per_collection(collection):
    results = TCIA_API_request('getSeries')
    SeriesInstanceUIDs = [SeriesInstanceUID['SeriesInstanceUID'] for SeriesInstanceUID in results]
    return SeriesInstanceUIDs

def get_TCIA_series():
    results = TCIA_API_request('getSeries')
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

def get_TCIA_instances_per_series(series_instance_uid):
    # Get a zip of the instances in this series to a file and unzip it
    result = TCIA_API_request_to_file("{}/{}.zip".format("dicom", series_instance_uid),
                "getImage", parameters="SeriesInstanceUID={}".format(series_instance_uid))

    # Now try to extract the instances to a directory DICOM/<series_instance_uid>
    try:
        with zipfile.ZipFile("{}/{}.zip".format("dicom", series_instance_uid)) as zip_ref:
            zip_ref.extractall("{}/{}".format("dicom", series_instance_uid))
        return
    except :
        logging.error("\tZip extract failed for series %s with error %s,%s,%s ", series_instance_uid,
                      sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
        raise


    # try:
    #     with zipfile.ZipFile("{}/{}.zip".format(DICOM, series)) as zip_ref:
    #         zip_ref.extractall("{}/{}".format(DICOM, series))
    #     if retry > 0:
    #         logging.info("\tGot valid zipfile for %s/%s on retry %s", study, series, retry)
    #     validation['downloaded'] = 1
    #     return {'returncode': 0, 'compressed': compressed, 'validation': validation}
    # except zipfile.BadZipFile:
    #     logging.error("\tZip extract failed for %s/%s with error BadZipFile on retry %s", study, series, retry)
    #     retry += 1
    # except zipfile.LargeZipFile:
    #     logging.error("\tZip extract failed for %s/%s with error LargeZipFile on retry %s", study, series, retry)
    #     retry += 1
    # except:
    #     logging.error(
    #         "\tZip extract failed for %s/%s with error %s,%s,%s on retry %s", study, series, sys.exc_info()[0],
    #                                                                               sys.exc_info()[1],
    #                                                                               sys.exc_info()[2], retry)


def create_jsonlines_from_list(original):
    in_json = StringIO(json.dumps(original)) 
    result = [json.dumps(record) for record in json.load(in_json)]
    result = '\n'.join(result)
    return result


def get_collection_size(collection):
    size = 0
    serieses=TCIA_API_request('getSeries', parameters="Collection={}".format(collection.replace(' ','_')))
    print("{} series in {}".format(len(serieses), collection), flush=True)
    for aseries in serieses:
        seriesSize=TCIA_API_request('getSeriesSize', parameters="SeriesInstanceUID={}".format(aseries['SeriesInstanceUID']))
#             print(seriesSize)
        size += int(float(seriesSize[0]['TotalSizeInBytes']))
        print("{} {}\r".format(aseries['SeriesInstanceUID'], size),end="")
    return size


def get_collection_sizes_in_bytes():
    sizes = {}
    collections = get_TCIA_collections()
    collections.sort(reverse=True)
    for collection in collections:
        sizes[collection] = get_collection_size(collection)
    return sizes


def get_collection_sizes():
    collections = get_TCIA_collections()
    counts = {collection:0 for collection in collections}
    serieses=TCIA_API_request('getSeries')
    for aseries in serieses:
        counts[aseries['Collection']] += int(aseries['ImageCount'])
    sorted_counts = [(k, v) for k, v in sorted(counts.items(), key=lambda item: item[1])]
    return sorted_counts


def get_collection_descriptions():
    # Get access token for the guest account
    result = run([
        'curl',
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

if __name__ == "__main__":
    collections = get_TCIA_collections()
    for collection in collections:
        patients = get_TCIA_patients_per_collection(collection)
        for patient in patients:
            studies = get_TCIA_studies_per_patient(collection, patient['PatientID'])
            for study in studies:
                seriess = get_TCIA_series_per_study(study['StudyInstanceUID'])
                for series in seriess:
                    instanceUIDs = get_TCIA_instance_uids_per_series(series['SeriesInstanceUID'])
                    for instanceUID in instanceUIDs:
                        instance = get_TCIA_instance(series['SeriesInstanceUID'], instanceUID['SOPInstanceUID'])


