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

# Validate a that a DICOMstore has expected instances in some version
# We only validate that the DICOMstore has instances with expected SOPInstanceUIDs
# We don't validate instance hashes

import sys
import os
import argparse
import time
from datetime import timedelta
import json
from logging import INFO
from multiprocessing import Process, Queue
from utilities.tcia_helpers import  get_TCIA_patients_per_collection, \
    get_collection_values_and_counts, get_TCIA_studies_per_collection, get_TCIA_series_per_collection
import logging
from python_settings import settings
import settings as etl_settings
import google.auth
from google.auth.transport import requests
from google.oauth2 import service_account


# settings.configure(etl_settings)
# assert settings.configured
import psycopg2
from psycopg2.extras import DictCursor

_BASE_URL = "https://healthcare.googleapis.com/v1"


def get_session():
    """Creates an authorized Requests Session."""
    # credentials = service_account.Credentials.from_service_account_file(
    #     filename=os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
    #     scopes=["https://www.googleapis.com/auth/cloud-platform"],
    # )

    # Create a requests Session object with the credentials.
    credentials, project = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    session = requests.AuthorizedSession(credentials)
    return session


PATIENT_TRIES=3

def dicomweb_search_instances(
    base_url, project_id, cloud_region, dataset_id, dicom_store_id, series
):
    """Handles the GET requests specified in DICOMweb standard."""
    url = "{}/projects/{}/locations/{}".format(base_url, project_id, cloud_region)

    dicomweb_path = "{}/datasets/{}/dicomStores/{}/dicomWeb/instances".format(
        url, dataset_id, dicom_store_id
    )
    # dicomweb_path = "{}/datasets/{}/dicomStores/{}/dicomWeb/studies".format(
    #     url, dataset_id, dicom_store_id
    # )
    # params = {"StudyInstanceUID" : '1.3.6.1.4.1.14519.5.2.1.2452.1800.117324429538754496356479065003'}
    # params = {"PatientName" : 'LUNG1-001'}

    # Make an authenticated API request
    session = get_session()

    headers = {"Content-Type": "application/dicom+json; charset=utf-8"}

    offset = 0
    limit = 1000
    all_instances = []
    while True:
        params = {"SeriesInstanceUID": series,
                  "offset": offset,
                  "limit": limit
                }

        response = session.get(dicomweb_path, headers=headers, params=params)
        # response = session.get(dicomweb_path, headers=headers)
        # response = session.get(dicomweb_path, params=params)
        if response.status_code == 204 and response.reason == 'No Content':
            break

        response.raise_for_status()

        instances = [i['00080018']['Value'][0] for i in response.json()]
        all_instances.extend(instances)
        offset += len(instances)

    return set(all_instances)

def validate_series(cur, validated, args, version, collection, patient, study, series, index):
    result = 0
    begin = time.time()
    query = (f"""
     SELECT DISTINCT sop_instance_uid
     FROM {args.all_table} as at
     WHERE at.series_instance_uid = '{series}'
     """)
    cur.execute(query)
    idc_instance_uids = set([i['sop_instance_uid'] for i in cur.fetchall()])
    rootlogger.info('      p%s: Series %s, %s, %s instances', args.id, series, index, len(idc_instance_uids))

    gch_instance_uids = dicomweb_search_instances(_BASE_URL, args.project, args.cloud_region, args.gchdataset,
                                                  args.gchstore, series)

    if idc_instance_uids != gch_instance_uids:
        errlogger.error('      p%s: Instance mismatch; %s/%s/%s/%s',
                    args.id,
                    collection,
                    patient,
                    study,
                    series)

    duration = str(timedelta(seconds=(time.time() - begin)))
    rootlogger.info('      p%s: Series %s, %s, completed in %s', args.id, series, index, duration)

def validate_study(cur, validated, args, version, collection, patient, study, index):
    result = 0
    begin = time.time()

    query = (f"""
     SELECT DISTINCT series_instance_uid
     FROM {args.all_table} as at
     WHERE at.study_instance_uid = '{study}'
     """)
    cur.execute(query)
    seriess = [series[0] for series in cur.fetchall()]
    rootlogger.info('    p%s: Study %s, %s, %s series', args.id, study, index, len(seriess))

    for series in seriess:
        series_index = f'{seriess.index(series) + 1}|{len(seriess)}'
        validate_series(cur, validated, args, version, collection, patient, study, series, series_index)

    duration = str(timedelta(seconds=(time.time() - begin)))
    rootlogger.info('    p%s: Study %s, %s, completed in %s', args.id, study, index, duration)
    return result

def validate_patient(cur, args, validated, version, collection, patient, index):
    result = 0
    begin = time.time()

    query = (f"""
     SELECT DISTINCT study_instance_uid
     FROM {args.all_table} AS at
     WHERE at.submitter_case_id = '{patient}'
     """)
    cur.execute(query)
    studies = [study[0] for study in cur.fetchall()]

    rootlogger.info('  p%s: Patient %s, %s, %s studies', args.id, patient, index, len(studies))

    for study in studies:
        study_index = f'{studies.index(study) + 1}|{len(studies)}'

        result = validate_study(cur, validated, args, version, collection, patient, study, study_index)

    duration = str(timedelta(seconds=(time.time() - begin)))
    rootlogger.info('  p%s: Patient %s, %s, completed in %s', args.id, patient, index, duration)
    return result


def worker(input, output, conn, args):
    rootlogger.debug('p%s: Worker starting: args: %s', args.id, args)
    conn = psycopg2.connect(dbname=args.db, user=settings.DATABASE_USERNAME,
                            password=settings.DATABASE_PASSWORD, host=settings.DATABASE_HOST)
    with conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:

            for more_args in iter(input.get, 'STOP'):
                result = 0
                with conn.cursor(cursor_factory=DictCursor) as cur:
                    for attempt in range(PATIENT_TRIES):
                        try:
                            validated, version, collection, patient, index = more_args
                            result = validate_patient(cur, args, validated, version, collection, patient,index)
                            break
                        except Exception as exc:
                            errlogger.error("p%s: Worker p%s, exception %s; reattempt %s on %s/%s", args.id, exc, attempt, collection, patient)

                    if attempt == PATIENT_TRIES:
                        errlogger.error("p%s, Failed to process %s/%s", args.id, collection, patient)
                        result = -1

                    donepatientlogger.info('%s%s', '' if result == 0 else '-', patient)
                    output.put(patient)


def validate_collection(cur, validated, args, version, collection, index):
    begin = time.time()
    query = (f"""
     SELECT DISTINCT submitter_case_id
     FROM {args.all_table} as at
     WHERE at.tcia_api_collection_id = '{collection}'
     """)
    cur.execute(query)
    patients = [patient[0] for patient in cur.fetchall()]
    rootlogger.info('p%s: Collection %s, %s, %s, patients' , args.id, collection, index, len(patients))
    done_patients = open(args.done_patients).read().splitlines()

    if args.num_processes == 0:
        args.id=0
        for patient in patients:
            patient_index = f'{patients.index(patient) + 1}|{len(patients)}'
            if not patient in done_patients:
                result = validate_patient(cur, args, validated, version, collection, patient, patient_index)
            else:
                rootlogger.info('  p%s: Patient %s, %s, previously validated', args.id, patient, index)

    else:
        processes = []
        # Create queues
        task_queue = Queue()
        done_queue = Queue()

        # List of patients enqueued
        enqueued_patients = []

        # Start worker processes
        for process in range(args.num_processes):
            args.id = process + 1
            processes.append(
                Process(target=worker,
                        args=(task_queue, done_queue, cur.connection, args)))
            processes[-1].start()

        # Enqueue each patient in the the task queue
        for patient in patients:
            patient_index = f'{patients.index(patient) + 1}|{len(patients)}'
            if not patient in done_patients:
                task_queue.put((validated, version, collection, patient, patient_index))
                enqueued_patients.append(patient)
            else:
                rootlogger.info('  p%s: Patient %s, %s, previously validated', args.id, patient, patient_index)

        # Collect the results for each patient
        while not enqueued_patients == []:
            # Timeout if waiting too long
            results = done_queue.get(True,)
            enqueued_patients.remove(results)

        # Tell child processes to stop
        for process in processes:
            task_queue.put('STOP')

        #Wait for them to stop
        for process in processes:
            process.join()

    duration = str(timedelta(seconds=(time.time() - begin)))
    done_patients = set(open(args.done_patients).read().splitlines())
    if set(patients).issubset(done_patients):
        rootlogger.info('p%s: Collection %s, %s, completed in %s', args.id, collection, index, duration)
        donecollectionlogger.info(collection)
    else:
        errlogger.error('p%s: Collection %s, %s, not completed in %s', args.id, collection, index, duration)



def validate_version(args):
    validated = open(args.done_collections).read().splitlines()
    skips = open(args.skips).read().splitlines()
    conn = psycopg2.connect(dbname=args.db, user=settings.DATABASE_USERNAME,
                            password=settings.DATABASE_PASSWORD, host=settings.DATABASE_HOST)
    with conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            # cur.execute("""
            # # SELECT *
            # # FROM version
            # # WHERE idc_version_number = (%s)""", (args.version,))

            version = args.version
            # cur.execute("""
            # SELECT * FROM collection
            # WHERE version_id = (%s)""", (version['id'],))
            query = (f"""
            SELECT DISTINCT tcia_api_collection_id 
            FROM {args.all_table} as at
            WHERE at.idc_version_number = {args.version}
            """)
            cur.execute(query)
            collections = [collection[0] for collection in cur.fetchall()]
            # if not len(tcia_collections) == len(collections):
            #     errlogger.error('Different number of collections; NBIA: %s, IDC: %s in', len(tcia_collections),
            #                     len(collections),
            #                     version['idc_version_number'])
            for collection in collections:
                # if collection['tcia_api_collection_id'] not in validated and collection['tcia_api_collection_id'] not in skips:
                collection_index = f'{collections.index(collection) + 1} of {len(collections)}'
                if collection not in validated:
                    if collection not in skips:
                        result = validate_collection(cur, validated, args, version, collection, collection_index)
                else:
                    rootlogger.info('p%s: Collection %s %s previously validated', args.id,
                                    collection, collection_index)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=2, help='Version to validate')
    args = parser.parse_args()
    parser.add_argument('--db', default=f'idc', help='Database against which to validate')
    parser.add_argument('--project', default='idc-dev-etl')
    parser.add_argument('--all_table', default=f'all_v{args.version}', help='Denormalization of version, patient,...')
    # parser.add_argument('--gchdataset', default='idc_tcia_mvp_wave1')
    # parser.add_argument('--gchstore', default='idc_tcia')
    parser.add_argument('--gchdataset', default='idc')
    parser.add_argument('--gchstore', default='v2')
    parser.add_argument('--cloud_region', default='us-central1', help='GCH dataset region')
    parser.add_argument('--num_processes', default=8, help="Number of concurrent processes")
    parser.add_argument('--skips', default='./logs/val_gch_dicomstore_skips.log' )
    parser.add_argument('--done_collections', default='./logs/val_gch_dicomstore_dones.log' )
    parser.add_argument('--done_patients', default='./logs/val_gch_dicomstore_patients.log' )
    args = parser.parse_args()

    args.id = 0

    # Directory to which to download files from TCIA/NBIA
    args.dicom = 'dicom'
    print("{}".format(args), file=sys.stdout)

    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/val_gch_dicomstore_log.log'.format(os.environ['PWD']))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    donecollectionlogger = logging.getLogger('done')
    done_fh = logging.FileHandler(args.done_collections)
    doneformatter = logging.Formatter('%(message)s')
    donecollectionlogger.addHandler(done_fh)
    done_fh.setFormatter(doneformatter)
    donecollectionlogger.setLevel(INFO)

    donepatientlogger = logging.getLogger('donepatient')
    donepatient_fh = logging.FileHandler(args.done_patients)
    donepatientformatter = logging.Formatter('%(message)s')
    donepatientlogger.addHandler(donepatient_fh)
    donepatient_fh.setFormatter(donepatientformatter)
    donepatientlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/val_gch_dicomstore_err.log'.format(os.environ['PWD']))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    # gch_instances = dicomweb_search_instances(_BASE_URL, args.project, args.cloud_region, args.gchdataset, args.gchstore, "1.3.6.1.4.1.14519.5.2.1.2452.1800.259219628863720082993747174733")
    validate_version(args)
