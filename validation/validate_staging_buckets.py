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

# For each collection, validate that new/revised instances are in the
# corresponding staging bucket.

TRIES=3

import sys
import os
import argparse
import time
from datetime import datetime, timezone, timedelta
import logging
from logging import INFO
from multiprocessing import Process, Queue
from queue import Empty
from google.cloud import storage

from python_settings import settings
import settings as etl_settings

settings.configure(etl_settings)
assert settings.configured
import psycopg2
from psycopg2.extras import DictCursor

#Get info on each blob in a collection
def get_collection_iterator(storage_client, bucket_name):
    pages = storage_client.list_blobs(bucket_name)
    return pages


def get_gcs_instance_uids(args, tcia_api_collection_id):
    storage_client = storage.Client()
    bucket_name = f"{args.prestaging_bucket_prefix}{tcia_api_collection_id.lower().replace(' ','_').replace('-','_')}"
    pages = storage_client.list_blobs(bucket_name)
    instance_uids = []

    for page in pages.pages:
        instance_uids.extend([blob.name.split('.')[0] for blob in list(page)])
    return instance_uids


def validate_series(cur, args, validations, series_index, gcs_instance_uuids,
                    version, collection, patient, study, series):
    validated = 0
    if not series['series_instance_uid'] in validations:
        begin = time.time()

        cur.execute("""
            SELECT * FROM instance
            WHERE series_id = (%s)""", (series['id'],))
        instances = cur.fetchall()

        rootlogger.info("      p%s: Series %s; %s; %s instances", args.id, series['series_instance_uid'], series_index, len(instances))
        for instance in instances:
            if not instance['instance_uuid'] in gcs_instance_uuids:
                errlogger.error("p%s: Instance %s/%s/%s/%s/%s, uuid: %S, not in staging bucket",
                        args.id, collection['tcia_api_collection_id'], patient['submitter_case_id'],
                        study['study_instance_uid'], series['series_instance_uid'],
                        instance['sop_instance_uid'], instance['instance_uuid'])
                validated = -1

        duration = str(timedelta(seconds=(time.time() - begin)))
        rootlogger.info("      p%s: Series %s, %s, completed in %s", args.id, series['series_instance_uid'], series_index, duration)
    else:
        rootlogger.info("      p%s: Series %s, %s, previously built", args.id, series['series_instance_uid'], series_index)
    donelogger.info('%s%s', '-' if validated else '', series['series_instance_uid'])
    return validated


def validate_study(cur, args, validations, study_index, gcs_instance_uuids,
                   version, collection, patient, study):
    validated = 0
    if not study['study_instance_uid'] in validations:
        begin = time.time()

        cur.execute("""
            SELECT * FROM series
            WHERE study_id = (%s)""", (study['id'],))
        seriess = cur.fetchall()

        rootlogger.info("    p%s: Study %s, %s, %s series", args.id, study['study_instance_uid'], study_index, len(seriess))
        for series in seriess:
            series_index = f'{seriess.index(series)+1} of {len(seriess)}'
            validated = validate_series(cur, args, validations, series_index, gcs_instance_uuids,
                    version, collection, patient, study, series) | validated

        duration = str(timedelta(seconds=(time.time() - begin)))
        rootlogger.info("    p%s: Study %s, %s,  completed in %s", args.id, study['study_instance_uid'], study_index, duration)
    else:
        rootlogger.info("    p%s: Study %s, %s, previously built", args.id, study['study_instance_uid'], study_index)
    donelogger.info('%s%s', '-' if validated else '', study['study_instance_uid'])
    return validated


def validate_patient(cur, args, validations, patient_index, gcs_instance_uuids,
                     version, collection, patient):
    validated = 0
    if not patient['submitter_case_id'] in validations:
        begin = time.time()

        cur.execute("""
            SELECT * FROM study
            WHERE patient_id = (%s)""", (patient['id'],))
        studies = cur.fetchall()

        rootlogger.info("  p%s: Patient %s, %s, %s studies", args.id, patient['submitter_case_id'], patient_index, len(studies))
        for study in studies:
            study_index = f'{studies.index(study)+1} of {len(studies)}'
            validated = validate_study(cur, args, validations, study_index, gcs_instance_uuids,
                           version, collection, patient, study) | validated

        duration = str(timedelta(seconds=(time.time() - begin)))
        rootlogger.info("  p%s: Patient %s, %s, completed in %s", args.id, patient['submitter_case_id'], patient_index, duration)
    else:
        rootlogger.info("  p%s: Patient %s, %s, previously built", args.id, patient['submitter_case_id'], patient_index)

    donelogger.info('%s%s', '-' if validated else '', patient['submitter_case_id'])
    return validated


def worker(input, output, args, validations):
    rootlogger.debug('p%s: Worker starting: args: %s', args.id, args)
    conn = psycopg2.connect(dbname=settings.DATABASE_NAME, user=settings.DATABASE_USERNAME,
                            password=settings.DATABASE_PASSWORD, host=settings.DATABASE_HOST)
    with conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:

            for more_args in iter(input.get, 'STOP'):
                validated = 0
                for attempt in range(TRIES):
                    try:
                        gcs_instance_uuids, patient_index, idc_version_number, tcia_api_collection_id, submitter_case_id = more_args
                        cur.execute("""
                            SELECT * FROM version
                            WHERE idc_version_number = (%s)""", (idc_version_number,))
                        version = cur.fetchone()
                        cur.execute("""
                             SELECT * FROM collection
                             WHERE version_id = (%s) AND tcia_api_collection_id = (%s)""",
                                    (version['id'], tcia_api_collection_id,))
                        collection = cur.fetchone()
                        cur.execute("""
                             SELECT * FROM patient
                             WHERE collection_id = (%s) AND submitter_case_id = (%s)""",
                                    (collection['id'], submitter_case_id,))
                        patient = cur.fetchone()
                        rootlogger.debug("p%s: In worker, submitter_case_id: %s", args.id, submitter_case_id)
                        validated = validate_patient(cur, args, validations, patient_index, gcs_instance_uuids,
                                         version, collection, patient) | validated
                        break
                    except Exception as exc:
                        errlogger.error("Worker p%s, exception %s; reattempt %s on patient %s/%s", args.id, exc, attempt, tcia_api_collection_id, submitter_case_id)


                if attempt == TRIES:
                    errlogger.error("p%s, Failed to process patient: %s", args.id, submitter_case_id)
                    validated = -1

                output.put((submitter_case_id, validated))


def validate_collection(cur, args, dones, validations, collection_index, version, collection):
    if not collection['tcia_api_collection_id'] in dones:
    # if collection['tcia_api_collection_id'] == 'RIDER Breast MRI': # Temporary code for development
        begin = time.time()

        cur.execute("""
            SELECT * FROM patient
            WHERE collection_id = (%s)""", (collection['id'],))
        patients = cur.fetchall()

        rootlogger.info("Collection %s, %s, %s patients", collection['tcia_api_collection_id'], collection_index, len(patients))
        gcs_instance_uuids = get_gcs_instance_uids(args, collection['tcia_api_collection_id'])

        validated = 0

        if args.num_processes==0:
            # for series in sorted_seriess:
            for patient in patients:
                args.id = 0
                patient_index = f'{patients.index(patient)+1} of {len(patients)}'
                validated = validate_patient(cur, args, validations, patient_index, gcs_instance_uuids,
                                 version, collection, patient) | validated

        else:
            processes = []
            # Create queues
            task_queue = Queue()
            done_queue = Queue()

            # List of patients enqueued
            enqueued_patients = []

            # Start worker processes
            for process in range(args.num_processes):
                args.id = process+1
                processes.append(
                    Process(target=worker, args=(task_queue, done_queue, args, validations)))
                processes[-1].start()

            # Enqueue each patient in the the task queue
            for patient in patients:
                patient_index = f'{patients.index(patient)+1} of {len(patients)}'
                task_queue.put((gcs_instance_uuids, patient_index, version['idc_version_number'], collection['tcia_api_collection_id'], patient['submitter_case_id']))
                enqueued_patients.append(patient['submitter_case_id'])

            # Collect the results for each patient
            try:
                while not enqueued_patients == []:
                    # Timeout if waiting too long
                    submitter_case_id, result = done_queue.get(True)
                    validated = validated | result
                    enqueued_patients.remove(submitter_case_id)

                # Tell child processes to stop
                for process in processes:
                    task_queue.put('STOP')

                # Wait for them to stop
                for process in processes:
                    process.join()

                # copy_prestaging_to_staging_bucket(args, collection)
                # ************ Temporary code during development********************
                # duration = str(timedelta(seconds=(time.time() - begin)))
                # rootlogger.info("Collection %s, %s, completed in %s", collection['tcia_api_collection_id'], collection_index, duration)
                # raise
                # ************ End temporary code ********************
                duration = str(timedelta(seconds=(time.time() - begin)))
                rootlogger.info("Collection %s, %s, completed in %s", collection['tcia_api_collection_id'], collection_index,
                                duration)


            except Empty as e:
                errlogger.error("Timeout in validate_collection %s", collection['tcia_api_collection_id'])
                for process in processes:
                    process.terminate()
                    process.join()
                duration = str(timedelta(seconds=(time.time() - begin)))
                rootlogger.info("Collection %s, %s, NOT completed in %s", collection['tcia_api_collection_id'], collection_index,
                                duration)

            with open(args.dones, 'a') as f:
                f.write(f"{'-' if validated else ''}{collection['tcia_api_collection_id']}\n")
            # Truncate the validations file minimize searches for the next collection
            os.truncate(args.validations,0)


    else:
        rootlogger.info("Collection %s, %s, previously built", collection['tcia_api_collection_id'], collection_index)


def validate_version(cur, validations, args, version):
    # Session = sessionmaker(bind= sql_engine)
    # version = version_is_done(sess, args.vnext)
    skips = open(args.skips).read().splitlines()
    try:
        dones = open(args.dones).read().splitlines()
    except:
        dones = []
    begin = time.time()
    cur.execute("""
        SELECT * FROM collection
        WHERE version_id = (%s)""", (version['id'],))
    # cur.execute("""
    #     SELECT * FROM collection
    #     WHERE version_id = (%s) AND tcia_api_collection_id = (%s)""", (version['id'],'RIDER Breast MRI'))
    collections = cur.fetchall()

    rootlogger.info("Version %s; %s collections", version['idc_version_number'], len(collections))
    for collection in collections:
        if not collection['tcia_api_collection_id'] in skips:
            collection_index = f'{collections.index(collection)+1} of {len(collections)}'
            validate_collection(cur, args, dones, validations, collection_index, version, collection)


def prevalidate(args):
    validations = open(args.validations).read().splitlines()
    conn = psycopg2.connect(dbname=settings.DATABASE_NAME, user=settings.DATABASE_USERNAME,
                            password=settings.DATABASE_PASSWORD, host=settings.DATABASE_HOST)
    with conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("""
            SELECT * 
            FROM version
            WHERE idc_version_number = (%s)""", (args.vnext,))

            version = cur.fetchone()
            validate_version(cur, validations, args, version)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--vnext', default=2, help='Next version to generate')
    parser.add_argument('--prestaging_bucket_prefix', default=f'idc_v2_', help='Copy instances here before forwarding to --staging_bucket')
    parser.add_argument('--num_processes', default=8, help="Number of concurrent processes")
    parser.add_argument('--skips', default='./logs/gcs_staging_skips', help="Collections to be skipped")
    parser.add_argument('--dones', default='./logs/gcs_staging_done_collections.txt', help="Completed collections")
    parser.add_argument('--validations', default='{}/logs/gcsvalidationslog.log'.format(os.environ['PWD']), help="Completed patients, studies, series" )
    parser.add_argument('--project', default='idc-dev-etl')
    args = parser.parse_args()

    # Directory to which to download files from TCIA/NBIA
    args.dicom = 'dicom'
    print("{}".format(args), file=sys.stdout)

    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/gcsvallog.log'.format(os.environ['PWD']))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    donelogger = logging.getLogger('done')
    done_fh = logging.FileHandler(args.validations)
    doneformatter = logging.Formatter('%(message)s')
    donelogger.addHandler(done_fh)
    done_fh.setFormatter(doneformatter)
    donelogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/gcsvalerr.log'.format(os.environ['PWD']))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    prevalidate(args)
