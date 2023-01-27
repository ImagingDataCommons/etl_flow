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

# One-use script to delete a version from the hierarchy. We originally had a top level version table
# with one row per IDC version. Each row was one-to-many with collections in that version, etc.
# Going forward, we will have a database (e.g. idc_v3) per IDC version. Thus the version table is no
# longer needed. The initial DB for idc_v3 is the final DB for V2, but we must first delete the V1
# subtree from the V2 DB (which has bit V1 and V2). Then we can eliminate the version table.
# For some reason, PSQL seems to take forever to do a drop from version where id=1

import sys
import os
import argparse
from google.cloud import storage
from logging import INFO
from multiprocessing import Process, Queue
from utilities.tcia_helpers import  get_TCIA_patients_per_collection, \
    get_collection_values_and_counts, get_TCIA_studies_per_collection, get_TCIA_series_per_collection
import logging
from python_settings import settings
import settings as etl_settings

settings.configure(etl_settings)
assert settings.configured
import psycopg2
from psycopg2.extras import DictCursor

PATIENT_TRIES=3

def validate_series(cur, validated, args, version, collection, patient, studies):
    result = 0
    rootlogger.info('p%s: Validating series in collection %s', args.id, collection['tcia_api_collection_id'])
    results = get_TCIA_series_per_collection(collection['tcia_api_collection_id'])
    tcia_seriess = {series['SeriesInstanceUID']:series['ImageCount'] for series in results}
    study_ids = [study['id'] for study in studies]
    cur.execute("""
    SELECT * FROM series
    WHERE study_id = ANY(%s)""", (study_ids,))
    seriess = cur.fetchall()

    if not len(tcia_seriess) == len(seriess):
        errlogger.error('p%s: Different number of series; NBIA: %s, IDC: %s in %s/%s', args.id, len(tcia_seriess), len(seriess),
                        version['idc_version_number'], collection['tcia_api_collection_id'])
        result = -1
        return result
    for series in seriess:
        if not series['series_instance_uid'] in tcia_seriess:
            errlogger.error(('p%s: Series %s not in collection %s', args.id, series['series_instance_uid'], collection['tcia_api_collection_id']))
            result = -1
            return result

        cur.execute("""
        SELECT * FROM instance
        WHERE series_id = (%s)""", (series['id'],))
        instances = cur.fetchall()

        if not tcia_seriess[series['series_instance_uid']] == len(instances):
            errlogger.error('p%s: Different number of instance; NBIA: %s, IDC: %s in %s/%s',
                            args.id, tcia_seriess[series['series_instance_uid']], len(instances),
                            version['idc_version_number'], collection['tcia_api_collection_id'])
            result = -1
            return result
    return result

def validate_studies(cur, args, validated, version, collection, patients):
    result = 0
    rootlogger.info('p%s: Validating studies in collection %s', args.id, collection['tcia_api_collection_id'])
    results = get_TCIA_studies_per_collection(collection['tcia_api_collection_id'], nbia_server=True)
    tcia_studies = set([study['StudyInstanceUID'] for study in results])

    patient_ids = [patient["id"] for patient in patients]
    cur.execute("""
    SELECT * FROM study
    WHERE patient_id = ANY(%s)""", (patient_ids,))
    studies = cur.fetchall()

    if not len(tcia_studies) == len(studies):
        errlogger.error('p%s: Different number of studies; NBIA: %s, IDC: %s in %s/%s', args.id, len(tcia_studies), len(studies),
                        version['idc_version_number'], collection['tcia_api_collection_id'])
        result = -1
        return result

    for study in studies:
        if not study['study_instance_uid'] in tcia_studies:
            errlogger.error(('p%s: Study %s not in collection %s', args.id, study['study_instance_uid'], collection['tcia_api_collection_id']))
            result = -1
            return result

    result = validate_series(cur, validated, args, version, collection, patients, studies) | result
    return result


def validate_patients(cur, validated, args, version, collection):
    result = 0
    rootlogger.info('p%s: Validating patients in collection %s', args.id, collection['tcia_api_collection_id'])
    results = get_TCIA_patients_per_collection(collection['tcia_api_collection_id'])
    tcia_patients = set([patient['PatientId'] for patient in results])

    cur.execute("""
    SELECT * FROM patient
    WHERE collection_id = (%s)""", (collection['id'],))
    patients = cur.fetchall()

    if not len(tcia_patients) == len(patients):
        errlogger.error('p%s: Different number of patients; NBIA: %s, IDC: %s in %s/%s', args.id, len(tcia_patients), len(patients),
                        version['idc_version_number'], collection['tcia_api_collection_id'])
        result = -1
        return result
    for patient in patients:
        if not patient['submitter_case_id'] in tcia_patients:
            errlogger.error('p%s: Patient %s not in collection %s', patient['submitter_case_id'], args.id, collection['tcia_api_collection_id'])
            result = -1
            return result

    result = validate_studies(cur, args, validated, version, collection, patients) | result
    return result


def worker(input, output, conn, args):
    rootlogger.debug('p%s: Worker starting: args: %s', args.id, args)
    conn = psycopg2.connect(dbname=settings.DATABASE_NAME, user=settings.DATABASE_USERNAME,
                            password=settings.DATABASE_PASSWORD, host=settings.DATABASE_HOST)
    with conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:

            for more_args in iter(input.get, 'STOP'):
                result = 0
                with conn.cursor(cursor_factory=DictCursor) as cur:
                    for attempt in range(PATIENT_TRIES):
                        try:
                            validated, idc_version_number, tcia_api_collection_id = more_args
                            cur.execute("""
                                SELECT * FROM version
                                WHERE idc_version_number = (%s)""", (idc_version_number,))
                            version = cur.fetchone()
                            cur.execute("""
                                SELECT * FROM collection
                                WHERE version_id = (%s) AND tcia_api_collection_id = (%s)""", (version['id'], tcia_api_collection_id,))
                            collection = cur.fetchone()

                            rootlogger.info('p%s: Validating collection %s', args.id, tcia_api_collection_id)
                            result = validate_patients(cur, validated, args, version, collection)
                            break
                        except Exception as exc:
                            errlogger.error("p%s: Worker p%s, exception %s; reattempt %s on collection %s", args.id, exc, attempt, tcia_api_collection_id)
                            conn.rollback()

                    if attempt == PATIENT_TRIES:
                        errlogger.error("p%s, Failed to process collection: %s", args.id, tcia_api_collection_id)
                        conn.rollback()
                        result = -1

                    donelogger.info('%s%s', '' if result == 0 else '-', tcia_api_collection_id)
                    output.put(tcia_api_collection_id)


def validate_collections(conn, cur, validated, args, version):
    tcia_collections = get_collection_values_and_counts()
    cur.execute("""
    SELECT * FROM collection
    WHERE version_id = (%s)""", (version['id'],))
    collections = cur.fetchall()
    if not len(tcia_collections) == len(collections):
        errlogger.error('Different number of collections; NBIA: %s, IDC: %s in', len(tcia_collections),
                        len(collections),
                        version['idc_version_number'])
    if args.num_processes == 0:
        for tcia_collection in tcia_collections:
            if not tcia_collection['tcia_api_collection_id'] in validated:
                rootlogger.info('Validating collection %s', tcia_collection)
                collection = next(
                    collection for collection in collections if collection['tcia_api_collection_id'] == tcia_collection)
                result = validate_patients(cur, validated, args, version, collection)
                donelogger.info('%s%s', '' if result == 0 else '-', tcia_collection)
            else:
                rootlogger.info('Collection %s previously done', tcia_collection)

    else:
        processes = []
        # Create queues
        task_queue = Queue()
        done_queue = Queue()

        # List of patients enqueued
        enqueued_collections = []

        # Start worker processes
        for process in range(args.num_processes):
            args.id = process + 1
            processes.append(
                Process(target=worker,
                        args=(task_queue, done_queue, conn, args)))
            processes[-1].start()

        # Enqueue each patient in the the task queue
        for collection in collections:
            if not collection['tcia_api_collection_id'] in validated:

                # patient_index = f'{collection.patients.index(patient) + 1} of {len(collection.patients)}'
                task_queue.put((validated, version['idc_version_number'], collection['tcia_api_collection_id']))
                enqueued_collections.append(collection['tcia_api_collection_id'])
            else:
                rootlogger.info('Collection %s previously done', collection)

        # Collect the results for each patient
        while not enqueued_collections == []:
            # Timeout if waiting too long
            results = done_queue.get(True,)
            enqueued_collections.remove(results)

        # Tell child processes to stop
        for process in processes:
            task_queue.put('STOP')

        #Wait for them to stop
        for process in processes:
            process.join()


def validate_version(args):
    validated = open(args.validated).read().splitlines()
    conn = psycopg2.connect(dbname=settings.DATABASE_NAME, user=settings.DATABASE_USERNAME,
                            password=settings.DATABASE_PASSWORD, host=settings.DATABASE_HOST)
    with conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("""
            SELECT * 
            FROM version
            WHERE idc_version_number = (%s)""", (args.vnext,))

            version = cur.fetchone()
            validate_collections(conn, cur, validated, args, version)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--vnext', default=2, help='Next version to generate')
    parser.add_argument('--num_processes', default=8, help="Number of concurrent processes")
    parser.add_argument('--validated', default='{}/logs/valdone.log'.format(os.environ['PWD']))
    parser.add_argument('--project', default='idc-dev-etl')
    args = parser.parse_args()

    # Directory to which to download files from TCIA/NBIA
    args.dicom = 'dicom'
    print("{}".format(args), file=sys.stdout)

    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/vallog.log'.format(os.environ['PWD']))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    donelogger = logging.getLogger('done')
    done_fh = logging.FileHandler(args.validated)
    doneformatter = logging.Formatter('%(message)s')
    donelogger.addHandler(done_fh)
    done_fh.setFormatter(doneformatter)
    donelogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/valerr.log'.format(os.environ['PWD']))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    validate_version(args)
