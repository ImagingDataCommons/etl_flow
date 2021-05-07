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

# One time use script to convert UUID based blob names of V2 instances, actually
# copy blobs giving corrected names, from "hex" format (32 packed hex characters)
# to standard 8-4-4-4-12 format.
# This script does not change the format of the UUIDs in the instance,
# series and study tables, nor does it delete the incorrectly names blobs. That
# is done separately

import sys
import os
import argparse
from google.cloud import storage,bigquery
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


def get_instance_uuids(args, collection):
    client = bigquery.Client()
    query = f"""
        SELECT distinct instance_uuid
        FROM {args.project}.{args.bqdataset}.{'version'} as v
        JOIN {args.project}.{args.bqdataset}.{'collection'} as c
        ON v.id = c.version_id
        JOIN {args.project}.{args.bqdataset}.{'patient'} as p
        ON c.id = p.collection_id
        JOIN {args.project}.{args.bqdataset}.{'study'} as st 
        ON p.id = st.patient_id
        JOIN {args.project}.{args.bqdataset}.{'series'} as se
        ON st.id = se.study_id
        JOIN {args.project}.{args.bqdataset}.{'instance'} as i
        ON se.id = i.series_id
        WHERE tcia_api_collection_id = '{collection}'"""

    query_job = client.query(query)

    instance_uuids = [row['instance_uuid'] for row in
                query_job]
    return instance_uuids


def rename_instances(args, collection):
    instance_uuids = get_instance_uuids(args, collection)
    client = storage.Client()
    collection_id = collection.replace(' ','_')
    try:
        dones = open('{}/logs/rename_v2_blobs_{}_dones.log'.format(os.environ["PWD"], collection_id)).read().splitlines()
        dones.sort()
    except:
        dones = []
    with open('{}/logs/rename_v2_blobs_{}_dones.log'.format(os.environ["PWD"], collection_id), 'a') as done_file:

        bucket = client.bucket(args.bucket)

        blobs = len(instance_uuids)

        n = 0
        for instance_uuid in instance_uuids:
            incorrect_blob = f'{instance_uuid}.dcm'
            correct_blob = f'{incorrect_blob[0:8]}-{incorrect_blob[8:12]}-{incorrect_blob[12:16]}-{incorrect_blob[16:20]}-{incorrect_blob[20:]}'
            if not instance_uuid in dones:
                rootlogger.info('%s:        Renaming %s blob %s/%s: %s to %s', args.id, collection, n, blobs, incorrect_blob, correct_blob)
                blob_copy = bucket.copy_blob(bucket.blob(incorrect_blob), bucket, correct_blob)
                done_file.write(f'{instance_uuid}\n')
            else:
                rootlogger.info('%s:        Skipping %s blob %s/%s: %s to %s', args.id, collection, n, blobs, incorrect_blob, correct_blob)

            n += 1


def worker(input, output, args):
    rootlogger.debug('p%s: Worker starting: args: %s', args.id, args)
    for more_args in iter(input.get, 'STOP'):
        for attempt in range(PATIENT_TRIES):
            try:
                collection = more_args
                rootlogger.info('p%s: Validating collection %s', args.id, collection)
                result = rename_instances(args, collection)
                break
            except Exception as exc:
                errlogger.error("Worker p%s, exception %s; reattempt %s on collection %s", args.id, exc,
                                attempt, collection)

        if attempt == PATIENT_TRIES:
            errlogger.error("p%s, Failed to process collection: %s", args.id, collection)
            result = -1

        donelogger.info('%s', collection)
        rootlogger.info('p%s: Renamed collection %s', args.id, collection)
        output.put(collection)

def get_collections(args):
    client = bigquery.Client()
    query = f"""
        SELECT distinct tcia_api_collection_id
        FROM {args.project}.{args.bqdataset}.{'collection'}"""

    query_job = client.query(query)

    collections = [row['tcia_api_collection_id'] for row in
                query_job]
    return collections


def validate_collections(validated, args):
    # cur.execute("""
    # SELECT * FROM collection
    # WHERE version_id = (%s)""", (version['id'],))
    # collections = cur.fetchall()
    collections = get_collections(args)
    if args.num_processes == 0:
        args.id = 0
        for collection in collections:
            if not collection in validated:
                # rootlogger.info('Validating collection %s', collection)
                # result = rename_instances(args, collection)
                # donelogger.info('%s', collection)

                for attempt in range(PATIENT_TRIES):
                    try:
                        # collection = more_args
                        rootlogger.info('p%s: Validating collection %s', args.id, collection)
                        result = rename_instances(args, collection)
                        break
                    except Exception as exc:
                        errlogger.error("Worker p%s, exception %s; reattempt %s on collection %s", args.id, exc,
                                        attempt, collection)

                if attempt == PATIENT_TRIES:
                    errlogger.error("p%s, Failed to process collection: %s", args.id, collection)
                    result = -1

                donelogger.info('%s', collection)
                rootlogger.info('p%s: Renamed collection %s', args.id, collection)

            else:
                rootlogger.info('Collection %s previously done', collection)

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
                        args=(task_queue, done_queue, args)))
            processes[-1].start()

        # Enqueue each patient in the the task queue
        for collection in collections:
            if not collection in validated:

                # patient_index = f'{collection.patients.index(patient) + 1} of {len(collection.patients)}'
                task_queue.put((collection))
                enqueued_collections.append(collection)
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


def rename_blobs(args):
    validated = open(args.validated).read().splitlines()
    validate_collections(validated, args)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', default='idc-dev-etl')
    parser.add_argument('--version', default=2)
    parser.add_argument('--bqdataset', default='idc_v2')
    parser.add_argument('--table', default='auxiliary_metadata')
    parser.add_argument('--bucket', default='idc_dev')
    parser.add_argument('--num_processes', default=32, help="Number of concurrent processes")
    parser.add_argument('--skips', default='{}/logs/rename_v2_blobs_skips.log'.format(os.environ['PWD']))
    parser.add_argument('--validated', default='{}/logs/rename_v2_blobs_dones.log'.format(os.environ['PWD']))
    args = parser.parse_args()

    # Directory to which to download files from TCIA/NBIA
    args.dicom = 'dicom'
    print("{}".format(args), file=sys.stdout)

    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/rename_v2_blobs_log.log'.format(os.environ['PWD']))
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
    err_fh = logging.FileHandler('{}/logs/rename_v2_blobs_err.log'.format(os.environ['PWD']))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    rename_blobs(args)
