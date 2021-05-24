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

# One time use routine to "rename" improperly named v1 blobs.

import argparse
import os
from subprocess import run, PIPE
import logging
from logging import INFO
import time
from datetime import timedelta
from multiprocessing import Process, Queue
from queue import Empty
from google.cloud import storage


from python_settings import settings
import settings as etl_settings

settings.configure(etl_settings)
assert settings.configured
import psycopg2
from psycopg2.extras import DictCursor

TRIES=3

def val_collection(cur, args, dones, collection_index, tcia_api_collection_id):
    if not tcia_api_collection_id in dones:

        client = storage.Client()
        bucket = client.bucket(args.final_bucket)
        n = 1

        try:
            done_instances = open(f'./logs/vfb__v1_{tcia_api_collection_id}_success.log').read().splitlines()
        except:
            done_instances = []

        increment = 10000
        query= f"""
        SELECT * 
        FROM all_v2_20210514_1526
        WHERE tcia_api_collection_id = '{tcia_api_collection_id}'
        order by sop_instance_uid
        """
        cur.execute(query)
        rowcount=cur.rowcount
        successes = open(f'./logs/vfb__v1_{tcia_api_collection_id}_success.log', 'a')
        failures = open(f'./logs/vfb_v1_{tcia_api_collection_id}_failures.log', 'a')
        failure_count=0
        while True:
            rows = cur.fetchmany(increment)
            if len(rows) == 0:
                break
            for row in rows:
                # if '-' in row['gcs_url']:
                #     if len(row['gcs_url'].rsplit('/',1)[-1].rsplit('-')[0]) != 8:
                # blob_name = f'{row["gcs_url"].rsplit("/")[-1]}.dcm'
                # if storage.Blob(bucket=bucket, name=blob_name).exists(client):
                #     rootlogger.info('%s: %s original blob exists', n, blob_name)
                # else:
                #     errlogger.error('%s: %s original blob not found', n, blob_name)
                index = f'{n}/{rowcount}'
                blob_name = f'{row["gcs_url"].rsplit("/")[-1]}.dcm'
                blob_rename = f"{row['instance_uuid']}.dcm"
                if not blob_rename in done_instances:
                    if storage.Blob(bucket=bucket, name=blob_rename).exists(client):
                        rootlogger.info('%s %s: %s: renamed blob exists %s', args.id, index, tcia_api_collection_id, blob_rename)
                        successes.write(f'{blob_rename}\n')
                    elif blob_name == blob_rename:
                        errlogger.error('%s %s: %s: name == rename but blob does not exist %s', args.id, index,
                                        tcia_api_collection_id, blob_name)
                        failures.write(f'{blob_rename}\n')
                        failure_count += 1
                    elif blob_name[-28:] != blob_rename[-28:]:
                        errlogger.error('%s %s: %s: name and rename not similar %s %s', args.id, index,
                                        tcia_api_collection_id, blob_name, blob_rename)
                        failures.write(f'{blob_rename}\n')
                        failure_count += 1
                    else:
                        # Didn't find renamed blob, so copy original blob to renamed blob
                        blob_name = f'{row["gcs_url"].rsplit("/")[-1]}.dcm'
                        if storage.Blob(bucket=bucket, name=blob_name).exists(client):
                            # Copy original blob to renamed blob
                            try:
                                blob_copy = bucket.copy_blob(bucket.blob(blob_name), bucket, blob_rename)
                                successes.write(f'{blob_rename}\n')
                                rootlogger.info('%s %s: %s: copied original blob to renamed blob %s %s', args.id, index, tcia_api_collection_id, blob_name, blob_rename)
                            except:
                                errlogger.error('%s %s: %s: copy failed original blob to renamed blob %s %s', args.id, index, tcia_api_collection_id, blob_name, blob_rename)
                                failures.write(f'{blob_rename}\n')
                                failure_count += 1

                        else:
                            errlogger.error('%s %s: %s: original and renamed blob not found % %', args.id, index, tcia_api_collection_id, blob_name, blob_rename)
                            failures.write(f'{blob_rename}\n')
                            failure_count += 1
                else:
                        rootlogger.info('%s %s: %s: skipping renamed blob %s ', args.id, index, tcia_api_collection_id, blob_rename)
                n += 1

        if failure_count == 0:
            # with open(args.dones, 'a') as f:
            #     f.write(f"{tcia_api_collection_id}\n")
            donelogger.info('%s', tcia_api_collection_id)
            rootlogger.info('%s: Completed collection %s ', args.id, tcia_api_collection_id)
        else:
            errlogger.error('%s: Failed collection %s; %s failures ', args.id, tcia_api_collection_id, failure_count)

    else:
        rootlogger.info("p%s: Collection %s, %s, previously built", args.id, tcia_api_collection_id, collection_index)


def worker(input, output, args, dones):
    rootlogger.debug('p%s: Worker starting: args: %s', args.id, args)
    conn = psycopg2.connect(dbname=settings.DATABASE_NAME, user=settings.DATABASE_USERNAME,
                            password=settings.DATABASE_PASSWORD, host=settings.DATABASE_HOST)
    with conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:

            for more_args in iter(input.get, 'STOP'):
                validated = 0
                for attempt in range(TRIES):
                    try:
                        collection_index, tcia_api_collection_id = more_args
                        # copy_collection(args, dones, collection_index, tcia_api_collection_id)
                        val_collection(cur, args, dones, collection_index, tcia_api_collection_id)
                        break
                    except Exception as exc:
                        errlogger.error("p%s, exception %s; reattempt %s on collection %s", args.id, exc, attempt, tcia_api_collection_id)


                if attempt == TRIES:
                    errlogger.error("p%s, Failed to process collection: %s", args.id, tcia_api_collection_id)

                output.put((tcia_api_collection_id))

def copy_collections(cur, args, version):
    try:
        todos = open(args.todos).read().splitlines()
    except:
        errlogger.error('No todos file')
        raise

    try:
        dones = open(args.dones).read().splitlines()
    except:
        dones = []
    begin = time.time()
    # cur.execute("""
    #     SELECT * FROM all_v2_20210514_1526""")
    cur.execute("""
        SELECT * FROM collection
        WHERE idc_version_number = 1""")
    collections = cur.fetchall()

    rootlogger.info("Version %s; %s collections", version['idc_version_number'], len(collections))
    if args.processes == 0:
        args.id=0
        for collection in collections:
            if collection['tcia_api_collection_id'] in todos:
                collection_index = f'{collections.index(collection)+1} of {len(collections)}'
                val_collection(cur, args, dones, collection_index,  collection['tcia_api_collection_id'])

    else:
        processes = []
        # Create queues
        task_queue = Queue()
        done_queue = Queue()

        # List of patients enqueued
        enqueued_collections = []

        # Start worker processes
        for process in range(args.processes):
            args.id = process + 1
            processes.append(
                Process(target=worker, args=(task_queue, done_queue, args, dones)))
            processes[-1].start()

        # Enqueue each patient in the the task queue
        for collection in collections:
            if collection['tcia_api_collection_id'] in todos:
                collection_index = f'{collections.index(collection) + 1} of {len(collections)}'
                task_queue.put((collection_index, collection['tcia_api_collection_id']))
                enqueued_collections.append(collection['tcia_api_collection_id'])

        # Collect the results for each patient
        try:
            while not enqueued_collections == []:
                # Timeout if waiting too long
                tcia_api_collection_id = done_queue.get(True)
                enqueued_collections.remove(tcia_api_collection_id)

            # Tell child processes to stop
            for process in processes:
                task_queue.put('STOP')

            # Wait for them to stop
            for process in processes:
                process.join()

            duration = str(timedelta(seconds=(time.time() - begin)))
            rootlogger.info("Collection %s, %s, completed in %s", collection['tcia_api_collection_id'], collection_index,
                            duration)


        except Empty as e:
            errlogger.error("Exception copy_collections ")
            for process in processes:
                process.terminate()
                process.join()
            duration = str(timedelta(seconds=(time.time() - begin)))
            rootlogger.info("Collection copying NOT completed")


def preval(args):
    conn = psycopg2.connect(dbname=settings.DATABASE_NAME, user=settings.DATABASE_USERNAME,
                            password=settings.DATABASE_PASSWORD, host=settings.DATABASE_HOST)
    with conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("""
            SELECT * 
            FROM version
            WHERE idc_version_number = (%s)""", (args.version,))

            version = cur.fetchone()
            copy_collections(cur, args, version)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=2, help='Next version to generate')
    parser.add_argument('--final_bucket', default='idc_dev', help='Bucket to validate')
    parser.add_argument('--processes', default=8, help="Number of concurrent processes")
    parser.add_argument('--project', default='idc-dev-etl')
    parser.add_argument('--todos', default='./logs/validate_v1_final_bucket_todos.log' )
    parser.add_argument('--dones', default='./logs/validate_v1_final_bucket_dones.log' )
    args = parser.parse_args()

    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/validate_v1_final_bucket_log.log'.format(os.environ['PWD']))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    donelogger = logging.getLogger('done')
    done_fh = logging.FileHandler(args.dones)
    doneformatter = logging.Formatter('%(message)s')
    donelogger.addHandler(done_fh)
    done_fh.setFormatter(doneformatter)
    donelogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/validate_v1_final_bucket_err.log'.format(os.environ['PWD']))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    preval(args)
