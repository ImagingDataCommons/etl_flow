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

# Test that there is a blob for each gcs_url.

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
from base64 import b64decode


from python_settings import settings
import settings as etl_settings

settings.configure(etl_settings)
assert settings.configured
import psycopg2
from psycopg2.extras import DictCursor

TRIES=3

def val_collection(cur, args, dones, collection_index, tcia_api_collection_id):
    if not tcia_api_collection_id in dones:
        rootlogger.info('%s: Validating collection %s ', args.id, tcia_api_collection_id)

        client = storage.Client()
        bucket = client.bucket(args.final_bucket)
        n = 1

        # Get the set of instances that have already been validated
        try:
            done_instances = set(open(f'{args.log_dir}/{tcia_api_collection_id}_success.log').read().splitlines())
        except:
            done_instances = []

        increment = 10000
        # query= f"""
        # SELECT *
        # FROM collection as c
        # JOIN patient as p
        # ON c.id = p.collection_id
        # JOIN study as st
        # ON p.id = st.patient_id
        # JOIN series as se
        # ON st.id = se.study_id
        # JOIN instance as i
        # ON se.id = i.series_id
        # WHERE c.idc_version_number = {args.version} and c.tcia_api_collection_id = '{tcia_api_collection_id}'
        # order by sop_instance_uid
        # """

        # Get all the instances that are from the all_table table
        query= f"""
        SELECT instance_uuid, instance_hash
        FROM {args.all_table}
        WHERE collection_id = '{tcia_api_collection_id}'
        order by sop_instance_uid
        """
        cur.execute(query)
        rowcount=cur.rowcount
        # Record successes here
        successes = open(f'{args.log_dir}/{tcia_api_collection_id}_success.log', 'a')
        # Record failures here
        failures = open(f'{args.log_dir}/{tcia_api_collection_id}_failures.log', 'a')
        failure_count=0
        while True:
            # get a batch of instances to validate
            rows = cur.fetchmany(increment)
            if len(rows) == 0:
                break
            for row in rows:
                index = f'{n}/{rowcount}'
                blob_name = f'{row["instance_uuid"]}.dcm'
                if not blob_name in done_instances:
                    retries = 0
                    while True:
                        try:
                            blob = storage.Blob(bucket=bucket, name=blob_name)
                            if blob.exists(client):
                                rootlogger.info('%s %s: %s: blob exists %s', args.id, index, tcia_api_collection_id, blob_name)
                                successes.write(f'{blob_name}\n')
                                break
                                # blob.reload()
                                # if row["instance_hash"] == b64decode(blob.md5_hash).hex():
                                #     rootlogger.info('%s %s: %s: blob exists %s', args.id, index, tcia_api_collection_id, blob_name)
                                #     successes.write(f'{blob_name}\n')
                                #     break
                                # else:
                                #     errlogger.error('%s %s: %s: blob hash mismatch %', args.id, index,
                                #                     tcia_api_collection_id, blob_name)
                                #     failures.write(f'{blob_name} hash mismatch\n')
                                #     failure_count += 1
                            else:
                                errlogger.error('%s %s: %s: blob not found %', args.id, index, tcia_api_collection_id, blob_name)
                                failures.write(f'{blob_name} not found\n')
                                break
                        except Exception as exc:
                            errlogger.error('%s %s: %s: error checking if blob %s exists %s\n, retry %s; %s', args.id,
                                            index, tcia_api_collection_id,
                                            blob_name, retries, exc)
                            if retries == TRIES:
                                failures.write(f'{blob_name}; {exc}\n')
                                failure_count += 1
                                break
                        retries += 1
                else:
                    if n % 10000 == 0:
                        rootlogger.info('%s %s: %s: skipping blobs %s-%s', args.id, index, tcia_api_collection_id, n-9999, n)
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
    conn = psycopg2.connect(dbname=args.db, user=settings.CLOUD_USERNAME, port=settings.CLOUD_PORT,
                            password=settings.CLOUD_PASSWORD, host=settings.CLOUD_HOST)
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

def val_collections(cur, args, version):
    # Session = sessionmaker(bind= sql_engine)
    # version = version_is_done(sess, args.version)
    try:
        todos = open(args.todos).read().splitlines()
    except:
        todos = []
    try:
        dones = open(args.dones).read().splitlines()
    except:
        dones = []
    begin = time.time()
        # cur.execute("""
        #     SELECT * FROM collection
        #     WHERE version_id = (%s)""", (version['id'],))
        # collections = cur.fetchall()
    collections = todos

    rootlogger.info("Version %s; %s collections", version, len(collections))
    if args.processes == 0:
        args.id=0
        for collection in collections:
            # if collection['tcia_api_collection_id'] in todos:
            collection_index = f'{collections.index(collection)+1} of {len(collections)}'
            val_collection(cur, args, dones, collection_index,  collection)

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

        # Enqueue each collection
        for collection in collections:
            collection_index = f'{collections.index(collection) + 1} of {len(collections)}'
            if not collection in dones:
                task_queue.put((collection_index, collection))
                enqueued_collections.append(collection)
            else:
                rootlogger.info("Collection %s, %s, previously completed", collection,
                                collection_index)

        # Collect the results for each collection
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
            rootlogger.info("Collection %s, %s, completed in %s", collection, collection_index,
                            duration)


        except Empty as e:
            errlogger.error("Exception copy_collections ")
            for process in processes:
                process.terminate()
                process.join()
            duration = str(timedelta(seconds=(time.time() - begin)))
            rootlogger.info("Collection copying NOT completed")


def prereval(args):
    conn = psycopg2.connect(dbname=args.db, user=settings.CLOUD_USERNAME, port = settings.CLOUD_PORT,
                            password=settings.CLOUD_PASSWORD, host=settings.CLOUD_HOST)
    with conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            # cur.execute("""
            # SELECT *
            # FROM version
            # WHERE idc_version_number = (%s)""", (args.version,))
            #
            # version = cur.fetchone()
            version = args.version
            val_collections(cur, args, version)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=7, help='Next version to generate')
    parser.add_argument('--final_bucket', default='idc-dev-open', help='Bucket to validate')
    args = parser.parse_args()
    parser.add_argument('--all_table', default=f'all_open', )
    parser.add_argument('--db', default=f'idc_v7', help='PSQL database to access')
    parser.add_argument('--processes', default=16, help="Number of concurrent processes")
    parser.add_argument('--project', default='idc-dev-etl')
    parser.add_argument('--todos', default='./logs/revalidate_final_bucket_todos.log' )
    parser.add_argument('--dones', default='./logs/revalidate_final_bucket_dones.log' )
    parser.add_argument('--log_dir', default=f'/mnt/disks/idc-etl/logs/validate_open_collections')
    args = parser.parse_args()

    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler(f'{args.log_dir}/log.log')
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
    err_fh = logging.FileHandler(f'{args.log_dir}/err.log')
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    prereval(args)
