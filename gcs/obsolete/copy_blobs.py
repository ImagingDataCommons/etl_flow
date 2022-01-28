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

# Copy all blobs named in the DB to some bucket.
# This is specifically to copy blobs from the dev bucket
# to the open bucket.
# Since we multiprocess by collection, this depends on the
# a table that is the join of the version, collection,..., instance tables.

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

        # src_client = storage.Client(project=args.src_project)
        # dst_client = storage.Client(project=args.dst_project)
        # src_bucket = src_client.bucket(args.src_bucket)
        # dst_bucket = dst_client.bucket(args.dst_bucket, user_project=args.dst_project)
        client = storage.Client()
        src_bucket = client.bucket(args.src_bucket, user_project=args.src_project)
        dst_bucket = client.bucket(args.dst_bucket, user_project=args.dst_project)
        n = 1

        try:
            done_instances = set(open(f'./logs/cb_{tcia_api_collection_id}_success.log').read().splitlines())
        except:
            done_instances = []

        increment = 5000
        query= f"""
        SELECT * 
        FROM {args.all_table}
        WHERE tcia_api_collection_id = '{tcia_api_collection_id}'
        order by sop_instance_uid
        """
        cur.execute(query)
        rowcount=cur.rowcount
        successes = open(f'./logs/cb_{tcia_api_collection_id}_success.log', 'a')
        failures = open(f'./logs/cb_{tcia_api_collection_id}_failures.log', 'a')
        failure_count=0
        while True:
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
                            blob_copy = src_bucket.copy_blob(src_bucket.blob(blob_name), dst_bucket)
                            rootlogger.info('%s %s: %s: copy succeeded %s', args.id, index, tcia_api_collection_id, blob_name)
                            successes.write(f'{blob_name}\n')
                            break
                        except Exception as exc:
                            errlogger.error('%s %s: %s: copy failed %s\n, retry %s; %s', args.id,
                                            index, tcia_api_collection_id,
                                            blob_name, retries, exc)
                            if retries == TRIES:
                                failures.write(f'{blob_name}; {exc}\n')
                                failure_count += 1
                                break
                        time.sleep(retries)
                        retries += 1
                else:
                    if n % 10000 == 0:
                        rootlogger.info('%s %s: %s: skipping blob %s ', args.id, index, tcia_api_collection_id, blob_name)
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
    # Session = sessionmaker(bind= sql_engine)
    # version = version_is_done(sess, args.version)
    try:
        skips = open(args.skips).read().splitlines()
    except:
        skips = []
    try:
        dones = open(args.dones).read().splitlines()
    except:
        dones = []
    begin = time.time()
    cur.execute("""
        SELECT * FROM collection
        WHERE version_id = (%s)""", (version['id'],))
    collections = cur.fetchall()

    rootlogger.info("Version %s; %s collections", version['idc_version_number'], len(collections))
    if args.processes == 0:
        args.id=0
        for collection in collections:
            if not collection['tcia_api_collection_id'] in skips:
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
            if not collection['tcia_api_collection_id'] in skips:
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
            errlogger.error("Exception copy_collections__obsolete ")
            for process in processes:
                process.terminate()
                process.join()
            duration = str(timedelta(seconds=(time.time() - begin)))
            rootlogger.info("Collection copying NOT completed")


def precopy(args):
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
    parser.add_argument('--src_bucket', default='idc_dev', help='Bucket to validate')
    parser.add_argument('--dst_bucket', default='idc-open', help='Bucket to validate')
    parser.add_argument('--all_table', default='all_v2')
    parser.add_argument('--processes', default=16, help="Number of concurrent processes")
    parser.add_argument('--src_project', default='idc-dev-etl')
    parser.add_argument('--dst_project', default='canceridc-data')
    parser.add_argument('--skips', default='./logs/copy_blobs_skips.log' )
    parser.add_argument('--dones', default='./logs/copy_blobs__dones.log' )
    args = parser.parse_args()

    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/copy_blobs_log.log'.format(os.environ['PWD']))
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
    err_fh = logging.FileHandler('{}/logs/copy_blobs_err.log'.format(os.environ['PWD']))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    precopy(args)
