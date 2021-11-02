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

"""
Copy all included collections (not in the excluded_collections table) to another bucket.
This is generally used to populate a bucket that can then be imported
into a DICOM store.
"""

import argparse
import os
import queue
import time
from subprocess import run, PIPE
import logging
rootlogger = logging.getLogger('root')
successlogger = logging.getLogger('success')
errlogger = logging.getLogger('root.err')

from logging import INFO
import time
from datetime import timedelta
from multiprocessing import Process, Queue
from queue import Empty
from google.cloud import storage, bigquery


from python_settings import settings
import settings as etl_settings

settings.configure(etl_settings)
assert settings.configured
import psycopg2
from psycopg2.extras import DictCursor

TRIES=3

# Get all collections in some version that are not excluded
def get_collections_in_version(args):
    client = bigquery.Client()
    query = f"""
    SELECT c.* 
    FROM `{args.src_project}.{args.bqdataset_name}.{args.bq_collection_table}` as c
    LEFT JOIN `{args.src_project}.{args.bqdataset_name}.{args.bq_excluded_collections}` as ex
    ON LOWER(c.collection_id) = LOWER(ex.tcia_api_collection_id)
    WHERE ex.tcia_api_collection_id is NULL
    ORDER BY c.collection_id
    """
    result = client.query(query).result()
    collection_ids = [collection['collection_id'] for collection in result]
    return collection_ids


def copy_instances(args, rows, n, rowcount, done_instances, src_bucket, dst_bucket):
    for row in rows:
        index = f'{n}/{rowcount}'
        blob_name = f'{row["uuid"]}.dcm'
        if not blob_name in done_instances:
            retries = 0
            while True:
                try:
                    blob_copy = src_bucket.copy_blob(src_bucket.blob(blob_name), dst_bucket)
                    # rootlogger.info('%s %s: %s: copy succeeded %s', args.id, index, args.collection, blob_name)
                    successlogger.info(f'{blob_name}')
                    break
                except Exception as exc:
                    if retries == TRIES:
                        errlogger.error('p%s %s: %s: copy failed %s\n, retry %s; %s', args.id,
                                    index, args.collection,
                                    blob_name, retries, exc)
                        break
                time.sleep(retries)
                retries += 1
            if n % args.batch == 0:
                rootlogger.info('p%s %s: %s', args.id, index, args.collection)
        else:
            if n % args.batch == 0:
                rootlogger.info('p%s %s: %s: skipping blob %s ', args.id, index, args.collection, blob_name)
        n += 1


def worker(input, args, done_instances):
    # rootlogger.info('p%s: Worker starting: args: %s', args.id, args )
    print(f'p{args.id}: Worker starting: args: {args}')

    conn = psycopg2.connect(dbname=args.db, user=settings.CLOUD_USERNAME, port=settings.CLOUD_PORT,
                            password=settings.CLOUD_PASSWORD, host=settings.CLOUD_HOST)
    with conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            client = storage.Client()
            src_bucket = client.bucket(args.src_bucket, user_project=args.src_project)
            dst_bucket = client.bucket(args.dst_bucket, user_project=args.dst_project)

            for rows, n, rowcount in iter(input.get, 'STOP'):
                copy_instances(args, rows, n, rowcount, done_instances, src_bucket, dst_bucket)
                # output.put(n)


def copy_all_instances(args, cur, query):

    try:
        # Create a set of previously copied blobs
        done_instances = set(open(f'{args.log_dir}/cc_{args.collection}_success.log').read().splitlines())
    except:
        done_instances = []

    increment = args.batch
    cur.execute(query)
    rowcount = cur.rowcount
    print(f'Copying collection {args.collection}; {rowcount} instances')

    strt = time.time()
    num_processes = max(1,min(args.processes, int(rowcount/increment)))
    processes = []
    # Create a pair of queue for each process

    task_queue = Queue()

    # task_queues = [Queue() for p in range(num_processes)]
    # done_queues = [Queue() for p in range(num_processes)]

    # List of patients enqueued
    enqueued_batches = []

    strt = time.time()

    # Start worker processes
    for process in range(num_processes):
        args.id = process + 1
        processes.append(
            Process(group=None, target=worker, args=(task_queue, args, done_instances)))
        # processes.append(
        #     Process(group=None, target=worker, args=(task_queues[process], args, done_instances)))
        # print(f'Started process {args.id}: {processes[-1]}')
        processes[-1].start()

    # Distribute the work across the task_queues
    n = 1
    q=0
    while True:
        rows = cur.fetchmany(increment)
        if len(rows) == 0:
            break
        task_queue.put((rows, n, rowcount))
        # task_queues[q%num_processes].put((rows, n, rowcount))
        enqueued_batches.append(n)
        # print(f'Enqueue {n} on queue {q%num_processes}')
        n += increment
        q+=1
    print('Work distribution complete')

    # Tell child processes to stop
    for i in range(num_processes):
        task_queue.put('STOP')
        # print(f'Stop queue {i}')

    # # Wait until all work is complete
    # q = 0
    # while not enqueued_batches == []:
    #     # Timeout if waiting too long
    #     try:
    #         results = done_queues[q%num_processes].get(timeout=1)
    #         enqueued_batches.remove(results)
    #     except queue.Empty:
    #         pass
    #     q += 1
    #
    # Close all the queues
    # for q in task_queues:
    #     q.close()
    # for q in done_queues:
    #     q.close()

    # Wait for process to terminate
    for process in processes:
        print(f'Joining process: {process.name}, {process.is_alive()}')
        process.join()
        # if process.is_alive():
        #     rootlogger.info('Collection: %s, terminating process %s',args.collection, process.name)
        #     process.kill()
         # print(f'Joined process {process.name.split("-")[-1]}, exitcode: {process.exitcode}')


    delta = time.time() - strt
    rate = rowcount/delta
    print(f'Completed collection {args.collection}, {rate} instances/sec, {num_processes} processes')



def precopy(args):
    conn = psycopg2.connect(dbname=args.db, user=settings.CLOUD_USERNAME, port=settings.CLOUD_PORT,
                            password=settings.CLOUD_PASSWORD, host=settings.CLOUD_HOST)

    # Get excluded collections

    # collections = open(args.collection_list).read().splitlines()

    collections = get_collections_in_version(args)

    try:
        dones = open(args.dones).read().splitlines()
    except:
        dones = []
    for collection in collections:
        if not collection in dones:
            args.collection = collection
            with conn:
                if os.path.exists('{}/logs/cc_{}_error.log'.format(args.log_dir, collection)):
                    os.remove('{}/logs/cc_{}_error.log'.format(args.log_dir, collection))

                # Change logging file. File name includes collection ID.
                for hdlr in successlogger.handlers[:]:
                    successlogger.removeHandler(hdlr)
                success_fh = logging.FileHandler('{}/cc_{}_success.log'.format(args.log_dir, collection))
                successlogger.addHandler(success_fh)
                successformatter = logging.Formatter('%(message)s')
                success_fh.setFormatter(successformatter)

                for hdlr in errlogger.handlers[:]:
                    errlogger.removeHandler(hdlr)
                err_fh = logging.FileHandler('{}/cc_{}_error.log'.format(args.log_dir, collection))
                errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
                errlogger.addHandler(err_fh)
                err_fh.setFormatter(errformatter)

                # Query to get the instances in the collection
                with conn.cursor(cursor_factory=DictCursor) as cur:
                    query = f"""
                        SELECT i.uuid
                        FROM collection as c 
                        JOIN patient as p
                        ON c.collection_id = p.collection_id
                        JOIN study as st
                        ON p.submitter_case_id = st.submitter_case_id
                        JOIN series as se
                        ON st.study_instance_uid = se.study_instance_uid
                        JOIN instance as i
                        ON se.series_instance_uid = i.series_instance_uid
                        WHERE c.collection_id = '{args.collection}'
                        ORDER by i.uuid
                        """
                    args.id = 0
                    copy_all_instances(args, cur, query)

            if not os.path.isfile('{}/logs/cc_{}_error.log'.format(args.log_dir, collection)) or os.stat('{}/logs/cc_{}_error.log'.format(os.environ['PWD'], collection)).st_size==0:
                # If no errors, then we are done with this collection
                with open(args.dones, 'a') as f:
                     f.write(f'{collection}\n')


