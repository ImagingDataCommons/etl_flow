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

# Copy all blobs named in some collection from the dev bucket to some other bucket.


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

def copy_instances(args, rows, n, rowcount, done_instances, src_bucket, dst_bucket):
    for row in rows:
        index = f'{n}/{rowcount}'
        blob_name = f'{row["uuid"]}.dcm'
        if not blob_name in done_instances:
            retries = 0
            while True:
                try:
                    blob_copy = src_bucket.copy_blob(src_bucket.blob(blob_name), dst_bucket)
                    rootlogger.info('%s %s: %s: copy succeeded %s', args.id, index, args.collection, blob_name)
                    successlogger.info(f'{blob_name}')
                    break
                except Exception as exc:
                    errlogger.error('%s %s: %s: copy failed %s\n, retry %s; %s', args.id,
                                    index, args.collection,
                                    blob_name, retries, exc)
                    if retries == TRIES:
                        break
                time.sleep(retries)
                retries += 1
        else:
            if n % 10000 == 0:
                rootlogger.info('%s %s: %s: skipping blob %s ', args.id, index, args.collection, blob_name)
        n += 1


def worker(input, output, args, done_instances):
    rootlogger.debug('p%s: Worker starting: args: %s', args.id, args )
    conn = psycopg2.connect(dbname='idc_path_v3.1', user=settings.LOCAL_DATABASE_USERNAME,
                            password=settings.LOCAL_DATABASE_PASSWORD, host=settings.LOCAL_DATABASE_HOST)
    with conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            client = storage.Client()
            src_bucket = client.bucket(args.src_bucket, user_project=args.src_project)
            dst_bucket = client.bucket(args.dst_bucket, user_project=args.dst_project)

            for rows, n, rowcount in iter(input.get, 'STOP'):
                copy_instances(args, rows, n, rowcount, done_instances, src_bucket, dst_bucket)


def copy_all_instances(args, cur, query):

    client = storage.Client()
    src_bucket = client.bucket(args.src_bucket, user_project=args.src_project)
    dst_bucket = client.bucket(args.dst_bucket, user_project=args.dst_project)
    n = 1

    try:
        done_instances = set(open(f'./logs/cc_{args.collection}_success.log').read().splitlines())
    except:
        done_instances = []

    increment = 1000
    cur.execute(query)
    rowcount=cur.rowcount
    if args.processes == 0:
        failures = open(f'./logs/cc_{args.collection}_failures.log', 'a')
        while True:
            rows = cur.fetchmany(increment)
            if len(rows) == 0:
                break
            for row in rows:
                index = f'{n}/{rowcount}'
                blob_name = f'{row["uuid"]}.dcm'
                if not blob_name in done_instances:
                    retries = 0
                    while True:
                        try:
                            blob_copy = src_bucket.copy_blob(src_bucket.blob(blob_name), dst_bucket)
                            rootlogger.info('%s %s: %s: copy succeeded %s', args.id, index, args.collection, blob_name)
                            successlogger.info(f'{blob_name}\n')
                            break
                        except Exception as exc:
                            errlogger.error('%s %s: %s: copy failed %s\n, retry %s; %s', args.id,
                                            index, args.collection,
                                            blob_name, retries, exc)
                            if retries == TRIES:
                                failures.write(f'{blob_name}; {exc}\n')
                                break
                        time.sleep(retries)
                        retries += 1
                else:
                    if n % 10000 == 0:
                        rootlogger.info('%s %s: %s: skipping blob %s ', args.id, index, args.collection, blob_name)
                n += 1

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
                Process(target=worker, args=(task_queue, done_queue, args, done_instances)))
            processes[-1].start()

        while True:
            rows = cur.fetchmany(increment)
            if len(rows) == 0:
                break
            task_queue.put((rows, n, rowcount))

            n += increment
        # Tell child processes to stop
        for process in processes:
            task_queue.put('STOP')

        # Wait for them to stop
        for process in processes:
            process.join()








def precopy(args):
    conn = psycopg2.connect(dbname='idc_path_v3.1', user=settings.LOCAL_DATABASE_USERNAME,
                            password=settings.LOCAL_DATABASE_PASSWORD, host=settings.LOCAL_DATABASE_HOST)
    with conn:
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


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=3, help='Next version to generate')
    args = parser.parse_args()
    parser.add_argument('--db', default=f'idc_v{args.version}')
    parser.add_argument('--src_bucket', default='idc_dev')
    parser.add_argument('--dst_bucket', default='public-datasets-dev-idc')
    parser.add_argument('--processes', default=32, help="Number of concurrent processes")
    parser.add_argument('--src_project', default='idc-dev-etl')
    parser.add_argument('--dst_project', default='idc-dev-etl')
    parser.add_argument('--collection', default='QIN-HEADNECK')

    args = parser.parse_args()

    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/copy_collections_log.log'.format(os.environ['PWD']))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    successlogger = logging.getLogger('success')
    success_fh = logging.FileHandler('{}/logs/cc_{}_success.log'.format(os.environ['PWD'], args.collection))
    successformatter = logging.Formatter('%(message)s')
    successlogger.addHandler(success_fh)
    success_fh.setFormatter(successformatter)
    successlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/copy_collections_err.log'.format(os.environ['PWD']))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    precopy(args)
