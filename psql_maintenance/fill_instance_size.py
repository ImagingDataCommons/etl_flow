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
import os
import json
import logging
from logging import INFO
import time
from multiprocessing import Process, Queue
import argparse
from google.cloud import storage
from utilities.bq_helpers import BQ_table_exists, create_BQ_table, delete_BQ_Table, load_BQ_from_CSV, load_BQ_from_json

from python_settings import settings

import psycopg2
from psycopg2.extras import DictCursor, RealDictCursor

def fill_instance_size(cur, conn, args, client, bucket, uuids, n):
    update_sql = """
        UPDATE instance
            SET size = %s
            WHERE uuid = %s"""

    start = time.time()
    for uuid in uuids:
        blob = bucket.blob(f'{uuid}.dcm')
        blob.reload()
        size = blob.size
        cur.execute(update_sql, (size, uuid))
    conn.commit()
    delta = time.time() - start
    print(f'{n}:{n+len(uuids)-1}, {len(uuids) / delta}/sec')


def worker(input, args):
    client = storage.Client()
    bucket = client.get_bucket(args.bucket)
    conn = psycopg2.connect(dbname=args.db, user=args.user, port=args.port,
                            password=args.password, host=args.host)
    with conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            for uuids, n in iter(input.get, 'STOP'):
                fill_instance_size(cur, conn, args, client, bucket, uuids, n)

def fillem(args):
    num_processes = args.processes
    processes = []

    task_queue = Queue()
    strt = time.time()

    # Start worker processes
    for process in range(num_processes):
        args.id = process + 1
        processes.append(
            Process(group=None, target=worker, args=(task_queue, args)))
        processes[-1].start()

    conn = psycopg2.connect(dbname=args.db, user=args.user, port=args.port,
                            password=args.password, host=args.host)
    with conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            query = f"""
                SELECT uuid 
                FROM instance
                WHERE size=0
                ORDER BY uuid"""
            cur.execute(query)
            n = 0
            while True:
                uuids = [row[0]for row in cur.fetchmany(args.batch)]
                if len(uuids) == 0:
                    break
                task_queue.put((uuids, n))
                # print(f'Queued {n}:{n+len(blobs)-1}')
                # task_queue.put((page, n))
                n += len(uuids)

            print('Primary work distribution complete; {} blobs'.format(n))

            # Tell child processes to stop
            for i in range(num_processes):
                task_queue.put('STOP')

            # Wait for process to terminate
            for process in processes:
                # print(f'Joining process: {process.name}, {process.is_alive()}')
                process.join()

            delta = time.time() - strt
            rate = (n) / delta
            print(f'Completed bucket {args.bucket}, {rate} instances/sec, {num_processes} processes')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=5, help='Version to upload')
    parser.add_argument('--db', default='idc_v5', help="Database to access")
    parser.add_argument('--project', default='idc-dev-etl')
    parser.add_argument('--bucket', default='idc_dev')
    parser.add_argument('--processes', default=32)
    parser.add_argument('--batch', default=100)
    args = parser.parse_args()
    parser.add_argument('--user', default=settings.CLOUD_USERNAME)
    parser.add_argument('--password', default=settings.CLOUD_PASSWORD)
    parser.add_argument('--host', default=settings.CLOUD_HOST)
    parser.add_argument('--port', default=settings.CLOUD_PORT)
    args = parser.parse_args()

    print('args: {}'.format(args))

    fillem(args)
