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

# Staging buckets are per-collection and contain new/revised instances in
# in a new version. These all need to be copied to the final bucket.

import argparse
import os
from subprocess import run, PIPE
import logging
from logging import INFO
import time
from datetime import timedelta
from multiprocessing import Process, Queue
from queue import Empty


from python_settings import settings
import settings as etl_settings

settings.configure(etl_settings)
assert settings.configured
import psycopg2
from psycopg2.extras import DictCursor

TRIES=3

def copy_collection(args, dones, collection_index, tcia_api_collection_id):
    if not tcia_api_collection_id in dones:
    # if collection['tcia_api_collection_id'] == 'RIDER Breast MRI': # Temporary code for development
        begin = time.time()
        rootlogger.info("p%s: Copying %s, %s", args.id, tcia_api_collection_id, collection_index)

        src_bucket = f"{args.src_bucket_prefix}{tcia_api_collection_id.lower().replace(' ','_').replace('-','_')}"

        try:
            try:
                os.remove(f'gsutil_result_{args.id}.log')
            except:
                pass

            result = run(['gsutil', '-m', '-q', 'cp', '-L', f'gsutil_result_{args.id}.log', f'gs://{src_bucket}/*', f'gs://{args.dst_bucket}'])
                         # stdout=PIPE, stderr=PIPE)
            end = time.time()
            duration = end - begin

            results = open(f'gsutil_result_{args.id}.log').read().splitlines()
            bytes_transferred = 0
            src = results[0].split(',').index('Source')
            res = results[0].split(',').index('Result')
            transferred = results[0].split(',').index('Bytes Transferred')
            for transfer in results[1:]:
                data = transfer.split(',')
                if data[res] != 'OK':
                    errlogger.error('Error transferring %s', data[src])
                    raise RuntimeError('Error transferring %s', data[src])
                else:
                    bytes_transferred += int(data[transferred])

            rootlogger.info("p%s: Copied %s, %s; %.2f GB in %.2fs, %.2f GB/s", args.id, tcia_api_collection_id, collection_index,
                bytes_transferred/2**30, duration, (bytes_transferred/2**30)/duration)


            with open(args.dones, 'a') as f:
                f.write(f"{tcia_api_collection_id}\n")
        except Exception as exc:
            errlogger.error('p%s: Copying %s failed',args.id, tcia_api_collection_id)
            raise exc
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
                        copy_collection(args, dones, collection_index, tcia_api_collection_id)
                        break
                    except Exception as exc:
                        errlogger.error("p%s, exception %s; reattempt %s on collection %s", args.id, exc, attempt, tcia_api_collection_id)


                if attempt == TRIES:
                    errlogger.error("p%s, Failed to process collection: %s", args.id, tcia_api_collection_id)

                output.put((tcia_api_collection_id))

def copy_collections(cur, args, version):
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
    collections = cur.fetchall()

    rootlogger.info("Version %s; %s collections", version['idc_version_number'], len(collections))
    if args.processes == 0:
        args.id=0
        for collection in collections:
            if not collection['tcia_api_collection_id'] in skips:
                if not collection['tcia_api_collection_id'] in skips:
                    collection_index = f'{collections.index(collection)+1} of {len(collections)}'
                    copy_collection(args, dones, collection_index,  collection['tcia_api_collection_id'])

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
            errlogger.error("Exception copy_collections ")
            for process in processes:
                process.terminate()
                process.join()
            duration = str(timedelta(seconds=(time.time() - begin)))
            rootlogger.info("Collection copying NOT completed")

# def copy_collections(cur, args, version):
#     # Session = sessionmaker(bind= sql_engine)
#     # version = version_is_done(sess, args.vnext)
#     skips = open(args.skips).read().splitlines()
#     try:
#         dones = open(args.dones).read().splitlines()
#     except:
#         dones = []
#     begin = time.time()
#     cur.execute("""
#         SELECT * FROM collection
#         WHERE version_id = (%s)""", (version['id'],))
#     collections = cur.fetchall()
#
#     rootlogger.info("Version %s; %s collections", version['idc_version_number'], len(collections))
#     for collection in collections:
#         if not collection['tcia_api_collection_id'] in skips:
#             collection_index = f'{collections.index(collection)+1} of {len(collections)}'
#             copy_collection(cur, args, dones, collection_index, version, collection)
#

def precopy(args):
    conn = psycopg2.connect(dbname=settings.DATABASE_NAME, user=settings.DATABASE_USERNAME,
                            password=settings.DATABASE_PASSWORD, host=settings.DATABASE_HOST)
    with conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("""
            SELECT * 
            FROM version
            WHERE idc_version_number = (%s)""", (args.vnext,))

            version = cur.fetchone()
            copy_collections(cur, args, version)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--vnext', default=2, help='Next version to generate')
    parser.add_argument('--src_bucket_prefix', default='idc_v2_', help='Bucket in which to save instances')
    parser.add_argument('--dst_bucket', default='idc_dev', help='Bucket in which to save instances')
    parser.add_argument('--processes', default=3, help="Number of concurrent processes")
    parser.add_argument('--project', default='idc-dev-etl')
    parser.add_argument('--skips', default='./logs/copy_staging_skips.log' )
    parser.add_argument('--dones', default='./logs/copy_staging_dones.log' )
    args = parser.parse_args()


    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/copy_staging_log.log'.format(os.environ['PWD']))
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
    err_fh = logging.FileHandler('{}/logs/copy_staging_err.log'.format(os.environ['PWD']))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)


    precopy(args)
