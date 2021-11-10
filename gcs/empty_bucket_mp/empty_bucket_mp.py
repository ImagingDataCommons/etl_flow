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
Multiprocess bucket emptier.
"""

import argparse
import os
import logging
from logging import INFO
rootlogger = logging.getLogger('root')
successlogger = logging.getLogger('success')
errlogger = logging.getLogger('root.err')

import time
from multiprocessing import Process, Queue
from google.cloud import storage, bigquery
from google.api_core.exceptions import ServiceUnavailable

from python_settings import settings
import settings as etl_settings

settings.configure(etl_settings)
assert settings.configured


def delete_instances(args, client, bucket, blob_names, n):
    try:
        with client.batch():
            for blob_name in blob_names:
                bucket.blob(blob_name).delete()

        rootlogger.info('p%s Delete blobs %s:%s ', args.id, n, n+len(blob_names)-1)
    except ServiceUnavailable:
        errlogger.error('p%s Delete blobs %s:%s failed', args.id, n, n+len(blob_names)-1)



def worker(input, args):
    client = storage.Client()
    bucket = storage.Bucket(client, args.bucket)
    for blob_names, n in iter(input.get, 'STOP'):
        delete_instances(args, client, bucket, blob_names, n)


def del_all_instances(args):
    client = storage.Client()

    bucket = storage.Bucket(client, args.bucket)

    print(f'Deleting bucket {args.bucket}')

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


    # Distribute the work across the task_queues
    n = 0
    page_token = ""
    # iterator = client.list_blobs(bucket, page_token=page_token, max_results=args.batch)
    iterator = client.list_blobs(bucket, page_token=page_token, page_size=args.batch)
    for page in iterator.pages:
        blobs = [blob.name for blob in page]
        # if len(blobs) == 0:
        #     break
        task_queue.put((blobs, n))
        print(f'Queued {n}:{n+len(blobs)-1}')
        # task_queue.put((page, n))

        n += page.num_items
    print('Primary work distribution complete; {} blobs'.format(n))

    # Tell child processes to stop
    for i in range(num_processes):
        task_queue.put('STOP')


    # Wait for process to terminate
    for process in processes:
        print(f'Joining process: {process.name}, {process.is_alive()}')
        process.join()

    delta = time.time() - strt
    rate = (n)/delta
    print(f'Completed bucket {args.bucket}, {rate} instances/sec, {num_processes} processes')


def pre_delete(args):

    bucket = args.bucket
    if os.path.exists('{}/logs/{}_error.log'.format(args.log_dir, bucket)):
        os.remove('{}/logs/{}_error.log'.format(args.log_dir, bucket))

    # Change logging file. File name includes bucket ID.
    for hdlr in successlogger.handlers[:]:
        successlogger.removeHandler(hdlr)
    success_fh = logging.FileHandler('{}/{}_success.log'.format(args.log_dir, bucket))
    successlogger.addHandler(success_fh)
    successformatter = logging.Formatter('%(message)s')
    success_fh.setFormatter(successformatter)

    for hdlr in errlogger.handlers[:]:
        errlogger.removeHandler(hdlr)
    err_fh = logging.FileHandler('{}/{}_error.log'.format(args.log_dir, bucket))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    del_all_instances(args)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', default='idc-dev-v5-dicomstore-staging')
    parser.add_argument('--dst_bucket', default=f'idc-dev-v5-dicomstore-staging')
    parser.add_argument('--processes', default=21, help="Number of concurrent processes")
    parser.add_argument('--batch', default=100, help='Size of batch assigned to each process')
    parser.add_argument('--project', default='idc-dev-etl')
    parser.add_argument('--log_dir', default=f'/mnt/disks/idc-etl/logs/delete_bucket_mp')

    args = parser.parse_args()

    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler(f'{os.environ["PWD"]}/logs/bucket.log')
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    successlogger = logging.getLogger('success')
    successlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')

    pre_delete(args)
