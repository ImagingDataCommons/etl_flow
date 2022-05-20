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
General purpose multiprocessing routine to copy the entire contents
of a bucket to another bucket.
Used to duplicate dev buckets such as idc-dev-open, idc-dev-cr, etc.
that hold all IDC data across all versions (not just the current version)
to open/public buckets.
"""

import argparse
import os
import logging
from logging import INFO
proglogger = logging.getLogger('root.prog')
successlogger = logging.getLogger('root.success')
errlogger = logging.getLogger('root.err')

import time
from multiprocessing import Process, Queue
from google.cloud import storage, bigquery
from google.api_core.exceptions import ServiceUnavailable, GoogleAPICallError

from python_settings import settings
import settings as etl_settings

if not settings.configured:
    settings.configure(etl_settings)
assert settings.configured

TRIES = 3

def copy_instances(args, client, src_bucket, dst_bucket, blob_names, n):
    for blob_name in blob_names:
        src_blob = src_bucket.blob(blob_name)
        dst_blob = dst_bucket.blob(blob_name)
        retries = 0
        while True:
            try:
                rewrite_token = False
                while True:
                    rewrite_token, bytes_rewritten, bytes_to_rewrite = dst_blob.rewrite(
                        src_blob, token=rewrite_token
                    )
                    if not rewrite_token:
                        break
                successlogger.info(f'{blob_name}')
                break
            except Exception as exc:
                if retries == TRIES:
                    errlogger.error('p%s %s: %s copy failed \n, retry %s; %s', args.id,
                                    n, blob_name, retries, exc)
                    break
            time.sleep(retries)
            retries += 1

    proglogger.info('p%s Copied blobs %s:%s ', args.id, n, n+len(blob_names)-1)


def worker(input, args):
    # proglogger.info('p%s: Worker starting: args: %s', args.id, args )
    # print(f'p{args.id}: Worker starting: args: {args}')

    RETRIES=3

    client = storage.Client()
    src_bucket = storage.Bucket(client, args.src_bucket)
    dst_bucket = storage.Bucket(client, args.dst_bucket)
    for blob_names, n in iter(input.get, 'STOP'):
        copy_instances(args, client, src_bucket, dst_bucket, blob_names, n)


def copy_all_instances(args):
    client = storage.Client()
    src_bucket = storage.Bucket(client, args.src_bucket)
    dst_bucket = storage.Bucket(client, args.dst_bucket)

    try:
        # Create a set of previously copied blobs
        done_instances = set(open(f'{args.log_dir}/{args.src_bucket}_success.log').read().splitlines())
    except:
        done_instances = []

    n=len(done_instances)

    print(f"{len(done_instances)} previously copied")
    done_instances = set(done_instances)

    print(f'Copying bucket {args.src_bucket} to {args.dst_bucket}, ')

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
    iterator = client.list_blobs(src_bucket,  page_size=args.batch)
    for page in iterator.pages:
        if page.num_items:
            blobs = set([blob.name for blob in page])
            blobs = blobs - done_instances
            task_queue.put((blobs, n))
            # print(f'Queued {n}:{n+len(blobs)-1}')
        else:
            break
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
    print(f'Completed bucket {args.src_bucket}, {rate} instances/sec, {num_processes} processes')


def pre_copy(args):

    bucket = args.src_bucket
    # if os.path.exists('{}/logs/{}_error.log'.format(args.log_dir, bucket)):
    #     os.remove('{}/logs/{}_error.log'.format(args.log_dir, bucket))
    if not os.path.exists('{}'.format(args.log_dir)):
        os.mkdir('{}'.format(args.log_dir))
        st = os.stat('{}'.format(args.log_dir))

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

    copy_all_instances(args)




# if __name__ == '__main__':
#     parser = argparse.ArgumentParser()
#     parser.add_argument('--src_project', default='idc-dev-etl')
#     parser.add_argument('--src_bucket', default='idc-dev-open')
#     parser.add_argument('--dst_project', default='idc-pdp-staging')
#     parser.add_argument('--dst_bucket', default=f'idc-open-pdp-staging')
#     parser.add_argument('--processes', default=1, help="Number of concurrent processes")
#     parser.add_argument('--batch', default=100, help='Size of batch assigned to each process')
#     parser.add_argument('--log_dir', default=f'/mnt/disks/idc-etl/logs/copy_bucket_mp')
#
#     args = parser.parse_args()
#
#     proglogger = logging.getLogger('prog')
#     prog_fh = logging.FileHandler(f'{os.environ["PWD"]}/logs/bucket.log')
#     progformatter = logging.Formatter('%(levelname)s:prog:%(message)s')
#     proglogger.addHandler(prog_fh)
#     prog_fh.setFormatter(progformatter)
#     proglogger.setLevel(INFO)
#
#     successlogger = logging.getLogger('success')
#     successlogger.setLevel(INFO)
#
#     errlogger = logging.getLogger('prog.err')
#
#     pre_copy(args)
