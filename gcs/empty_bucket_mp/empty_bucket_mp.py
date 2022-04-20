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
Multiprocess bucket emptier. Does not delete the bucket.
May saturate a small VM, depending on the number of processes.
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
from google.cloud import storage
from google.api_core.exceptions import ServiceUnavailable, NotFound

from python_settings import settings
import settings as etl_settings

if not settings.configured:
    settings.configure(etl_settings)
assert settings.configured


def delete_instances(args, client, bucket, blobs, n):
    try:
        with client.batch():
            for blob in blobs:
                bucket.blob(blob[0], generation=blob[1]).delete()
                # bucket.blob(blob[0], generation=blob[1]).delete()

        successlogger.info('p%s Delete %s blobs %s:%s ', args.id, args.bucket, n, n+len(blobs)-1)
    except ServiceUnavailable:
        errlogger.error('p%s Delete %s blobs %s:%s failed', args.id, args.bucket, n, n+len(blobs)-1)
    except NotFound:
        errlogger.error('p%s Delete %s blobs %s:%s failed, not found', args.id, args.bucket, n, n+len(blobs)-1)
    except Exception as exc:
        errlogger.error('p%s Exception %s %s:%s', args.id, exc, n, n+len(blobs)-1)



def worker(input, args):
    client = storage.Client()
    bucket = storage.Bucket(client, args.bucket)
    for blobs, n in iter(input.get, 'STOP'):
        delete_instances(args, client, bucket, blobs, n)


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
    iterator = client.list_blobs(bucket, versions=True, page_token=page_token, page_size=args.batch)
    for page in iterator.pages:
        blobs = [[blob.name, blob.generation] for blob in page]
        if len(blobs) == 0:
            break
        task_queue.put((blobs, n))
        # print(f'Queued {n}:{n+len(blobs)-1}')
        # task_queue.put((page, n))

        n += page.num_items
    print('Primary work distribution complete; {} blobs'.format(n))

    # Tell child processes to stop
    for i in range(num_processes):
        task_queue.put('STOP')


    # Wait for process to terminate
    for process in processes:
        # print(f'Joining process: {process.name}, {process.is_alive()}')
        process.join()

    delta = time.time() - strt
    rate = (n)/delta
    print(f'Completed bucket {args.bucket}, {rate} instances/sec, {num_processes} processes')


def pre_delete(args):

    bucket = args.bucket
    # if os.path.exists('{}/logs/{}_error.log'.format(args.log_dir, bucket)):
    #     os.remove('{}/logs/{}_error.log'.format(args.log_dir, bucket))
    if not os.path.exists('{}'.format(args.log_dir)):
        os.mkdir('{}'.format(args.log_dir))
        st = os.stat('{}'.format(args.log_dir))
        # os.chmod('{}'.format(args.log_dir), st.st_mode | 0o222)

    # Change logging file. File name includes bucket ID.
    for hdlr in successlogger.handlers[:]:
        successlogger.removeHandler(hdlr)
    success_fh = logging.FileHandler('{}/success.log'.format(args.log_dir))
    successlogger.addHandler(success_fh)
    successformatter = logging.Formatter('%(message)s')
    success_fh.setFormatter(successformatter)

    for hdlr in errlogger.handlers[:]:
        errlogger.removeHandler(hdlr)
    err_fh = logging.FileHandler('{}/error.log'.format(args.log_dir))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    del_all_instances(args)


# if __name__ == '__main__':
#     parser = argparse.ArgumentParser()
#     parser.add_argument('--bucket', default='')
#     parser.add_argument('--processes', default=128, help="Number of concurrent processes")
#     parser.add_argument('--batch', default=100, help='Size of batch assigned to each process')
#     parser.add_argument('--project', default='idc-dev-etl')
#     parser.add_argument('--log_dir', default=f'/mnt/disks/idc-etl/logs/empty_bucket_mp')
#
#     args = parser.parse_args()
#
#     if not os.path.exists('{}'.format(args.log_dir)):
#         os.mkdir('{}'.format(args.log_dir))
#
#     proglogger = logging.getLogger('root.prog')
#     prog_fh = logging.FileHandler(f'{os.environ["PWD"]}/logs/bucket.log')
#     progformatter = logging.Formatter('%(levelname)s:prog:%(message)s')
#     proglogger.addHandler(prog_fh)
#     prog_fh.setFormatter(progformatter)
#     proglogger.setLevel(INFO)
#
#     successlogger = logging.getLogger('success')
#     successlogger.setLevel(INFO)
#
#     errlogger = logging.getLogger('root.err')
#
#     pre_delete(args)
