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

from utilities.logging_config import successlogger, progresslogger, errlogger

import time
from multiprocessing import Process, Queue
from google.cloud import storage
from google.api_core.exceptions import ServiceUnavailable, NotFound

def delete_instances(args, client, bucket, blobs, n):
    try:
        # with client.batch():
        #     for blob in blobs:
        #         bucket.blob(blob).delete()
        #         # bucket.blob(blob[0], generation=blob[1]).delete()
        for blob in blobs:
            bucket.blob(blob).delete()
            # bucket.blob(blob[0], generation=blob[1]).delete()
            successlogger.info(f'{blob}')
    except ServiceUnavailable:
        errlogger.error('p%s Delete %s blob %s failed', args.id, args.bucket, blob)
    except NotFound:
        errlogger.error('p%s Delete %s blobs % failed, not found', args.id, args.bucket, blob)
    except Exception as exc:
        errlogger.error('p%s Exception on %s blob %s: %s', args.id, args.bucket, blob, exc)


def worker(input, args):
    client = storage.Client()
    bucket = storage.Bucket(client, args.bucket)
    for blobs, n in iter(input.get, 'STOP'):
        delete_instances(args, client, bucket, blobs, n)


def del_all_instances(args, instance_list):
    bucket = args.bucket
    client = storage.Client()
    bucket = storage.Bucket(client, args.bucket)

    dones = set(open(successlogger.handlers[0].baseFilename).read().splitlines())

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
    n=0
    # Submit args.batch size chunks to process
    while instance_list:
        some_instances= list(set(instance_list[0:args.batch]) - dones)
        instance_list = instance_list[args.batch:]
        if some_instances:
            task_queue.put((some_instances,n))
        n += args.batch
    progresslogger.info('Primary work distribution complete; {} blobs'.format(n))

    # Tell child processes to stop
    for i in range(num_processes):
        task_queue.put('STOP')


    # Wait for process to terminate
    for process in processes:
        # print(f'Joining process: {process.name}, {process.is_alive()}')
        process.join()

    delta = time.time() - strt
    rate = (n)/delta
    progresslogger.info(f'Completed bucket {args.bucket}, {rate} instances/sec, {num_processes} processes')
