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
Multiprocess bucket blob counter. Processes work from an input queue of prefixes.
The prefix length is the number of hex digits in the prefix. The number of processes
is separately specified. The number of prefixes is 16^prefix_length.
Thus, a prefix_length of 2 will create 256 prefixes, and can be efficiently processed
by 256 processes.
"""

import argparse
from utilities.logging_config import successlogger, progresslogger, errlogger
from multiprocessing import Process, Queue
from google.cloud import storage
from python_settings import settings

def worker(input, output, args):
    client = storage.Client()
    for prefix in iter(input.get, 'STOP'):
        client = storage.Client()
        # bucket = storage.Bucket(client, args.bucket)
        blobs = 0

        progresslogger.info(f'Counting blobs in prefix {prefix}')

        iterator = client.list_blobs(args.bucket, prefix=prefix)
        for page in iterator.pages:
            blobs += page.num_items
        successlogger.info(f'Prefix {prefix} has {blobs} blobs')
        output.put(blobs)

def count_all_instances(args):
    all_blobs = 0
    processes = []
    # Create queues
    task_queue = Queue()
    done_queue = Queue()

    num_processes = args.num_processes
    for process in range(num_processes):
        args.pid = process + 1
        processes.append(
            Process(target=worker, args=(task_queue, done_queue, args)))
        processes[-1].start()

    args.pid = 0
    for i in range(pow(16, args.prefix_length)):
        prefix = hex(i)[2:].zfill(args.prefix_length)
        task_queue.put(prefix)


    # Tell child processes to stop
    for process in processes:
        task_queue.put('STOP')
    # Wait for them to stop
    for process in processes:
        process.join()

    while not done_queue.empty():
        all_blobs += done_queue.get(True)

    successlogger.info(f'Total blobs in bucket {args.bucket} is {all_blobs}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION)
    parser.add_argument('--bucket', default='dicom_store_import_v21_idc-open-idc1_idc-dev-etl', help='Bucket to count')
    parser.add_argument('--num_processes', default=256, help='Number of concurrent processes')
    parser.add_argument('--prefix_length', default=2, help='Prefix length')
    args = parser.parse_args()

    count_all_instances(args)

