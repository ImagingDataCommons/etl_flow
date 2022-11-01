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

# One use script:
# Copy all the idc instances in  cptac_cm, cptac_lscc from idc-dev-cm to idc-dev-open

import argparse
import json
from utilities.logging_config import successlogger, progresslogger, errlogger
from utilities.bq_helpers import query_BQ

import time
from multiprocessing import Process, Queue
from google.cloud import storage, bigquery

import settings

TRIES = 3

def get_blob_names(args):
    client = bigquery.Client()
    query = f"""
    SELECT
    DISTINCT i_uuid
    FROM
        `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.all_joined_included`
    WHERE
        collection_id in ('CPTAC-CM', 'CPTAC-LSCC')
        AND i_source = 'idc'
        AND i_rev_idc_version < 10
    ORDER by i_uuid
    """
    result = client.query(query)
    blobs = [f'{row.i_uuid}.dcm' for row in result]
    return blobs


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

    progresslogger.info('p%s Copied blobs %s:%s ', args.id, n, n+len(blob_names)-1)


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
    all_blobs = get_blob_names(args)
    src_client = storage.Client(project=args.src_project)
    dst_client = storage.Client(project=args.dst_project)
    src_bucket = storage.Bucket(src_client, args.src_bucket)
    dst_bucket = storage.Bucket(dst_client, args.dst_bucket)

    try:
        # Create a set of previously copied blobs
        done_instances = set(open(successlogger.handlers[0].baseFilename).read().splitlines())
    except:
        done_instances = []

    n=len(done_instances)
    # done_instances = []
    # iterator = client.list_blobs(dst_bucket, page_size=args.batch)
    # for page in iterator.pages:
    #     blobs = [blob.name for blob in page]
    #     done_instances.extend(blobs)
    #     # if len(blobs) == 0:
    #     #     break

    progresslogger.info(f"{len(done_instances)} previously copied")
    done_instances = set(done_instances)

    progresslogger.info(f'Copying collection cptac_cm, cptac_lscc from {args.src_bucket} to {args.dst_bucket}, ')

    num_processes = args.processes
    processes = []
    # Create a pair of queue for each process

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
    while all_blobs:
        blobs = set(all_blobs[:args.batch])
        all_blobs = all_blobs[args.batch:]
        blobs = blobs - done_instances
        if blobs:
            task_queue.put((blobs, n))
        # print(f'Queued {n}:{n+len(blobs)-1}')
        n += len(blobs)
    progresslogger.info('Primary work distribution complete; {} blobs'.format(n))

    # Tell child processes to stop
    for i in range(num_processes):
        task_queue.put('STOP')

    # Wait for process to terminate
    for process in processes:
        progresslogger.info(f'Joining process: {process.name}, {process.is_alive()}')
        process.join()

    delta = time.time() - strt
    rate = (n)/delta
    progresslogger.info(f'Completed bucket {args.src_bucket}, {rate} instances/sec, {num_processes} processes')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--src_project', default=settings.DEV_PROJECT)
    parser.add_argument('--src_bucket', default='idc-dev-defaced')
    parser.add_argument('--dst_project', default=settings.PDP_PROJECT)
    parser.add_argument('--dst_bucket', default=f'idc-open-pdp-staging')
    parser.add_argument('--processes', default=48, help="Number of concurrent processes")
    parser.add_argument('--batch', default=100, help='Size of batch assigned to each process')

    args = parser.parse_args()
    print(f'args: {json.dumps(args.__dict__, indent=2)}')
    copy_all_instances(args)
