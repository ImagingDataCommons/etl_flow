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
import json
import os
import argparse
import logging
from logging import INFO
from google.cloud import bigquery, storage
import time
from multiprocessing import Process, Queue
from utilities.logging_config import successlogger, progresslogger, errlogger

# Copy the blobs that are new to a version from dev pre-staging buckets
# to dev staging buckets.
import settings


# Get a the dev_url and pub_url of all new instances. The dev_url is the url of the
# premerge bucket or staging bucket holding the new instance. The pub_url is the
# url of the bucket to which to copy it
def get_urls(args):
    client = bigquery.Client()
    query = f"""
    SELECT
      instance_uuid uuid
    FROM
      `idc-dev-etl.idc_v{args.version}_pub.auxiliary_metadata`
    WHERE
      instance_revised_idc_version = {args.version}
      AND tcia_api_collection_id = '{args.collection}'
    """
    # urls = list(client.query(query))
    query_job = client.query(query)  # Make an API request.
    query_job.result()  # Wait for the query to complete.
    destination = query_job.destination
    destination = client.get_table(destination)
    return destination

def move_some_blobs(args, client, urls, n, dones):
    done = 0
    copied = 0
    for blob_name in urls:
        if not blob_name in dones:
            src_bucket = client.bucket(args.src_bucket)
            src_blob = src_bucket.blob(blob_name)
            trg_bucket = client.bucket(args.trg_bucket)
            trg_blob = trg_bucket.blob(blob_name)
            for attempt in range(3):
                try:
                    rewrite_token = False
                    while True:
                        rewrite_token, bytes_rewritten, bytes_to_rewrite = trg_blob.rewrite(
                            src_blob, token=rewrite_token
                        )
                        if not rewrite_token:
                            break
                    src_blob.delete()

                    successlogger.info('%s', blob_name)
                    progresslogger.info(f'p{args.id}: {done+n}of{len(urls)+n}: {args.src_bucket}/{blob_name} --> {args.trg_bucket}/{blob_name}')
                    break
                except Exception as exc:
                    errlogger.error('p%s: Blob: %s, attempt: %s;  %s', args.id, blob_name, attempt, exc)

        done += 1
    if copied == 0:
        progresslogger.info(f'p{args.id}: Skipped {n}:{n+done-1}')


def worker(input, args, dones):
    # proglogger.info('p%s: Worker starting: args: %s', args.id, args )
    # print(f'p{args.id}: Worker starting: args: {args}')

    # RETRIES = 3
    # try:
    #     dones = set(open(f'{successlogger.handlers[0].baseFilename}').read().splitlines())
    # except:
    #     dones = []

    client = storage.Client()
    for urls, n in iter(input.get, 'STOP'):
        move_some_blobs(args, client, urls, n, dones)


def copy_all_blobs(args):
    bq_client = bigquery.Client()
    destination = get_urls(args)

    num_processes = args.processes
    processes = []
    # Create a pair of queue for each process

    task_queue = Queue()

    strt = time.time()
    dones = set(open(f'{successlogger.handlers[0].baseFilename}').read().splitlines())

    # Start worker processes
    for process in range(num_processes):
        args.id = process + 1
        processes.append(
            Process(group=None, target=worker, args=(task_queue, args, dones)))
        processes[-1].start()

    # Distribute the work across the task_queues
    n = 0
    for page in bq_client.list_rows(destination, page_size=args.batch).pages:
        uuids = [f'{row.uuid}.dcm' for row in page]
        task_queue.put((uuids, n))
        # print(f'Queued {n}:{n+args.batch-1}')
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='Version to work on')
    # parser.add_argument('--log_dir', default=f'{settings.LOGGING_BASE}/{settings.BASE_NAME}')
    parser.add_argument('--batch', default=100)
    parser.add_argument('--processes', default=16)
    parser.add_argument('--collection', default = 'CPTAC-LSCC')
    parser.add_argument('--src_bucket', default = 'idc-open-idc1')
    parser.add_argument('--trg_bucket', default = 'idc-open-pdp-staging')
    args = parser.parse_args()
    args.id = 0 # Default process ID

    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    # if not os.path.exists(settings.LOGGING_BASE):
    #     os.mkdir(settings.LOGGING_BASE)
    # if not os.path.exists(args.log_dir):
    #     os.mkdir(args.log_dir)
    #
    # successlogger = logging.getLogger('root.success')
    # successlogger.setLevel(INFO)
    # for hdlr in successlogger.handlers[:]:
    #     successlogger.removeHandler(hdlr)
    # success_fh = logging.FileHandler('{}/success.log'.format(args.log_dir))
    # successlogger.addHandler(success_fh)
    # successformatter = logging.Formatter('%(message)s')
    # success_fh.setFormatter(successformatter)
    #
    # errlogger = logging.getLogger('root.err')
    # for hdlr in errlogger.handlers[:]:
    #     errlogger.removeHandler(hdlr)
    # err_fh = logging.FileHandler('{}/error.log'.format(args.log_dir))
    # errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    # errlogger.addHandler(err_fh)
    # err_fh.setFormatter(errformatter)


    copy_all_blobs(args)