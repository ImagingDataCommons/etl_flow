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
import argparse
import logging
from logging import INFO
from google.cloud import bigquery, storage
import time
from multiprocessing import Process, Queue

# Copy the blobs that are new to a version from dev staging buckets
# to pub buckets.

def get_urls(args):
    client = bigquery.Client()
    query = f"""
    SELECT
      dev.gcs_url as dev_url,
      pub.gcs_url as pub_url
    FROM
      `idc-dev-etl.idc_v{args.version}.auxiliary_metadata` dev
    JOIN
      `idc-pdp-staging.idc_v{args.version}.auxiliary_metadata` pub
    ON
      dev.instance_uuid = pub.instance_uuid
    WHERE
      dev.instance_revised_idc_version = {args.version}
    """
    # urls = list(client.query(query))
    query_job = client.query(query)  # Make an API request.
    query_job.result()  # Wait for the query to complete.
    destination = query_job.destination
    destination = client.get_table(destination)
    return destination

def copy_some_blobs(args, client, urls, n):
    try:
        dones = open(f'{args.log_dir}/success.log').read().splitlines()
    except:
        dones = []
    for blob in urls:
        blob_name = blob['dev_url'].split('/')[3]
        if not blob_name in dones:
            dev_bucket_name=blob['dev_url'].split('/')[2]
            dev_bucket = client.bucket(blob['dev_url'].split('/')[2])
            dev_blob = dev_bucket.blob(blob_name)
            pub_bucket_name = blob['pub_url'].split('/')[2]
            if 'public-datasets-idc' in pub_bucket_name:
                pub_bucket_name = 'idc-open-pdp-staging'
            pub_bucket = client.bucket(pub_bucket_name)
            pub_blob = pub_bucket.blob(blob_name)
            try:
                rewrite_token = False
                while True:
                    rewrite_token, bytes_rewritten, bytes_to_rewrite = pub_blob.rewrite(
                        dev_blob, token=rewrite_token
                    )
                    if not rewrite_token:
                        break
                successlogger.info('%s', blob_name)
                print(f'p{args.id}: {n}of{len(urls)}: {dev_bucket_name}/{blob_name} --> {pub_bucket_name}/{blob_name}')
            except Exception as exc:
                errlogger.error('Blob: %s: %s', blob_name, exc)
        n += 1


def worker(input, args):
    # proglogger.info('p%s: Worker starting: args: %s', args.id, args )
    # print(f'p{args.id}: Worker starting: args: {args}')

    RETRIES = 3

    client = storage.Client()
    for urls, n in iter(input.get, 'STOP'):
        copy_some_blobs(args, client, urls, n)


def copy_all_blobs(args):
    bq_client = bigquery.Client()
    destination = get_urls(args)

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
    for page in bq_client.list_rows(destination, page_size=args.batch).pages:
        urls = [{"dev_url": row.dev_url, "pub_url": row.pub_url} for row in page]
        task_queue.put((urls, n))
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
    parser.add_argument('--version', default=7, help='Version to work on')
    parser.add_argument('--log_dir', default=f'/mnt/disks/idc-etl/logs/copy_new_blobs_to_pub_buckets')
    parser.add_argument('--batch', default=1000)
    parser.add_argument('--processes', default=48)
    args = parser.parse_args()
    args.id = 0 # Default process ID

    if not os.path.exists('{}'.format(args.log_dir)):
        os.mkdir('{}'.format(args.log_dir))

    successlogger = logging.getLogger('root.success')
    successlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')

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


    copy_all_blobs(args)