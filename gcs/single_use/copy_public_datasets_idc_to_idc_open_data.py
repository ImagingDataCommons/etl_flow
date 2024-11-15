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

import pandas
from google.cloud import bigquery, storage
import time
from multiprocessing import Process, Queue
from utilities.logging_config import successlogger, progresslogger, errlogger

import settings

# The query should return a table with a single column, 'blobs'
# basically <instance_uuid>.dcm
def get_urls(args):
    client = bigquery.Client()

    table_id = f'idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.temp_dones_whc'

    job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        schema=[
            bigquery.SchemaField("blob_name", bigquery.enums.SqlTypeNames.STRING),
        ],
        write_disposition="WRITE_TRUNCATE",
    )
    with open(f'{successlogger.handlers[0].baseFilename}', 'rb') as f:
        job = client.load_table_from_file(f, 'idc-dev-etl.idc_v20_dev.temp_dones_whc', job_config=job_config)
    # job = client.load_table_from_dataframe(df, table_id, job_config=job_config
    job.result()

    query = f"""
with alll as (SELECT distinct concat(se_uuid,'/', i_uuid, '.dcm') as blob_name
    FROM `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.all_joined_public` aj
    WHERE ((i_source='tcia' AND tcia_access='Public') OR (i_source='idc' AND idc_access='Public')) 
    AND i_excluded=FALSE AND i_redacted=FALSE
    AND ((i_source='tcia' and pub_gcs_tcia_url='public-datasets-idc')
    OR (i_source='idc' AND pub_gcs_idc_url='public-datasets-idc'))
)
SELECT *
FROM alll
LEFT JOIN `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.temp_dones_whc` tdw
ON alll.blob_name = tdw.blob_name WHERE tdw.blob_name IS NULL
      """

    query_job = client.query(query)  # Make an API request.
    query_job.result()  # Wait for the query to complete.
    destination = query_job.destination
    destination = client.get_table(destination)
    return destination

'tem_'
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
                    errlogger.error('p%s: %s/%s copy failed\n   %s', args.id, args.src_bucket, blob_name, exc)
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
        #
        # blob_names_todo = sorted(list(set(blob_names) - dones))
        # if blob_names_todo:
        #     copy_instances(args, client, src_bucket, dst_bucket, blob_names_todo, n)
        # else:
        #     progresslogger.info(f'p{args.id}: Blobs {n}:{n+len(blob_names)-1} previously copied')

# Copy the blobs resulting from the BQ query
# args must have the following components:
# version: Version to work on)
# src_bucket: Bucket from which to copy)
# dst_bucket: Bucket to which to copy)
# batch: Batch sizw to workers)
# processes: Number of processes to run)

def copy_all_blobs(args):
    bq_client = bigquery.Client()
    # dones = set(open(f'{successlogger.handlers[0].baseFilename}').read().splitlines())
    destination = get_urls(args)

    strt = time.time()
    num_processes = args.processes
    processes = []
    task_queue = Queue()
    # Start worker processes
    for process in range(num_processes):
        args.id = process + 1
        processes.append(
            Process(group=None, target=worker, args=(task_queue, args)))
        processes[-1].start()

    # Distribute the work across the task_queues
    n = 0
    for page in bq_client.list_rows(destination, page_size=args.batch).pages:
        urls = [row.blob_name for row in page]
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
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='Version to work on')
    # parser.add_argument('--log_dir', default=f'{settings.LOGGING_BASE}/{settings.BASE_NAME}')
    parser.add_argument('--batch', default=1000)
    parser.add_argument('--processes', default=1)
    parser.add_argument('--src_bucket', default='public-datasets-idc')
    parser.add_argument('--dst_bucket', default='idc-open-data')
    args = parser.parse_args()
    args.id = 0 # Default process ID

    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')



    copy_all_blobs(args)

