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

import settings
import json
import argparse
from multiprocessing import Process, Queue
import time
from utilities.logging_config import successlogger, progresslogger, errlogger
from google.cloud import bigquery, storage

def build_dones_table(args):
    # Construct a BigQuery client object.
    client = bigquery.Client()

    schema = [
            bigquery.SchemaField("trg_url", "STRING"),
    ]

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=0, autodetect=True, schema=schema, \
            write_disposition='WRITE_TRUNCATE'
    )

    with open(successlogger.handlers[0].baseFilename, "rb") as source_file:
        job = client.load_table_from_file(source_file, args.table_id, job_config=job_config)

    job.result()  # Waits for the job to complete.

    table = client.get_table(args.table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), args.table_id
        )
    )
    return table.num_rows


def build_src_and_trg_table(args):
    client = bigquery.Client()
    done = build_dones_table(args)
    query = f"""
    with uuids AS (SELECT 
        se_uuid,
        uuid,
        REGEXP_REPLACE(dev_gcs_url, r'^gs://([a-zA-Z0-9\-]+)/([a-zA-Z0-9\-]+)/(.*)$', '\\\\1') src_bucket,
        REGEXP_REPLACE(dev_gcs_url, r'^gs://([a-zA-Z0-9\-]+)/([a-zA-Z0-9\-]+)/(.*)$', '\\\\3') src_url,
        IF(REGEXP_REPLACE({args.dev_or_pub}_gcs_url, r'^gs://([a-zA-Z0-9\-]+)/([a-zA-Z0-9\-]+)/(.*)$', '\\\\1')='public-datasets-idc', 
            'idc-open-pdp-staging', REGEXP_REPLACE({args.dev_or_pub}_gcs_url,r'^gs://([a-zA-Z0-9\-]+)/([a-zA-Z0-9\-]+)/(.*)$', '\\\\1')) trg_bucket,    
        REGEXP_REPLACE(dev_gcs_url, r'^gs://([a-zA-Z0-9\-]+)/([a-zA-Z0-9\-]+)/(.*)$', '\\\\2/\\\\3') trg_url,
    FROM `{args.dev_project}.{args.dev_dataset}.uuid_url_map_view`
    WHERE access = 'Public')
    SELECT uuids.* except(se_uuid, uuid)
    FROM uuids
    LEFT JOIN {args.table_id} dones
    ON uuids.trg_url = dones.trg_url
    WHERE dones.trg_url IS NULL 
    ORDER BY se_uuid, uuid
    """

    query_job = client.query(query)
    query_job.result()  # Wait for the query to complete.
    destination = query_job.destination
    destination = client.get_table(destination)
    return destination, done

def copy_some_blobs(args, client, uuids, n):
    done = 0
    copied = 0
    for uuid in uuids:
        # if not uuid['trg_url'] in dones:
        src_bucket = client.bucket(uuid['src_bucket'])
        src_blob = src_bucket.blob(uuid['src_url'])
        trg_bucket = client.bucket(uuid['trg_bucket'])
        trg_blob = trg_bucket.blob(uuid['trg_url'])
        for attempt in range(3):
            try:
                rewrite_token = False
                while True:
                    rewrite_token, bytes_rewritten, bytes_to_rewrite = trg_blob.rewrite(
                        src_blob, token=rewrite_token
                    )
                    if not rewrite_token:
                        break
                successlogger.info('%s', uuid['trg_url'])
                progresslogger.info(f"p{args.id}: {done+n}of{len(uuids) + n}: {uuid['src_bucket']}/{uuid['src_url']} --> {uuid['trg_bucket']}/{uuid['trg_url']}")
                copied += 1
                break
            except Exception as exc:
                errlogger.error(f"p{args.id}: Blob: {uuid['src_bucket']}/{uuid['src_bucket']}, attempt: {attempt};  {exc}")
        # else:
        #     # Skipping previously copied blob
        #     progresslogger.info(f'p{args.id}: Skipped {n+done-1}')

        done += 1
    if copied == 0:
        progresslogger.info(f'p{args.id}: Skipped {n}:{n+done-1}')


def worker(input, args):
    client = storage.Client()
    for uuids, n in iter(input.get, 'STOP'):
        copy_some_blobs(args, client, uuids, n)


def copy_all_blobs(args):
    bq_client = bigquery.Client()
    destination, done = build_src_and_trg_table(args)

    num_processes = args.processes
    processes = []
    # Create a pair of queue for each process

    task_queue = Queue()

    strt = time.time()
    # dones = set(open(f'{successlogger.handlers[0].baseFilename}').read().splitlines())

    # Start worker processes
    for process in range(num_processes):
        args.id = process + 1
        processes.append(
            Process(group=None, target=worker, args=(task_queue, args)))
        processes[-1].start()

    # Distribute the work across the task_queues
    n = done
    for page in bq_client.list_rows(destination, page_size=args.batch).pages:
        uuids = [
            {"src_bucket": row.src_bucket, "src_url": row.src_url, "trg_bucket": row.trg_bucket, "trg_url": row.trg_url}
            for row in page]
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
    parser.add_argument('--table_id', default='idc-dev-etl.whc_dev.dones', help='BQ table into which to import dones')
    parser.add_argument('--dev_project', default=settings.DEV_PROJECT)
    parser.add_argument('--dev_dataset', default=settings.BQ_DEV_INT_DATASET)
    parser.add_argument('--dev_or_pub', default='pub', help='Dev or pub to control which blobs to copy and rename')
    parser.add_argument('--batch', default=1000)
    parser.add_argument('--processes', default=1)
    args = parser.parse_args()
    args.id = 0 # Default process ID

    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    copy_all_blobs(args)