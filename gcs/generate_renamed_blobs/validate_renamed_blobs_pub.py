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
from base64 import b64decode
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
        i_hash,
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
    ON CONCAT(uuids.trg_bucket,'/',uuids.trg_url) = dones.trg_url
    WHERE dones.trg_url IS NULL 
    ORDER BY se_uuid, uuid
    """

    query_job = client.query(query)
    result = query_job.result()  # Wait for the query to complete.
    destination = query_job.destination
    destination = client.get_table(destination)
    return destination, done

def validate_some_blobs(args, client, uuids, n):
    done = 0
    strt = time.time()
    for uuid in uuids:
        trg_bucket = client.bucket(uuid['trg_bucket'])
        trg_blob = trg_bucket.blob(uuid['trg_url'])
        tries=3
        while True:
            try:
                if trg_blob.exists():
                        trg_blob.update()
                        if b64decode(trg_blob.md5_hash).hex()  == uuid['i_hash']:
                            successlogger.info(f'{uuid["trg_bucket"]}/{uuid["trg_url"]}')
                        else:
                            errlogger.error(f'{uuid["trg_bucket"]}/{uuid["trg_url"]} hash')
                else:
                    # Skipping previously copied blob
                    errlogger.error(f'{uuid["trg_bucket"]}/{uuid["trg_url"]}')
                break
            except Exception as exc:
                if tries:
                    tries -= 1
                    continue
                else:
                    errlogger.error(f'{uuid["trg_bucket"]}/{uuid["trg_url"]} error: {exc}')
                    break
        done += 1
    delta = time.time() - strt
    progresslogger.info(f'p{args.id}: Verified {n}:{n+done-1}: {len(uuids)/delta:.2f}')
    return


def worker(input, args):
    client = storage.Client()
    try:
        for uuids, n in iter(input.get, 'STOP'):
            validate_some_blobs(args, client, uuids, n)
    except Exception as exc:
        errlogger.error(f'p{args.id}: Exiting with error: {exc}')


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
            {"i_hash": row.i_hash, "trg_bucket": row.trg_bucket, "trg_url": row.trg_url}
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
        progresslogger.info(f'Joining process: {process.name}, {process.is_alive()}')
        process.join()

    delta = time.time() - strt
    rate = (n)/delta


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='Version to work on')
    parser.add_argument('--table_id', default='idc-dev-etl.whc_dev.dones', help='BQ table into which to import dones')
    parser.add_argument('--dev_project', default=settings.DEV_PROJECT)
    parser.add_argument('--dev_dataset', default=settings.BQ_DEV_INT_DATASET)
    parser.add_argument('--dev_or_pub', default='dev', help='Dev or pub to control which blobs to copy and rename')
    parser.add_argument('--batch', default=1000)
    parser.add_argument('--processes', default=132)
    args = parser.parse_args()
    args.id = 0 # Default process ID

    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    copy_all_blobs(args)