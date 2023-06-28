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

# Copy blobs, in some bucket, having flat names to blobs having hierarchical names

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
            bigquery.SchemaField("trg_name", "STRING"),
    ]

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=0, autodetect=True, schema=schema, \
            write_disposition='WRITE_TRUNCATE'
    )

    with open(successlogger.handlers[0].baseFilename, "rb") as source_file:
        job = client.load_table_from_file(source_file, args.dones_table_id, job_config=job_config)

    job.result()  # Waits for the job to complete.

    table = client.get_table(args.dones_table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), args.dones_table_id
        )
    )
    return table.num_rows


def build_src_and_trg_table(args):
    client = bigquery.Client()
    dones = build_dones_table(args)
    query = f"""
    with alls as (
        SELECT 
        DISTINCT
        CONCAT(i_uuid,'.dcm') src_name,
        CONCAT(se_uuid,'/',i_uuid,'.dcm') trg_name
        FROM `{args.project}.{args.dataset}.all_joined` aj
        JOIN `{args.project}.{args.dataset}.all_collections` ac
        ON aj.idc_collection_id = ac.idc_collection_id
        WHERE ((i_source='tcia' AND dev_tcia_url='{args.bucket}') OR (i_source='idc' AND dev_idc_url='{args.bucket}'))
        AND aj.i_rev_idc_version < {int(settings.CURRENT_VERSION)}
        )
    SELECT alls.src_name, alls.trg_name
    FROM alls
    LEFT JOIN {args.dones_table_id} dones
    ON alls.trg_name = dones.trg_name
    WHERE dones.trg_name IS Null
    ORDER BY alls.trg_name
    """

    query_job = client.query(query)
    query_job.result()  # Wait for the query to complete.
    destination = query_job.destination
    destination = client.get_table(destination)
    return destination, dones

def copy_some_blobs(args, client, uuids, n):
    done = 0
    for uuid in uuids:
        bucket = client.bucket(args.bucket)
        src_blob = bucket.blob(uuid['src_name'])
        trg_blob = bucket.blob(uuid['trg_name'])
        for attempt in range(3):
            try:
                rewrite_token = False
                while True:
                    rewrite_token, bytes_rewritten, bytes_to_rewrite = trg_blob.rewrite(
                        src_blob, token=rewrite_token
                    )
                    if not rewrite_token:
                        break
                successlogger.info('%s', uuid['trg_name'])
                progresslogger.info(f"p{args.id}: {done+n}of{len(uuids) + n}")
                break
            except Exception as exc:
                errlogger.error(f"p{args.id}: Blob: {uuid[args.bucket]}/{uuid['src_name']}, attempt: {attempt};  {exc}")
        done += 1


def worker(input, args):
    client = storage.Client()
    for uuids, n in iter(input.get, 'STOP'):
        try:
            copy_some_blobs(args, client, uuids, n)
        except Exception as exc:
            errlogger.error(f'p{args.id}: {exc}')


def copy_all_blobs(args):
    bq_client = bigquery.Client()
    destination, dones = build_src_and_trg_table(args)
    progresslogger.info(f'Copying {destination.num_rows-dones} of {destination.num_rows}')
    # dones = set(open(f'{successlogger.handlers[0].baseFilename}').read().splitlines())

    num_processes = args.processes
    processes = []
    # Create a pair of queue for each process
    task_queue = Queue()
    # Start worker processes
    for process in range(num_processes):
        args.id = process + 1
        processes.append(
            Process(group=None, target=worker, args=(task_queue, args)))
        processes[-1].start()

    # Distribute the work across the task_queues
    n = dones
    for page in bq_client.list_rows(destination, page_size=args.batch).pages:
        uuids = [
            {"src_name": row.src_name, "trg_name": row.trg_name}
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


# if __name__ == '__main__':
#     parser = argparse.ArgumentParser()
#     parser.add_argument('--version', default=settings.CURRENT_VERSION, help='Version to work on')
#     parser.add_argument('--dones_table_id', default='idc-dev-etl.whc_dev.dones', help='BQ table into which to import dones')
#     parser.add_argument('--project', default=settings.DEV_PROJECT)
#     parser.add_argument('--dataset', default=f'idc_v{settings.CURRENT_VERSION}_dev')
#     parser.add_argument('--bucket', default='idc-dev-defaced', help='Bucket whose blobs are to be copied')
#     parser.add_argument('--batch', default=1000)
#     parser.add_argument('--processes', default=1)
#     args = parser.parse_args()
#     args.id = 0 # Default process ID
#
#     progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')
#
#     copy_all_blobs(args)