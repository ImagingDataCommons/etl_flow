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
from utilities.logging_config import successlogger, progresslogger, errlogger
from google.cloud import bigquery, storage
import time
from multiprocessing import Process, Queue

# Copy the blobs that are new to a version from dev pre-staging buckets
# to dev staging buckets.

def get_urls(args):
    client = bigquery.Client()
    query = f"""
    SELECT
      distinct
      collection_id,
      st_uuid,
      se_uuid,
      i_uuid,
      series_instance_uid,
      sop_instance_uid
    FROM
      `idc-dev-etl.idc_v{args.version}_dev.all_joined`
    WHERE
      collection_id in {args.collections} 
      OR (collection_id='TCGA-READ' and i_source='tcia')
      OR (collection_id='TCGA-ESCA' and i_source='tcia')
    """
    # urls = list(client.query(query))
    query_job = client.query(query)  # Make an API request.
    query_job.result()  # Wait for the query to complete.
    destination = query_job.destination
    destination = client.get_table(destination)
    return destination

def copy_some_blobs(args, client, dones, metadata, n):
     for blob in metadata:
        src_blob_name = f"{blob['i_uuid']}.dcm"
        if args.hfs_level == 'series':
            dst_blob_name = f"{blob['se_uuid']}/{blob['i_uuid']}.dcm"
        else:
            dst_blob_name = f"{blob['st_uuid']}/{blob['se_uuid']}/{blob['i_uuid']}.dcm"
        if not src_blob_name in dones:
            src_bucket_name='idc-dev-open'
            src_bucket = client.bucket(src_bucket_name)
            src_blob = src_bucket.blob(src_blob_name)
            dst_bucket_name = args.dst_bucket
            dst_bucket = client.bucket(dst_bucket_name)
            dst_blob = dst_bucket.blob(dst_blob_name)

            # Large blobs need to special handling
            try:
                rewrite_token = False
                while True:
                    rewrite_token, bytes_rewritten, bytes_to_rewrite = dst_blob.rewrite(
                        src_blob, token=rewrite_token
                    )
                    if not rewrite_token:
                        break
                successlogger.info('%s', src_blob_name)
                print(f'p{args.id}: {n}of{len(metadata)}: {src_bucket_name}/{src_blob_name} --> {dst_bucket_name}/{dst_blob_name}')
            except Exception as exc:
                errlogger.error('p%s: %sof%s Blob: %s: %s', args.id, n, len(metadata), src_blob_name, exc)
        n += 1


def worker(input, args, dones):
    # proglogger.info('p%s: Worker starting: args: %s', args.id, args )
    # print(f'p{args.id}: Worker starting: args: {args}')

    RETRIES = 3

    client = storage.Client()
    for metadata, n in iter(input.get, 'STOP'):
        copy_some_blobs(args, client, dones, metadata, n)


def copy_all_blobs(args):
    # try:
    #     # dones = open(f'{args.log_dir}/success.log').read().splitlines()
    #     dones = open(successlogger.handlers[0].baseFilename).read().splitlines()
    # except:
    #     dones = []
    dones = []

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
            Process(group=None, target=worker, args=(task_queue, args, dones)))
        processes[-1].start()

    # Distribute the work across the task_queues
    n = 0
    for page in bq_client.list_rows(destination, page_size=args.batch).pages:
        metadata = [{"collection_id": row.collection_id, "st_uuid": row.st_uuid, "se_uuid": row.se_uuid, "series_instance_uid": row.series_instance_uid, "sop_instance_uid": row.sop_instance_uid, "i_uuid": row.i_uuid} for row in page]
        task_queue.put((metadata, n))
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


# if __name__ == '__main__':
#     parser = argparse.ArgumentParser()
#     parser.add_argument('--version', default=8, help='Version to work on')
#     parser.add_argument('--log_dir', default=f'/mnt/disks/idc-etl/logs/copy_and_rename_instances')
#     parser.add_argument('--collections', default="('APOLLO-5-LSCC', 'CPTAC-SAR')")
#     parser.add_argument('--hfs_level', default='series',help='Name blobs as study/series/instance if study, series/instance if series')
#     parser.add_argument('--src_bucket', default='idc-dev-open', help='Bucket from which to copy blobs')
#     parser.add_argument('--dst_bucket', default='whc_series_instance', help='Bucket into which to copy blobs')
#     parser.add_argument('--batch', default=100)
#     parser.add_argument('--processes', default=16)
#     args = parser.parse_args()
#     args.id = 0 # Default process ID
#
#     copy_all_blobs(args)
