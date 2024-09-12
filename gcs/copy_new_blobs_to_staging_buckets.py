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
import argparse
import settings
from google.cloud import bigquery, storage
import time
from multiprocessing import Process, Queue
from utilities.logging_config import successlogger, progresslogger, errlogger

# Copy the blobs that are new to a version from dev pre-staging buckets
# to idc-pdp-staging staging buckets .

# Get the source and target URLS
def get_urls(args):
    client = bigquery.Client()
    query = f"""
    SELECT
      dev.gcs_url as dev_url,
      pub.gcs_url as pub_url
    FROM
      `idc-dev-etl.idc_v{args.version}_pub.auxiliary_metadata` dev
    JOIN
      `idc-pdp-staging.idc_v{args.version}.auxiliary_metadata` pub
    ON
      dev.instance_uuid = pub.instance_uuid
    WHERE
      dev.series_revised_idc_version = {args.version} 
    ORDER BY dev_url
    """
    # urls = list(client.query(query))
    query_job = client.query(query)  # Make an API request.
    query_job.result()  # Wait for the query to complete.
    destination = query_job.destination
    destination = client.get_table(destination)
    return destination


def copy_some_blobs(args, client, urls, n, dones):
    done = 0
    copied = 0
    for blob in urls:
        blob_name = '/'.join(blob['dev_url'].split('/')[3:])
        if not blob_name in dones:
            dev_bucket_name=blob['dev_url'].split('/')[2]
            dev_bucket = client.bucket(dev_bucket_name)
            dev_blob = dev_bucket.blob(blob_name)
            pub_bucket_name = blob['pub_url'].split('/')[2]

            # We don't copy directly to the public-datasets-idc bucket.
            # We copy to a staging bucket and Google copies to the public bucket
            if 'public-datasets-idc' in pub_bucket_name:
                pub_bucket_name = 'public-datasets-idc-staging'
            elif 'idc-open-cr' in pub_bucket_name:
                pub_bucket_name = 'idc-open-cr-staging'
            elif 'idc-open-idc1' in pub_bucket_name:
                pub_bucket_name = 'idc-open-idc1-staging'
            else:
                errlogger.error(f'Unrecognized destination bucket name: {pub_bucket_name}')
                exit
            pub_bucket = client.bucket(pub_bucket_name)
            pub_blob = pub_bucket.blob(blob_name)
            for attempt in range(3):
                try:
                    rewrite_token = False
                    while True:
                        rewrite_token, bytes_rewritten, bytes_to_rewrite = pub_blob.rewrite(
                            dev_blob, token=rewrite_token
                        )
                        if not rewrite_token:
                            break
                    successlogger.info('%s', blob_name)
                    progresslogger.info(f'p{args.id}: {done+n}of{len(urls)+n}: {dev_bucket_name}/{blob_name} --> {pub_bucket_name}/{blob_name}')
                    break
                except Exception as exc:
                    errlogger.error('p%s: Blob: %s, attempt: %s;  %s', args.id, blob_name, attempt, exc)
                    time.sleep(5)
            if range == 0:
                errlogger.error('p%s: Blob: %s, copy failed; {exc}', args.id, blob_name, exc)

        done += 1
    if copied == 0:
        progresslogger.info(f'p{args.id}: Skipped {n}:{n+done-1}')


def worker(input, args, dones):
    client = storage.Client()
    for urls, n in iter(input.get, 'STOP'):
        try:
            copy_some_blobs(args, client, urls, n, dones)
        except Exception as exc:
            errlogger.error(f'p{args.id}: In worker: {exc}')


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
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='Version to work on')
    parser.add_argument('--batch', default=1000)
    parser.add_argument('--processes', default=8 )
    args = parser.parse_args()
    args.id = 0 # Default process ID

    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    copy_all_blobs(args)