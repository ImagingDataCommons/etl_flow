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
# import argparse
from multiprocessing import Process, Queue, Lock
from utilities.logging_config import successlogger, progresslogger, errlogger
from google.cloud import bigquery, storage


def get_some_found_renamed_blobs(args, lock, prefix):
    with open(f'{settings.LOG_DIR}/found_blobs','a') as f:
        blobs = set()
        storage_client = storage.Client()
        iterator = storage_client.list_blobs(args.bucket, prefix=prefix, delimiter=args.delimiter)
        # iterator = storage_client.list_blobs('idc-dev-defaced', delimiter='/')
        if args.delimiter:
            for page in iterator.pages:
                # if page.prefixes:
                    # series = series.union(set([series for series in page.prefixes]))
                for prefix in page.prefixes:
                    blobs_iterator = storage_client.list_blobs(args.bucket, prefix=prefix)
                    for blob_page in blobs_iterator.pages:
                        blob_names = [f'{blob.name}' for blob in blob_page]
                        blobs = blobs.union(set(blob_names))
                        blob_name_string = [f'{blob}\n' for blob in blob_names]
                        lock.acquire()
                        try:
                            f.write(''.join(blob_name_string))
                        finally:
                            lock.release()
        else:
            for blob_page in iterator.pages:
                blob_names = [f'{blob.name}' for blob in blob_page]
                blobs = blobs.union(set(blob_names))
                blob_name_string = [f'{blob}\n' for blob in blob_names]
                lock.acquire()
                try:
                    f.write(''.join(blob_name_string))
                finally:
                    lock.release()

            # else:
            #     break

def worker(input, args, lock):
    client = storage.Client()
    for prefix in iter(input.get, 'STOP'):
        try:
            get_some_found_renamed_blobs(args, lock, prefix)
        except Exception as exc:
            errlogger.error(f'p{args.id}, {prefix}: {exc}')


def get_found_renamed_blobs(args):
    num_processes = args.processes
    processes = []
    task_queue = Queue()
    lock = Lock()

    # Start worker processes
    for process in range(num_processes):
        args.id = process + 1
        processes.append(
            Process(group=None, target=worker, args=(task_queue, args, lock)))
        processes[-1].start()

    # Distribute the work across the task_queues
    for i in range(256):
        prefix = ("%0.2X"%i).lower()
        task_queue.put((prefix))
        # print(f'Queued {n}:{n+args.batch-1}')
    print('Primary work distribution complete')

    # Tell child processes to stop
    for i in range(2 * num_processes):
        task_queue.put('STOP')

    # Wait for process to terminate
    for process in processes:
        progresslogger.info(f'Joining process: {process.name}, {process.is_alive()}')
        process.join()

def get_expected_renamed_blobs(args):
    client = bigquery.Client()
    # dones = build_dones_table(args)
    query = f"""
    SELECT 
    DISTINCT
    CONCAT(se_uuid,'/',i_uuid,'.dcm') trg_name
    FROM `{args.project}.{args.dataset}.all_joined` aj
    JOIN `{args.project}.{args.dataset}.all_collections` ac
    ON aj.idc_collection_id = ac.idc_collection_id
    WHERE ((i_source='tcia' AND dev_tcia_url='{args.bucket}') OR (i_source='idc' AND dev_idc_url='{args.bucket}'))
    AND aj.i_rev_idc_version < {int(settings.CURRENT_VERSION)}
    """

    query_job = client.query(query)
    query_job.result()  # Wait for the query to complete.
    destination = query_job.destination
    destination = client.get_table(destination)

    blobs = []
    with open(f'{settings.LOG_DIR}/expected_blobs', 'w') as f:
        for page in client.list_rows(destination, page_size=args.batch).pages:
            blob_names = [ row.trg_name for row in page]
            blobs.extend(blob_names)
            blob_name_string = [f'{blob}\n' for blob in blob_names]
            f.write(''.join(blob_name_string))
    return set(blobs)


def validate_renamed_blobs_mp(args):
    if args.read_found_file:
        try:
            found_renamed_blobs = set(open(f'{settings.LOG_DIR}/found_blobs').read().splitlines())
        except:
            get_found_renamed_blobs(args)
            found_renamed_blobs = set(open(f'{settings.LOG_DIR}/found_blobs').read().splitlines())
    else:
        get_found_renamed_blobs(args)
        found_renamed_blobs = set(open(f'{settings.LOG_DIR}/found_blobs').read().splitlines())

    if args.read_expected_file:
        try:
            expected_renamed_blobs = set(open(f'{settings.LOG_DIR}/expected_blobs').read().splitlines())
        except:
            expected_renamed_blobs = get_expected_renamed_blobs(args)
    else:
        expected_renamed_blobs = get_expected_renamed_blobs(args)

    if found_renamed_blobs == expected_renamed_blobs:
        successlogger.info(f'Success')
    else:
        errlogger.error(f'Unfound blobs:')
        for blob in expected_renamed_blobs - found_renamed_blobs:
            errlogger.error(blob)
        errlogger.error(f'\n\n\n\nUnexpected blobs:')
        for blob in found_renamed_blobs - expected_renamed_blobs:
            errlogger.error(blob)



# if __name__ == '__main__':
#     parser = argparse.ArgumentParser()
#     parser.add_argument('--read_found_file', default=True, help='Read found blobs names from a file if True')
#     parser.add_argument('--read_expected_file', default=True, help='Read expected blobs names from a file if True')
#     parser.add_argument('--bucket', default='idc-dev-defaced', help='Bucket to validate')
#     parser.add_argument('--project', default=settings.DEV_PROJECT)
#     parser.add_argument('--dataset', default=f'idc_v{settings.CURRENT_VERSION}_dev')
#     parser.add_argument('--batch', default=1000)
#     args = parser.parse_args()
#     args.id = 0 # Default process ID
#
#     progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')
#
#     validate_renamed_blobs(args)