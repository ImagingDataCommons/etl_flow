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


# Generate a manifest of uuids that are new to a version.
# The resulting manifest is intended to be submitted to
# DCF to detect if there are collisions with other uuids.

import argparse
import settings
from google.cloud import bigquery
from utilities.bq_helpers import query_BQ, export_BQ_to_GCS, delete_BQ_Table
import requests
from time import  time,sleep
from utilities.logging_config import successlogger, progresslogger,errlogger, warninglogger
from multiprocessing import Process, Queue


def worker(input, argss):
    # proglogger.info('p%s: Worker starting: args: %s', args.id, args )
    # print(f'p{args.id}: Worker starting: args: {args}')

    MAXTTRIES = 5

    for uuids in iter(input.get, 'STOP'):
        for uuid in uuids:
            retry = MAXTTRIES
            while retry:
                res = requests.get(f'https://nci-crdc.datacommons.io/ga4gh/drs/v1/objects/dg.4DFC/{uuid}')
                # res = requests.get(f'https://nci-crdc.datacommons.io/index/{uuid}')
                if res.status_code == 404:
                    successlogger.info(uuid)
                    break
                elif res.status_code == 200:
                    errlogger.error(uuid)
                    break
                else:
                    progresslogger.info(f'{uuid}: {res.text}, tried={MAXTTRIES - retry + 1}')
                    sleep(pow(2,MAXTTRIES - retry + 1))
                    retry -= 1
            if retry == 0:
                warninglogger.info(uuid)


def gen_revision_manifest(args):
    BQ_client = bigquery.Client()
    query= f"""
        SELECT uuid
        FROM `idc-dev-etl.idc_v{args.version}_dev.instance` 
        WHERE rev_idc_version = {args.version} AND excluded=False AND redacted=False
        ORDER BY uuid
        LIMIT {args.test_size}

    """

    # # Run a query that generates the manifest data
    # results = query_BQ(BQ_client, args.dst_bqdataset, args.temp_table, query, write_disposition='WRITE_TRUNCATE')

    # Run a query that generates the manifest data
    uuids = [row['uuid'] for row in BQ_client.query(query)]

    num_processes = int(min(args.processes, args.test_size/args.per_process_batch))
    processes = []
    task_queue = Queue()

    # Start worker processes
    for process in range(num_processes):
        args.id = process + 1
        processes.append(
            Process(group=None, target=worker, args=(task_queue, args)))
        processes[-1].start()

    start = time()

    # Distribute the work across the task_queues
    n = 0
    while n < len(uuids):
        task_queue.put(uuids[n: n+args.per_process_batch])
        n += args.per_process_batch

    progresslogger.info('Primary work distribution complete; {} blobs'.format(n))

    # Tell child processes to stop
    for i in range(num_processes):
        task_queue.put('STOP')
        task_queue.put('STOP')

    # Wait for process to terminate
    for process in processes:
        print(f'Joining process: {process.name}, {process.is_alive()}')
        process.join()

    elapsed = time() - start
    progresslogger.info(f'Test size: {args.test_size}, Processes: {num_processes}')
    progresslogger.info(f'Elapsed time: {elapsed}s')
    progresslogger.info(f'Overall effective rate: {args.test_size/elapsed}/s')
    progresslogger.info(f'Per-process effective rate: {args.test_size/elapsed/num_processes}/s')

if __name__ == '__main__':
    version = settings.CURRENT_VERSION
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=version)
    parser.add_argument('--processes', default = 24)
    parser.add_argument('--per_process_batch', default=100)
    parser.add_argument('--test_size', default=12800)
    args = parser.parse_args()

    gen_revision_manifest(args)