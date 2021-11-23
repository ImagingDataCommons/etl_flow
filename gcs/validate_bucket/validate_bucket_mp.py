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

"""
Multiprocess script to validate that the instances in a bucket are only
those in some set of collections
"""

import json
import os
import logging
from logging import INFO
rootlogger = logging.getLogger('root')
successlogger = logging.getLogger('success')
errlogger = logging.getLogger('root.err')

import time
from multiprocessing import Process, Queue
from google.cloud import storage, bigquery
from google.api_core.exceptions import ServiceUnavailable, NotFound

from python_settings import settings
import settings as etl_settings

settings.configure(etl_settings)
assert settings.configured

def get_blobs_in_bucket(args):
    client = bigquery.Client()
    query = f"""
        SELECT
          instance_uuid
        FROM
          `idc-dev-etl.idc_v5.auxiliary_metadata` AS aux
        JOIN
          `{args.project}.{args.bqdataset}.{args.collection_table}` AS o
        ON
          aux.tcia_api_collection_id = o.tcia_api_collection_id
        UNION ALL
        SELECT
          instance_uuid
        FROM
          `idc-dev-etl.idc_v5.retired` AS r
        JOIN
          `{args.project}.{args.bqdataset}.{args.collection_table}` AS o
        ON
          r.collection_id = o.tcia_api_collection_id
    """
    blobs = [f'{i[0]}.dcm' for i in client.query(query)]
    return blobs


def check_instances(args, client, bucket, blobs, allowed_blobs, n):
    blobs = set(blobs)
    if not blobs.issubset(allowed_blobs):
        for blob in blobs:
            if not blob in allowed_blobs:
                errlogger.error('p%s blob %s not in allowed blobs', args.id, blob)
    else:
        successlogger.info('p%s Verified %s blobs %s:%s ', args.id, args.bucket, n, n+len(blobs)-1)


def worker(input, args, allowed_blobs):
    client = storage.Client()
    bucket = storage.Bucket(client, args.bucket)
    for blobs, n in iter(input.get, 'STOP'):
        check_instances(args, client, bucket, blobs, allowed_blobs, n)


def check_all_instances(args):
    client = storage.Client()

    try:
        psql_blobs = json.load(open(args.blob_names))
    except:
        psql_blobs = get_blobs_in_bucket(args)
        json.dump(psql_blobs, open(args.blob_names), 'w')

    allowed_blobs = set(psql_blobs)

    bucket = storage.Bucket(client, args.bucket)

    print(f'Deleting bucket {args.bucket}')

    num_processes = args.processes
    processes = []

    task_queue = Queue()

    strt = time.time()

    # Start worker processes
    for process in range(num_processes):
        args.id = process + 1
        processes.append(
            Process(group=None, target=worker, args=(task_queue, args, allowed_blobs)))
        processes[-1].start()


    # Distribute the work across the task_queues
    n = 0
    page_token = ""
    # iterator = client.list_blobs(bucket, page_token=page_token, max_results=args.batch)
    iterator = client.list_blobs(bucket, versions=True, page_token=page_token, page_size=args.batch)
    for page in iterator.pages:
        blobs = [blob.name for blob in page]
        if len(blobs) == 0:
            break
        task_queue.put((blobs, n))
        # print(f'Queued {n}:{n+len(blobs)-1}')
        # task_queue.put((page, n))

        n += page.num_items
    print('Primary work distribution complete; {} blobs'.format(n))

    # Tell child processes to stop
    for i in range(num_processes):
        task_queue.put('STOP')


    # Wait for process to terminate
    for process in processes:
        # print(f'Joining process: {process.name}, {process.is_alive()}')
        process.join()

    delta = time.time() - strt
    rate = (n)/delta
    print(f'Completed bucket {args.bucket}, {rate} instances/sec, {num_processes} processes')


def pre_validate(args):

    bucket = args.bucket
    if os.path.exists('{}/logs/{}_error.log'.format(args.log_dir, bucket)):
        os.remove('{}/logs/{}_error.log'.format(args.log_dir, bucket))

    # Change logging file. File name includes bucket ID.
    for hdlr in successlogger.handlers[:]:
        successlogger.removeHandler(hdlr)
    success_fh = logging.FileHandler('{}/success.log'.format(args.log_dir))
    successlogger.addHandler(success_fh)
    successformatter = logging.Formatter('%(message)s')
    success_fh.setFormatter(successformatter)

    for hdlr in errlogger.handlers[:]:
        errlogger.removeHandler(hdlr)
    err_fh = logging.FileHandler('{}/{}_error.log'.format(args.log_dir, bucket))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    check_all_instances(args)


