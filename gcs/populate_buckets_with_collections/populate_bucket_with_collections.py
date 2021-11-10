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
General purpose multiprocessing routine to copy the instances of some set of
collections from one bucket to another.

A parameter, args.retired, controls whether retired instances (as listed
in the retired table) are copied.
In general args.retired should be False when populating a bucket that will
imported into a DICOM store, where (only current instances are wanted.
args.retired should be Tree when populating a bucket that will be public
(even if having Limited access) as well as the dev counterparts of these
buckets.

The inital use of this routine was in splitting idc_dev into multiple
buckets in support of Google hosting and defacing.

It can also be used to copy data among those buckets as needed, for example,
when the set of collections to be defaced/redacted changes. Note that, for
this purpose the module does not delete the source blob. That should be done
separately.
"""

import argparse
import os
import logging

rootlogger = logging.getLogger('root')
successlogger = logging.getLogger('success')
errlogger = logging.getLogger('root.err')

import time
from multiprocessing import Process, Queue
from google.cloud import storage, bigquery

from python_settings import settings
import settings as etl_settings

settings.configure(etl_settings)
assert settings.configured

TRIES = 3
"""
args paramaters
bqdataset_name: bq datas et from which to access tables
bq_collections_table': BQ table listing group of collections to be populate
retired: Copy retired instances in collection if True
src_bucket: Bucket from which to copy blobs
dst_bucket, Bucket to which to copy blobs
processes: Number of concurrent processes
batch: Size of batch of blobs to be copied
src_project: Project of destination bucket
dst_project: Project of source bucket
log_dir: Directory in which some log files are kept.
dones: File listing collections that have been copied
"""


def get_collections_in_version(args):
    client = bigquery.Client()
    query = ""
    if 'excluded_tables' in args.__dict__ and args.excluded_tables:
        query = f"""
            WITH ex as
            ({query}
                SELECT tcia_api_collection_id
                FROM `{args.src_project}.{args.bqdataset_name}.{args.excluded_tables[0]}` 
            """
        for table in args.excluded_tables[1:]:
            query = f"""
                {query}
                UNION ALL
                SELECT tcia_api_collection_id
                FROM `{args.src_project}.{args.bqdataset_name}.{table}` 
                """
        query = f"""
            {query})
            """
        query = f"""
            {query}
            SELECT c.* 
            FROM `idc-dev-etl.idc_v5.collection` AS c
            LEFT JOIN ex
            ON c.collection_id =ex.tcia_api_collection_id
            where ex.tcia_api_collection_id is NULL

            ORDER BY c.collection_id 
            """
        result = client.query(query).result()
        collection_ids = [collection['collection_id'] for collection in result]
    else:
        query = f"""
            SELECT c.* 
            FROM `idc-dev-etl.idc_v5.{args.bq_collections_table}` AS c
            where v1=False and v2=True
            ORDER BY c.tcia_api_collection_id 
            """
        result = client.query(query).result()
        collection_ids = [collection['tcia_api_collection_id'] for collection in result]
    return collection_ids


def copy_instances(args, rows, n, rowcount, done_instances, src_bucket, dst_bucket):
    for row in rows:
        index = f'{n}/{rowcount}'
        blob_name = f'{row}.dcm'
        if not blob_name in done_instances:
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

                    # blob_copy = src_bucket.copy_blob(src_bucket.blob(blob_name), dst_bucket)
                    # # rootlogger.info('%s %s: %s: copy succeeded %s', args.id, index, args.collection, blob_name)
                    # successlogger.info(f'{blob_name}')
                    # break

                except Exception as exc:
                    if retries == TRIES:
                        errlogger.error('p%s %s: %s: copy failed %s\n, retry %s; %s', args.id,
                                        index, args.collection,
                                        blob_name, retries, exc)
                        break
                time.sleep(retries)
                retries += 1
            if n % args.batch == 0:
                rootlogger.info('p%s %s: %s', args.id, index, args.collection)
        else:
            if n % args.batch == 0:
                rootlogger.info('p%s %s: %s: skipping blob %s ', args.id, index, args.collection, blob_name)
        n += 1


def worker(input, args, done_instances):
    # rootlogger.info('p%s: Worker starting: args: %s', args.id, args )
    # print(f'p{args.id}: Worker starting: args: {args}')

    client = storage.Client()
    src_bucket = client.bucket(args.src_bucket, user_project=args.src_project)
    dst_bucket = client.bucket(args.dst_bucket, user_project=args.dst_project)

    for rows, n, rowcount in iter(input.get, 'STOP'):
        copy_instances(args, rows, n, rowcount, done_instances, src_bucket, dst_bucket)
        # output.put(n)


def copy_all_instances(args):
    client = bigquery.Client()
    try:
        # Create a set of previously copied blobs
        done_instances = set(open(f'{args.log_dir}/{args.collection}_success.log').read().splitlines())
    except:
        done_instances = []

    # We first copy the instances in the current IDC version,

    # Query to get the instances in the collection
    query = f"""
        SELECT i.uuid
        FROM `idc-dev-etl.idc_v{args.version}.collection` as c 
        JOIN `idc-dev-etl.idc_v{args.version}.patient` as p
        ON c.collection_id = p.collection_id
        JOIN `idc-dev-etl.idc_v{args.version}.study` as st
        ON p.submitter_case_id = st.submitter_case_id
        JOIN `idc-dev-etl.idc_v{args.version}.series` as se
        ON st.study_instance_uid = se.study_instance_uid
        JOIN `idc-dev-etl.idc_v{args.version}.instance` as i
        ON se.series_instance_uid = i.series_instance_uid
        WHERE c.collection_id = '{args.collection}'
        ORDER by i.uuid
        """
    args.id = 0

    increment = args.batch
    # cur.execute(query)
    query_job = client.query((query))
    query_job.result()
    # Get the destination table for the query results.
    #
    # All queries write to a destination table. If a destination table is not
    # specified, the BigQuery populates it with a reference to a temporary
    # anonymous table after the query completes.
    destination = query_job.destination

    # Get the schema (and other properties) for the destination table.
    #
    # A schema is useful for converting from BigQuery types to Python types.
    destination = client.get_table(destination)

    prowcount = destination.num_rows
    print(f'Copying collection {args.collection}; primary {prowcount} instances')

    num_processes = max(1, min(args.processes, int(prowcount / increment)))
    processes = []
    # Create a pair of queue for each process

    task_queue = Queue()

    # task_queues = [Queue() for p in range(num_processes)]
    # done_queues = [Queue() for p in range(num_processes)]

    strt = time.time()

    # Start worker processes
    for process in range(num_processes):
        args.id = process + 1
        processes.append(
            Process(group=None, target=worker, args=(task_queue, args, done_instances)))
        # processes.append(
        #     Process(group=None, target=worker, args=(task_queues[process], args, done_instances)))
        # print(f'Started process {args.id}: {processes[-1]}')
        processes[-1].start()

    # Distribute the work across the task_queues
    n = 1
    while True:
        # rows = cur.fetchmany(increment)
        rows = [r.uuid for r in client.list_rows(destination, max_results=increment, start_index=n - 1)]
        if len(rows) == 0:
            break
        task_queue.put((rows, n, prowcount))
        n += increment
    print('Primary work distribution complete')

    # Next we copy retired instances
    # Query to get the instances from the retired table
    if args.retired:
        query = f"""
            SELECT r.instance_uuid
            FROM `idc-dev-etl.idc_v{args.version}.retired` as r 
            WHERE r.collection_id = '{args.collection}'
            ORDER by r.instance_uuid
            """

        query_job = client.query((query))
        query_job.result()
        # Get the destination table for the query results.
        #
        # All queries write to a destination table. If a destination table is not
        # specified, the BigQuery populates it with a reference to a temporary
        # anonymous table after the query completes.
        destination = query_job.destination

        # Get the schema (and other properties) for the destination table.
        #
        # A schema is useful for converting from BigQuery types to Python types.
        destination = client.get_table(destination)

        rrowcount = destination.num_rows
        if rrowcount:
            print(f'Copying retired {args.collection}; primary {rrowcount} instances')

            # Distribute the work across the task_queues
            n = 1
            while True:
                # rows = cur.fetchmany(increment)
                rows = [r.instance_uuid for r in
                        client.list_rows(destination, max_results=increment, start_index=n - 1)]
                if len(rows) == 0:
                    break
                task_queue.put((rows, n, rrowcount))
                n += increment
            print('Retired work distribution complete')
        else:
            print(f'No retired instances in collection {args.collection}')
    else:
        rrowcount = 0

    # Tell child processes to stop
    for i in range(num_processes):
        task_queue.put('STOP')

    # Wait for process to terminate
    for process in processes:
        print(f'Joining process: {process.name}, {process.is_alive()}')
        process.join()

    delta = time.time() - strt
    rate = (prowcount + rrowcount) / delta
    print(f'Completed collection {args.collection}, {rate} instances/sec, {num_processes} processes')


def precopy(args):
    client = bigquery.Client()
    collections = get_collections_in_version(args)

    if not os.path.exists('{}'.format(args.log_dir)):
        os.mkdir('{}'.format(args.log_dir))
        st = os.stat('{}'.format(args.log_dir))
        os.chmod('{}'.format(args.log_dir), st.st_mode | 0o222)

    try:
        dones = open(args.dones).read().splitlines()
    except:
        dones = []
    for collection in collections:
        if not collection in dones:
            args.collection = collection
            if os.path.exists('{}/logs/{}_error.log'.format(args.log_dir, collection)):
                os.remove('{}/logs/{}_error.log'.format(args.log_dir, collection))

            # Change logging file. File name includes collection ID.
            for hdlr in successlogger.handlers[:]:
                successlogger.removeHandler(hdlr)
            success_fh = logging.FileHandler('{}/{}_success.log'.format(args.log_dir, collection))
            successlogger.addHandler(success_fh)
            successformatter = logging.Formatter('%(message)s')
            success_fh.setFormatter(successformatter)

            for hdlr in errlogger.handlers[:]:
                errlogger.removeHandler(hdlr)
            err_fh = logging.FileHandler('{}/{}_error.log'.format(args.log_dir, collection))
            errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
            errlogger.addHandler(err_fh)
            err_fh.setFormatter(errformatter)

            copy_all_instances(args)

            if not os.path.isfile('{}/logs/cc_{}_error.log'.format(args.log_dir, collection)) or os.stat(
                    '{}/logs/cc_{}_error.log'.format(os.environ['PWD'], collection)).st_size == 0:
                # If no errors, then we are done with this collection
                with open(args.dones, 'a') as f:
                    f.write(f'{collection}\n')



