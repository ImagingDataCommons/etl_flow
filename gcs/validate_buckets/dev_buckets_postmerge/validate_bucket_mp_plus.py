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
Validate that a bucket holds the correct set of instance blobs
"""
import settings
import json
import builtins
# Noramlly the progresslogger file is trunacated. The following causes it to be appended.
# builtins.APPEND_PROGRESSLOGGER = True
from utilities.logging_config import successlogger, progresslogger, errlogger
from google.cloud import storage, bigquery
from multiprocessing import Process, Queue
from base64 import b64decode
import logging
from logging import INFO, ERROR
from utilities.bq_helpers import load_BQ_from_json, load_BQ_from_CSV


prestageblobs = logging.getLogger('root.prestageblobs')
prestageblobs.setLevel(INFO)
for hdlr in prestageblobs.handlers[:]:
    errlogger.removeHandler(hdlr)
prestage_fh = logging.FileHandler('{}/prestage_blobs.log'.format(settings.LOG_DIR))
errformatter = logging.Formatter('%(message)s')
prestageblobs.addHandler(prestage_fh)
prestage_fh.setFormatter(errformatter)

prestageseries = logging.getLogger('root.prestageseries')
prestageseries.setLevel(INFO)
for hdlr in prestageseries.handlers[:]:
    errlogger.removeHandler(hdlr)

prestage_fh = logging.FileHandler('{}/prestage_series.log'.format(settings.LOG_DIR))
errformatter = logging.Formatter('%(message)s')
prestageseries.addHandler(prestage_fh)
prestage_fh.setFormatter(errformatter)

foundblobs = logging.getLogger('root.foundblobs')
foundblobs.setLevel(INFO)
for hdlr in foundblobs.handlers[:]:
    errlogger.removeHandler(hdlr)
prestage_fh = logging.FileHandler('{}/found_blobs.log'.format(settings.LOG_DIR))
errformatter = logging.Formatter('%(message)s')
foundblobs.addHandler(prestage_fh)
prestage_fh.setFormatter(errformatter)

foundseries = logging.getLogger('root.foundseries')
foundseries.setLevel(INFO)
for hdlr in foundseries.handlers[:]:
    errlogger.removeHandler(hdlr)
prestage_fh = logging.FileHandler('{}/found_series.log'.format(settings.LOG_DIR))
errformatter = logging.Formatter('%(message)s')
foundseries.addHandler(prestage_fh)
prestage_fh.setFormatter(errformatter)


def worker(input, args, blob_logger, series_logger, dones):
    # proglogger.info('p%s: Worker starting: args: %s', args.id, args )
    # print(f'p{args.id}: Worker starting: args: {args}')

    RETRIES=3

    client = storage.Client()
    # bucket = client.bucket(args.bucket)

    for bucket, prefixes, n in iter(input.get, 'STOP'):
        try:
            for prefix in prefixes:
                if prefix not in dones:
                    instance_iterator = client.list_blobs(bucket, versions=False, page_size=args.batch, \
                                         prefix=prefix)
                    for page in instance_iterator.pages:
                        for blob in page:
                            blob_logger.info(f'{blob.name}, {bucket}, {b64decode(blob.md5_hash).hex()}')
                    series_logger.info(prefix)
            # if blob_names_todo:
            #     copy_instances(args, client, src_bucket, dst_bucket, blob_names_todo, n)
            # else:
            #     progresslogger.info(f'p{args.id}: Blobs {n}:{n+len(blob_names)-1} previously copied')
        except Exception as exc:
            errlogger.error(f'p{args.id}: Error {exc}')

def get_expected_blobs_in_bucket(args, premerge=False):
    client = bigquery.Client()
    query = f"""
      SELECT distinct concat(se_uuid,'/', i_uuid, '.dcm') as blob_name
      FROM `idc-dev-etl.idc_v{args.version}_dev.all_joined` aj
      JOIN `idc-dev-etl.idc_v{args.version}_dev.all_collections` aic
      ON aj.idc_collection_id = aic.idc_collection_id
      WHERE ((i_source='tcia' and aic.{"dev" if args.dev_or_pub=="dev" else "pub_gcs"}_tcia_url="{args.bucket}")
      OR (i_source='idc' and aic.{"dev" if args.dev_or_pub=="dev" else "pub_gcs"}_idc_url="{args.bucket}"))
      AND i_excluded = False
      AND if({premerge}, i_rev_idc_version < {args.version}, i_rev_idc_version <= {args.version})
      ORDER BY blob_name
  """

    query_job = client.query(query)  # Make an API request.
    query_job.result()  # Wait for the query to complete.

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
    with open(args.expected_blobs, 'w') as f:
        for page in client.list_rows(destination, page_size=args.batch).pages:
            rows = [f'{row["blob_name"]}\n' for row in page]
            f.write(''.join(rows))


def get_found_blobs_in_bucket(args):
    client = storage.Client()
    bucket = client.bucket(args.bucket)
    page_token = ""
    # Get the completed series
    done_series = open(f'{foundseries.handlers[0].baseFilename}').read().splitlines()

    # Start worker processes
    num_processes = args.processes
    processes = []
    task_queue = Queue()
    for process in range(num_processes):
        args.id = process + 1
        processes.append(
            Process(group=None, target=worker, args=(task_queue, args, foundblobs, foundseries, done_series)))
        processes[-1].start()

    # iterator = client.list_blobs(bucket, page_token=page_token, max_results=args.batch)
    n = 0
    with open(args.found_blobs, 'w') as f:
        series_iterator = client.list_blobs(bucket, versions=False, page_size=args.batch, \
                                            prefix='', delimiter='/')
        for page in series_iterator.pages:
            prefixes = [prefix for prefix in page.prefixes]
            task_queue.put((bucket, prefixes, n))
            # for prefix in page.prefixes:
            #     instance_iterator = client.list_blobs(bucket, versions=False, page_token=page_token, page_size=args.batch, \
            #                              prefix=prefix)
            #     for page in instance_iterator.pages:
            #         blobs = [f'{blob.name}\n' for blob in page]
            #         f.write(''.join(blobs))

    # Tell child processes to stop
    for i in range(num_processes):
        task_queue.put('STOP')
        task_queue.put('STOP')
    # Wait for process to terminate
    for process in processes:
        print(f'Joining process: {process.name}, {process.is_alive()}')
        process.join()

def find_source_bucket(args, blob):
    for bucket in args.buckets:
        if bucket.blob(blob).exists():
            return bucket.name
    return 'not found'

def get_prestaging_blobs_in_bucket(args):
    client = storage.Client()
    bucket = client.bucket(args.bucket)

    # Get the completed series
    done_series = open(f'{prestageseries.handlers[0].baseFilename}').read().splitlines()

    # Start worker processes
    num_processes = args.processes
    processes = []
    task_queue = Queue()
    for process in range(num_processes):
        args.id = process + 1
        processes.append(
            Process(group=None, target=worker, args=(task_queue, args, prestageblobs, prestageseries, done_series)))
        processes[-1].start()

    for bucket in args.buckets:
        n = 0
        with open(args.found_blobs, 'w') as f:
            series_iterator = client.list_blobs(bucket, versions=False, page_size=args.batch, \
                                                prefix='', delimiter='/')
            for page in series_iterator.pages:
                prefixes = [prefix for prefix in page.prefixes]
                task_queue.put((bucket,prefixes, n))

    # Tell child processes to stop
    for i in range(num_processes):
        task_queue.put('STOP')
        task_queue.put('STOP')
    # Wait for process to terminate
    for process in processes:
        print(f'Joining process: {process.name}, {process.is_alive()}')
        process.join()



def check_all_instances_mp(args, premerge=False):
    bq_client = bigquery.Client()
    try:
        prestaging_blobs = open(f'{prestageblobs.handlers[0].baseFilename}').read().splitlines()
        assert len(prestaging_blobs) > 0
        progresslogger.info(f'Already have prestaging blobs')
    except:
        get_prestaging_blobs_in_bucket(args)
        prestaging_blobs = set(open(args.prestaging_blobs).read().splitlines())
    prestaging_blobs = [row.replace(' ', '') for row in prestaging_blobs]

    try:
        unexpected_blobs = set(open(args.unexpected_blobs).read().splitlines())
        assert len(unexpected_blobs) > 0
        progresslogger.info(f'Already have unexpected blobs')
        unexpected_blobs = [blob.replace('[',"").replace(']',"").replace("'","").replace(" ","") for blob in unexpected_blobs]
    except:
        try:
            found_blobs = [blob.split(',') for blob in open(args.found_blobs).read().splitlines()]
            assert len(found_blobs) > 0
            progresslogger.info(f'Already have found blobs')
        except:
            progresslogger.info(f'Getting found blobs')
            get_found_blobs_in_bucket(args)
            found_blobs = [blob.split(',') for blob in open(args.found_blobs).read().splitlines()]

        try:
            expected_blobs = set(open(args.expected_blobs).read().splitlines())
            progresslogger.info(f'Already have expected blobs')
        except:
            progresslogger.info(f'Getting expected blobs')
            get_expected_blobs_in_bucket(args, premerge)
            expected_blobs = set(open(args.expected_blobs).read().splitlines())

        with open(args.unexpected_blobs, 'w') as f:
            unexpected_blobs = [blob for blob in found_blobs if not blob[0] in expected_blobs]
            for blob in unexpected_blobs:
                f.write(f'{blob}\n')

        if found_blobs == expected_blobs:
            successlogger.info(f"Bucket {args.bucket} has the correct set of blobs")
            return

    errlogger.error(f"Bucket {args.bucket} does not have the correct set of blobs")
    errlogger.error(f"Unexpected blobs in bucket: {len(unexpected_blobs)}")

    # json_object = '\n'.join([json.dumps(record) for record in unexpected_blobs])
    # load_BQ_from_json(bq_client, 'idc-dev-etl', 'whc_dev', 'unexpected_blobs', json_object, write_disposition='WRITE_TRUNCATE')
    # json_object = '\n'.join([json.dumps(record) for record in prestaging_blobs])
    # load_BQ_from_json(bq_client, 'idc-dev-etl', 'whc_dev', 'prestaging_blobs', unexpected_blobs, write_disposition='WRITE_TRUNCATE')
    schema = [
        bigquery.SchemaField('blob_name', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('bucket', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('hash', 'STRING', mode='NULLABLE')
        ]

    load_BQ_from_CSV(bq_client, 'idc-dev-etl', 'whc_dev', 'unexpected_blobs', '\n'.join(unexpected_blobs), aschema=schema, write_disposition='WRITE_TRUNCATE')
    load_BQ_from_CSV(bq_client, 'idc-dev-etl', 'whc_dev', 'prestaging_blobs', '\n'.join(prestaging_blobs), aschema=schema, write_disposition='WRITE_TRUNCATE')
    exit

    unexpected_buckets = {}
    for blob in unexpected_blobs:
        bucket = find_source_bucket(args, blob)
        if bucket not in unexpected_buckets:
            unexpected_buckets[bucket] = 1
        else:
            unexpected_buckets[bucket] += 1
        errlogger.error(f'{bucket}: {blob}')

    with open(args.unexpected_bucket_counts, 'w') as f:
        for bucket in unexpected_buckets:
            f.write(f'{bucket}: {unexpected_buckets[bucket]}\n')

    for blob in unexpected_blobs:
        f.write(f'{blob}\n')

    errlogger.error(f"Expected blobs not found in bucket: {len(expected_blobs - found_blobs)}")
    for blob in expected_blobs - found_blobs:
        errlogger.error(blob)



    return


