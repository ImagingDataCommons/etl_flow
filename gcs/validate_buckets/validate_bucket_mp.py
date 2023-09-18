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
import builtins
# Noramlly the progresslogger file is trunacated. The following causes it to be appended.
# builtins.APPEND_PROGRESSLOGGER = True
from utilities.logging_config import successlogger, progresslogger, errlogger
from google.cloud import storage, bigquery
from multiprocessing import Process, Queue

def worker(input, args, dones):
    # proglogger.info('p%s: Worker starting: args: %s', args.id, args )
    # print(f'p{args.id}: Worker starting: args: {args}')

    RETRIES=3

    client = storage.Client()
    bucket = client.bucket(args.bucket)

    for prefixes, n in iter(input.get, 'STOP'):
        try:
            for prefix in prefixes:
                if prefix not in dones:
                    instance_iterator = client.list_blobs(bucket, versions=False, page_size=args.batch, \
                                         prefix=prefix)
                    for page in instance_iterator.pages:

                        for blob in page:
                            successlogger.info(blob.name)
                    progresslogger.info(prefix)
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
    done_series = open(f'{progresslogger.handlers[0].baseFilename}').read().splitlines()

    # Start worker processes
    num_processes = args.processes
    processes = []
    task_queue = Queue()
    for process in range(num_processes):
        args.id = process + 1
        processes.append(
            Process(group=None, target=worker, args=(task_queue, args, done_series)))
        processes[-1].start()

    # iterator = client.list_blobs(bucket, page_token=page_token, max_results=args.batch)
    n = 0
    with open(args.found_blobs, 'w') as f:
        series_iterator = client.list_blobs(bucket, versions=False, page_size=args.batch, \
                                            prefix='', delimiter='/')
        for page in series_iterator.pages:
            prefixes = [prefix for prefix in page.prefixes]
            task_queue.put((prefixes, n))
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


def check_all_instances_mp(args, premerge=False):
    try:
        expected_blobs = set(open(args.expected_blobs).read().splitlines())
        progresslogger.info(f'Already have expected blobs')
    except:
        progresslogger.info(f'Getting expected blobs')
        get_expected_blobs_in_bucket(args, premerge)
        expected_blobs = set(open(args.expected_blobs).read().splitlines())
        # json.dump(psql_blobs, open(args.blob_names), 'w')

    try:
        found_blobs = set(open(args.found_blobs).read().splitlines())
        # found_blobs = open(f'{successlogger.handlers[0].baseFilename}').read().splitlines()
        progresslogger.info(f'Already have found blobs')
    except:
        progresslogger.info(f'Getting found blobs')
        get_found_blobs_in_bucket(args)
        found_blobs = open(f'{successlogger.handlers[0].baseFilename}').read().splitlines()
        # json.dump(psql_blobs, open(args.blob_names), 'w')


    progresslogger.info(f'Getting found blobs')
    get_found_blobs_in_bucket(args)
    found_blobs = set(open(f'{successlogger.handlers[0].baseFilename}').read().splitlines())

    if found_blobs == expected_blobs:
        successlogger.info(f"Bucket {args.bucket} has the correct set of blobs")
    else:
        errlogger.error(f"Bucket {args.bucket} does not have the correct set of blobs")
        errlogger.error(f"Unexpected blobs in bucket: {len(found_blobs - expected_blobs)}")
        for blob in found_blobs - expected_blobs:
            errlogger.error(blob)
        errlogger.error(f"Expected blobs not found in bucket: {len(expected_blobs - found_blobs)}")
        for blob in expected_blobs - found_blobs:
            errlogger.error(blob)

    return


