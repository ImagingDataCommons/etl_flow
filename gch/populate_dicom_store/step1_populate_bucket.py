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

#### This is the first step in populating a DICOM store ####
# We populate three buckets with all the instances in a version.
# These buckets correspond to data in the GCS idc-open-data, idc-open-cr
# and idc-open-idc1 buckets.
# The bucket of idc-open-data instances can be used by Google to populate
# their DICOM store
# Note that, for yet unexplained reasons, the process can
# fail to copy some instances to the target bucket. Therefore,
# this process should be rerun until the bucket is fully populated.
# Because we may need to rerun this script more than once, we do not
# empty the staging bucket when we begin.

import sys
import argparse
from googleapiclient import discovery
from google.api_core.exceptions import Conflict, TooManyRequests, ServiceUnavailable, NotFound
#from step2_import_bucket import import_buckets
from utilities.logging_config import successlogger, progresslogger, errlogger
import settings
from google.cloud import storage, bigquery
from multiprocessing import Process, Queue
from time import  sleep


def get_gch_client():
    """Returns an authorized API client by discovering the Healthcare API and
    creating a service object using the service account credentials in the
    GOOGLE_APPLICATION_CREDENTIALS environment variable."""
    api_version = "v1"
    service_name = "healthcare"

    return discovery.build(service_name, api_version)


def get_dataset_operation(
        project_id,
        cloud_region,
        dataset_id,
        operation):
    client = get_gch_client()
    op_parent = "projects/{}/locations/{}/datasets/{}".format(project_id, cloud_region, dataset_id)
    op_name = "{}/operations/{}".format(op_parent, operation)
    request = client.projects().locations().datasets().operations().get(name=op_name)
    response = request.execute()
    return response


def create_import_bucket(args):
    client = storage.Client(project=args.bucket_project)

    # Try to create the destination bucket
    new_bucket = client.bucket(args.import_bucket_name)
    new_bucket.iam_configuration.uniform_bucket_level_access_enabled = True
    new_bucket.versioning_enabled = False
    try:
        result = client.create_bucket(new_bucket, location='US-CENTRAL1')
        # return(0)
    except Conflict:
        # Bucket exists
        pass
    except Exception as e:
        # Bucket creation failed somehow
        errlogger.error("Error creating bucket %s: %s",args.import_bucket_name, e)
        return(-1)

def build_dones_table(args):
    # Construct a BigQuery client object.
    client = bigquery.Client()

    schema = [
            bigquery.SchemaField("blob_id", "STRING"),
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

# Populate a bucket with instances to be inserted in the DICOM store
def copy_some_instances(args, client, uids, n):
    try:
        done = 0
        dst_bucket = client.bucket(args.import_bucket_name)
        for row in uids:
            blob_id = row['blob_id']
            BUCKET_TRIES = 10
            for i in range(BUCKET_TRIES):
                try:
                    src_bucket = client.bucket(row['bucket'])
                    src_blob = src_bucket.blob(blob_id)
                    break
                except AttributeError as exc:
                    errlogger.warning(
                        f"p{args.id}: Trying to create bucket: {exc}; attempt {i}\n")
                    sleep(1)
                except TooManyRequests as exc:
                    errlogger.warning(
                        f"p{args.id}: Blob: Too many requests: {repr(exc)};  {exc}\n")
                    sleep(1)
                except ServiceUnavailable as exc:
                    errlogger.warning(
                        f"p{args.id}: Blob: Service unavailable: {repr(exc)};  {exc}\n")
                    sleep(1)
            if i == BUCKET_TRIES:
                errlogger.error(
                    f"p{args.id}: Failed to create bucket)")
                break

            dst_blob = dst_bucket.blob(blob_id)
            rewrite_token = False
            while True:
                try:
                    rewrite_token, bytes_rewritten, total_bytes = dst_blob.rewrite(
                        src_blob, token=rewrite_token
                    )
                    if not rewrite_token:
                        successlogger.info(f'{blob_id}')
                        break
                except AttributeError as exc:
                    errlogger.warning(
                        f"p{args.id}: Trying to create bucket: {exc}; attempt {i}\n")
                    sleep(1)
                except TooManyRequests as exc0:
                    errlogger.warning(
                        f"p{args.id}: Blob: Too many requests: {repr(exc0)};  {exc0}")
                    sleep(1)
                except ServiceUnavailable as exc0:
                    errlogger.warning(
                        f"p{args.id}: Blob: Service unavailable: {repr(exc0)};  {exc0}")
                    sleep(1)
                except NotFound as exc:
                    errlogger.error(
                        f"p{args.id}: Blob: Source blob {row['bucket']}/{blob_id} not found")
                    break

                    break
            done += 1
        progresslogger.info(f"p{args.id}: {done + n}of{len(uids) + n}")
    except Exception as exc2:
        # breakpoint()
        errlogger.exception(f'p{args.id}: copy: exception type: {repr(exc2)}; {exc2}; row = {row}')
    return

def worker(input, args):
    client = storage.Client()
    for uids, n in iter(input.get, 'STOP'):
        try:
            copy_some_instances(args, client, uids, n)
        except Exception as exc3:
            # breakpoint()
            errlogger.error(f'p{args.id}: worker, exception type: {repr(exc3)} exception {exc3}')
    return


def populate_import_buckets(args):
    client = bigquery.Client()
    dones = build_dones_table(args)

    query = f"""
    WITH alls AS (
        SELECT
          CONCAT(se_uuid, '/', i_uuid, '.dcm') blob_id,
        # If this series is new/revised in this version and we 
        # have not merged new instances into dev buckets
        IF(se_rev_idc_version = {args.version} and not {args.merged},
            # We use the premerge url prefix
            CONCAT('idc_v', {args.version}, 
                '_',
                i_source,
                '_',
                REPLACE(REPLACE(LOWER(collection_id),'-','_'), ' ','_')
                ),

        #else
            # This instance is not new so use the public GCS bucket
            pub_gcs_bucket) bucket
        FROM
          `{settings.DEV_PROJECT}.idc_v{args.version}_dev.all_joined_public_and_current`
        WHERE pub_gcs_bucket = '{args.pub_gcs_bucket}'
        {f"AND collection_id IN {args.collections}" if args.collections else ""}
    )
    SELECT alls.*
    FROM alls
    LEFT JOIN {args.dones_table_id} dones
    ON alls.blob_id = dones.blob_id
    WHERE dones.blob_id IS Null
    ORDER BY blob_id 
    """

    query_job = client.query(query)
    result = query_job.result()
    destination = query_job.destination
    destination = client.get_table(destination)

    progresslogger.info(f'p{0}: {dones} of {dones+destination.num_rows} completed')

    num_processes = args.processes
    processes = []
    task_queue = Queue()
    # Start worker processes
    for process in range(num_processes):
        args.id = process + 1
        processes.append(
            Process(group=None, target=worker, args=(task_queue, args)))
        processes[-1].start()

    # Populate the staging bucket
    n = dones
    for page in client.list_rows(destination, page_size=args.batch).pages:
        uuids = [{'blob_id':row.blob_id, 'bucket':row.bucket} \
            for row in page]
        task_queue.put((uuids,n))
        n += args.batch

    # Tell child processes to stop
    for i in range(num_processes):
        task_queue.put('STOP')

    # Wait for process to terminate
    for process in processes:
        progresslogger.info(f'Joining process: {process.name}, {process.is_alive()}')
        process.join()


def populate_buckets(args):
    for suffix in [
        # 'idc-open-idc1',
        # 'idc-open-cr',
        'idc-open-data'
    ]:
        args.pub_gcs_bucket = suffix
        args.import_bucket_name = f'dicom_store_import_v{args.version}_{suffix}'
        # create_import_bucket(args)
        populate_import_buckets(args)

if __name__ == '__main__':
    # version = settings.CURRENT_VERSION
    version = 20
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=version)
    parser.add_argument('--client', default=storage.Client())
    parser.add_argument('--collections', default=(), help='Collections to include. If empty, include all collections')
    parser.add_argument('--bucket_project', default='nci-idc-bigquery-data', help='Project in which to build buckets')
    parser.add_argument('--processes', default=512)
    parser.add_argument('--batch', default=100)
    parser.add_argument('--dones_table_id', default='idc-dev-etl.whc_dev.step1_dones', help='BQ table from which to import dones')
    parser.add_argument('--log_dir', default=settings.LOG_DIR)
    parser.add_argument('--merged', default=True, help='True if premerge buckets have been merged')
    args = parser.parse_args()
    args.id = 0 # Default process ID
    progresslogger.info(f"{args}")

    populate_buckets(args)