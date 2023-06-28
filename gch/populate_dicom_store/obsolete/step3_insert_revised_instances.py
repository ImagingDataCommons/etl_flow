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

#### This is the third step in populating a DICOM store ####
# We now insert the version of an instance if the instance has been
# revised but is not retired. This will replace the revised instances
# which we deleted in the previous step.
# For this purpose we populate a bucket with those instance, and then
# import the entire bucket. This is much faster than storing each
# instance individually.

import argparse
import json
from time import sleep
from googleapiclient import discovery
from google.api_core.exceptions import Conflict
from step1_import_buckets import import_buckets
from utilities.logging_config import successlogger, progresslogger, errlogger
import settings
import google
from google.cloud import storage, bigquery
from google.auth.transport import requests
from multiprocessing import Process, Queue

from sqlalchemy import create_engine
from sqlalchemy_utils import register_composites
from sqlalchemy.orm import Session

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


def wait_done(response, args, sleep_time, verbose=True):
    operation = response['name'].split('/')[-1]
    while True:
        result = get_dataset_operation(settings.GCH_PROJECT, settings.GCH_REGION, settings.GCH_DATASET, operation)
        if verbose:
            print("{}".format(result))

        if 'done' in result:
            if not verbose:
                print("{}".format(result))
            break
        sleep(sleep_time)
    return result


def import_dicom_instances(project_id, cloud_region, dataset_id, dicom_store_id, content_uri):
    """
    Import data into the DICOM store by copying it from the specified source.
    """
    client = get_gch_client()
    dicom_store_parent = "projects/{}/locations/{}/datasets/{}".format(
        project_id, cloud_region, dataset_id
    )
    dicom_store_name = "{}/dicomStores/{}".format(dicom_store_parent, dicom_store_id)

    body = {"gcsSource": {"uri": "gs://{}".format(content_uri)}}

    request = (
        client.projects()
        .locations()
        .datasets()
        .dicomStores()
        .import_(name=dicom_store_name, body=body)
    )

    response = request.execute()
    print("Initiated DICOM import,response: {}".format(response))

    return response


def create_staging_bucket(args):
    client = storage.Client(project='idc-dev-etl')

    # Try to create the destination bucket
    new_bucket = client.bucket(args.staging_bucket)
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
        errlogger.error("Error creating bucket %s: %s",args.staging_bucket, e)
        return(-1)

# Populate a bucket with instances to be inserted in the DICOM store
def copy_some_instances(args, uids, dones):
    client = storage.Client()
    dst_bucket = client.bucket(args.staging_bucket)
    for row in uids:
        blob_id = row['blob_id']
        if not blob_id in dones:
            src_bucket = client.bucket(row['bucket'])
            src_blob = src_bucket.blob(blob_id)
            dst_blob = dst_bucket.blob(blob_id)
            TRIES = 3
            for attempt in range(TRIES):
                try:
                    token, bytes_rewritten, total_bytes = dst_blob.rewrite(src_blob)
                    while token:
                        progresslogger.debug('******p%s: Rewrite bytes_rewritten %s, total_bytes %s', args.pid, bytes_rewritten,
                                          total_bytes)
                        token, bytes_rewritten, total_bytes = dst_blob.rewrite(src_blob, token=token)
                    successlogger.info(blob_id)
                    break
                except Exception as exc:
                    if attempt == (TRIES - 1):
                        errlogger.error(f'p{args.id}:{blob_id}: {exc}')
        else:
            progresslogger.info(f'p{args.id} {blob_id} exists')


def worker(input, args, dones):
    # client = storage.Client()
    for uids, n in iter(input.get, 'STOP'):
        progresslogger.info(f'p{args.id}: {n}')
        copy_some_instances(args, uids, dones)


def populate_staging_bucket(args, dicomweb_sess):
    client = bigquery.Client()

    try:
        # Get the previously copied blobs
        # done_instances = set(open(f'{args.log_dir}/insert_success.log').read().splitlines())
        dones = open(f'{successlogger.handlers[0].baseFilename}').read().splitlines()
    except:
        dones = []

    # The following generates the source bucket of each instance. If the instance's rev_idc_version is the current
    # version, then the instance is still in a premerge bucket. Otherwise it is in one of the dev merged buckets.
    # query = f"""
    # SELECT DISTINCT CONCAT(aj.se_uuid, '/', aj.i_uuid, '.dcm') blob_id,
    # IF(i_rev_idc_version!={settings.CURRENT_VERSION},
    #     IF(aj.i_source='tcia', ac.dev_tcia_url, ac.dev_idc_url), CONCAT('idc_v', {settings.CURRENT_VERSION},'_',aj.i_source,REPLACE(REPLACE(LOWER(aj.collection_id), '-','_'),' ','_'))) bucket
    # FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined` aj
    # JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_collections` ac ON aj.collection_id=ac.tcia_api_collection_id
    # WHERE i_rev_idc_version!=i_init_idc_version
    #   AND i_final_idc_version=0
    #   AND ((aj.i_source='tcia' AND ac.tcia_access='Public') OR (aj.i_source='idc' AND ac.idc_access='Public'))
    #   AND NOT aj.collection_id LIKE 'APOLLO%'
    #   AND NOT i_excluded
    # ORDER BY blob_id
    # """

    query = f"""
    SELECT DISTINCT CONCAT(aj.se_uuid, '/', aj.i_uuid, '.dcm') blob_id, 
    IF(i_rev_idc_version!={settings.CURRENT_VERSION},
        IF(aj.i_source='tcia', ac.dev_tcia_url, ac.dev_idc_url), CONCAT('idc_v', {settings.CURRENT_VERSION},'_',aj.i_source,REPLACE(REPLACE(LOWER(aj.collection_id), '-','_'),' ','_'))) bucket
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined` aj
    JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_collections` ac ON aj.collection_id=ac.tcia_api_collection_id
    WHERE i_rev_idc_version!=i_init_idc_version 
      AND i_final_idc_version=0
      AND ((aj.i_source='tcia' AND ac.tcia_access='Public') OR (aj.i_source='idc' AND ac.idc_access='Public'))
      AND NOT aj.collection_id LIKE 'APOLLO%'
      AND NOT i_excluded
    ORDER BY blob_id
    """

    query_job = client.query(query)
    query_job.result()
    destination = query_job.destination
    destination = client.get_table(destination)

    # todo_uids = [{'se_uuid':row.se_uuid, 'i_uuid':row.i_uuid, 'bucket':row.bucket} for row in result \
    #              if f'{row.se_uuid}/{row.i_uuid}' not in dones]

    num_processes = args.processes
    processes = []
    task_queue = Queue()
    # Start worker processes
    for process in range(num_processes):
        args.id = process + 1
        processes.append(
            Process(group=None, target=worker, args=(task_queue, args, dones)))
        processes[-1].start()

   # Populate thestaging bucket
    n=len(dones)
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
        print(f'Joining process: {process.name}, {process.is_alive()}')
        process.join()


    # # Import the staging bucket into the DICOM store
    # print('Importing {}'.format(args.staging_bucket))
    # content_uri = '{}/*'.format(args.staging_bucket)
    # response = import_dicom_instances(settings.GCH_PROJECT, settings.GCH_REGION, settings.GCH_DATASET,
    #                                   settings.GCH_DICOMSTORE, content_uri)
    # print(f'Response: {response}')
    # result = wait_done(response, args, args.period)
    #
    # # Don't forget to delete the staging bucket

def import_staging_bucket(args):
    args.src_buckets = [args.staging_bucket]
    args.period = 60
    import_buckets(args)

def repair_store(args):
    # sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{settings.CLOUD_DATABASE}'
    # sql_engine = create_engine(sql_uri, echo=True) # Use this to see the SQL being sent to PSQL
    # # sql_engine = create_engine(sql_uri)
    # args.sql_uri = sql_uri # The subprocesses need this uri to create their own SQL engine
    #
    # # Enable the underlying psycopg2 to deal with composites
    # conn = sql_engine.connect()
    # register_composites(conn)

    scoped_credentials, dst_project = google.auth.default(
        ["https://www.googleapis.com/auth/cloud-platform"]
    )
    # Creates a requests Session object with the credentials.
    dicomweb_sess = requests.AuthorizedSession(scoped_credentials)

    create_staging_bucket(args)
    populate_staging_bucket(args, dicomweb_sess)
    import_staging_bucket(args)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--client', default=storage.Client())
    parser.add_argument('--processes', default=16)
    parser.add_argument('--batch', default=100)
    parser.add_argument('--log_dir', default=settings.LOG_DIR)
    parser.add_argument('--period',default=60)
    parser.add_argument('--staging_bucket', default=f'populate_dicom_store_step3_staging_bucket_v{settings.CURRENT_VERSION}')
    args = parser.parse_args()
    args.id = 0 # Default process ID

    repair_store(args)