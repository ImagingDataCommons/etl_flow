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
from idc.models import Base, Version, Patient, Study, Series, Instance, Collection, CR_Collections, Defaced_Collections, Open_Collections, Redacted_Collections
from ingestion.utilities.utils import empty_bucket, to_webapp
from utilities.logging_config import successlogger, progresslogger, errlogger
import settings
import google
from google.cloud import storage
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


def get_collection_groups(sess):
    dev_buckets = {}
    collections = sess.query(CR_Collections.tcia_api_collection_id, CR_Collections.dev_tcia_url, CR_Collections.dev_path_url)
    for collection in collections:
        dev_buckets[collection.tcia_api_collection_id] = {
            'tcia': collection.dev_tcia_url,
            'path': collection.dev_path_url
        }
    collections = sess.query(Defaced_Collections.tcia_api_collection_id, Defaced_Collections.dev_tcia_url, Defaced_Collections.dev_path_url)
    for collection in collections:
        dev_buckets[collection.tcia_api_collection_id] = {
            'tcia': collection.dev_tcia_url,
            'path': collection.dev_path_url
        }
    collections = sess.query(Open_Collections.tcia_api_collection_id, Open_Collections.dev_tcia_url, Open_Collections.dev_path_url)
    for collection in collections:
        dev_buckets[collection.tcia_api_collection_id] = {
            'tcia': collection.dev_tcia_url,
            'path': collection.dev_path_url
        }
    collections = sess.query(Redacted_Collections.tcia_api_collection_id, Redacted_Collections.dev_tcia_url, Redacted_Collections.dev_path_url)
    for collection in collections:
        dev_buckets[collection.tcia_api_collection_id] = {
            'tcia': collection.dev_tcia_url,
            'path': collection.dev_path_url
        }
    return dev_buckets


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
def populate_staging_bucket(args, uids):
    client = storage.Client()
    dst_bucket = client.bucket(args.staging_bucket)
    for row in uids:
        bucket = client.bucket(row['bucket'])
        dst_blob = dst_bucket.blob(f'{row["uuid"]}.dcm')
        if not dst_blob.exists():
            src_bucket = client.bucket(row['bucket'])
            blob_id = f'{row["uuid"]}.dcm'
            src_blob = src_bucket.blob(blob_id)
            dst_blob = dst_bucket.blob(blob_id)
            try:
                token, bytes_rewritten, total_bytes = dst_blob.rewrite(src_blob)
                while token:
                    progresslogger.debug('******p%s: Rewrite bytes_rewritten %s, total_bytes %s', args.pid, bytes_rewritten,
                                      total_bytes)
                    token, bytes_rewritten, total_bytes = dst_blob.rewrite(src_blob, token=token)
                successlogger.info(f'{row["uuid"]}')
            except Exception as exc:
               errlogger.error(f'{blob_id}: {exc}')
        else:
            progresslogger.info(f'{row["uuid"]} exists')

def worker(input, args):
    # client = storage.Client()
    for uids, n in iter(input.get, 'STOP'):
        progresslogger.info(f'p{args.id}: {n}')
        populate_staging_bucket(args, uids)


def insert_instances(args, sess, dicomweb_sess):
    try:
        # Get the previously copied blobs
        # done_instances = set(open(f'{args.log_dir}/insert_success.log').read().splitlines())
        done_instances = set(open(f'{successlogger.handlers[0].baseFilename}').read().splitlines())
    except:
        done_instances = set()

    num_processes = args.processes
    processes = []
    task_queue = Queue()
    # Start worker processes
    for process in range(num_processes):
        args.id = process + 1
        processes.append(
            Process(group=None, target=worker, args=(task_queue, args)))
        processes[-1].start()

    # Collections that are included in the DICOM store are in one of four groups
    collections = sorted(
        [row.tcia_api_collection_id for row in sess.query(Open_Collections.tcia_api_collection_id).union(
            sess.query(Defaced_Collections.tcia_api_collection_id),
            sess.query(CR_Collections.tcia_api_collection_id),
            sess.query(Redacted_Collections.tcia_api_collection_id)).all()])

    # dev_staging_buckets tells us which staging bucket has a collection's instances
    dev_buckets = get_collection_groups(sess)

    try:
        uids = json.load(open(f'{args.log_dir}/inserted_uids.txt'))
    except:
        rows =  sess.query(Collection.collection_id,Study.study_instance_uid, Series.series_instance_uid,
            Instance.sop_instance_uid,Instance.uuid, Instance.source, Instance.init_idc_version, Instance.rev_idc_version).join(Version.collections).join(Collection.patients).\
            join(Patient.studies).join(Study.seriess).join(Series.instances).filter(Instance.init_idc_version !=
            Instance.rev_idc_version).filter(Instance.final_idc_version == 0).\
            filter(Version.version == settings.CURRENT_VERSION).all()
        uids = [{'collection_id':row.collection_id, 'study_instance_uid':row.study_instance_uid, 'series_instance_uid':row.series_instance_uid,
                 'sop_instance_uid':row.sop_instance_uid,'uuid':row.uuid,
                 'bucket': dev_buckets[row.collection_id][row.source.name] if row.rev_idc_version != settings.CURRENT_VERSION else \
                        f'idc_v{settings.CURRENT_VERSION}_{row.source.name}_{to_webapp(row.collection_id)}'} \
                        for row in rows if row.collection_id in collections]
        with open(f'{args.log_dir}/inserted_uids.txt','w') as f:
            json.dump(uids,f)
    todo_uids = [row for row in uids if row['uuid'] not in done_instances]

    n=0
    while todo_uids:
        some_uids = todo_uids[0:args.batch]
        todo_uids = todo_uids[args.batch:]
        task_queue.put((some_uids,n))
        n += args.batch

    # Tell child processes to stop
    for i in range(num_processes):
        task_queue.put('STOP')

    # Wait for process to terminate
    for process in processes:
        print(f'Joining process: {process.name}, {process.is_alive()}')
        process.join()


    print('Importing {}'.format(args.staging_bucket))
    content_uri = '{}/*'.format(args.staging_bucket)
    response = import_dicom_instances(settings.GCH_PROJECT, settings.GCH_REGION, settings.GCH_DATASET,
                                      settings.GCH_DICOMSTORE, content_uri)
    print(f'Response: {response}')
    result = wait_done(response, args, args.period)

    # Don't forget to delete the staging bucket

def repair_store(args):
    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{settings.CLOUD_DATABASE}'
    sql_engine = create_engine(sql_uri, echo=True) # Use this to see the SQL being sent to PSQL
    # sql_engine = create_engine(sql_uri)
    args.sql_uri = sql_uri # The subprocesses need this uri to create their own SQL engine

    # Enable the underlying psycopg2 to deal with composites
    conn = sql_engine.connect()
    register_composites(conn)

    scoped_credentials, dst_project = google.auth.default(
        ["https://www.googleapis.com/auth/cloud-platform"]
    )
    # Creates a requests Session object with the credentials.
    dicomweb_sess = requests.AuthorizedSession(scoped_credentials)

    create_staging_bucket(args)
    with Session(sql_engine) as sess:
        insert_instances(args, sess, dicomweb_sess)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--client', default=storage.Client())
    parser.add_argument('--processes', default=1)
    parser.add_argument('--batch', default=100)
    parser.add_argument('--log_dir', default=settings.LOG_DIR)
    parser.add_argument('--period',default=60)
    parser.add_argument('--staging_bucket', default=f'populate_dicom_store_step3_staging_bucket_{settings.CURRENT_VERSION}')
    args = parser.parse_args()
    args.id = 0 # Default process ID

    repair_store(args)