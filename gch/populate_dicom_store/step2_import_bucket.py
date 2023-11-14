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

# Load data in some GCS buckets into a DICOM store from a staging bucket.
# Typically the DICOM store should be empty (or non-existant) when this
# script is executed.

import sys
import json
import argparse
from fnmatch import fnmatch
from time import sleep
from google.cloud import storage
from googleapiclient.errors import HttpError

from python_settings import settings

from utilities.gch_helpers import get_dicom_store, get_dataset, create_dicom_store, create_dataset
from googleapiclient import discovery

from utilities.logging_config import progresslogger, successlogger, errlogger


def get_gch_client():
    """Returns an authorized API client by discovering the Healthcare API and
    creating a service object using the service account credentials in the
    GOOGLE_APPLICATION_CREDENTIALS environment variable."""
    api_version = "v1"
    service_name = "healthcare"

    return discovery.build(service_name, api_version, cache_discovery=False)

def get_wildcard_buckets(wild_card_bucket):
    client = storage.Client(project='idc-dev-etl')
    all_buckets = client.list_buckets(project='idc-dev-etl')
    buckets = []
    for bucket in all_buckets:
        if fnmatch(bucket.name, wild_card_bucket):
            buckets.append(bucket.name)
    return buckets

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
            # print("{}".format(result))
            try:
                progresslogger.info(f'counter: {result["metadata"]["counter"]}')
            except:
                progresslogger.info(f'No progress')
        if 'done' in result:
            try:
                progresslogger.info(f'counter: {result["metadata"]["counter"]}; done: {result["done"]}; response: {result["response"]}')
                break
            except:
                progresslogger.info((f'{result}'))
                break
        sleep(sleep_time)
    return result


def import_dicom_instances(project_id, cloud_region, dataset_id, dicom_store_id, content_uri):
    """Import data into the DICOM store by copying it from the specified
    source.
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
    # print("Initiated DICOM import,response: {}".format(response))
    progresslogger.info('Initiated DICOM import, response: %s', response)
    return response


def import_buckets(args):
    client = storage.Client()

    # Create the GCH dataset if it does not exist
    try:
        dataset = get_dataset(settings.GCH_PROJECT, settings.GCH_REGION, settings.GCH_DATASET)
    except HttpError:
        # Dataset doesn't exist. Create it.
        response = create_dataset(settings.GCH_PROJECT, settings.GCH_REGION, settings.GCH_DATASET)

    # Create the DICOM store if it does not exist
    try:
        datastore = get_dicom_store(settings.GCH_PROJECT, settings.GCH_REGION, settings.GCH_DATASET, settings.GCH_DICOMSTORE)
    except HttpError:
        # Datastore doesn't exist. Create it
        datastore = create_dicom_store(settings.GCH_PROJECT, settings.GCH_REGION, settings.GCH_DATASET, settings.GCH_DICOMSTORE)
    pass

    try:
        progresslogger.info('\nImporting %s', args.staging_bucket)
        content_uri = '{}/*/*'.format(args.staging_bucket)
        response = import_dicom_instances(settings.GCH_PROJECT, settings.GCH_REGION, settings.GCH_DATASET,
                        settings.GCH_DICOMSTORE, content_uri)
        progresslogger.info('Response: %s', response)
        result = wait_done(response, args, args.period)
        successlogger.info(args.staging_bucket)
    except HttpError as e:
        err = json.loads(e.content)
        errlogger.info('Error loading %s; code: %s, message: %s', args.staging_bucket, err['error']['code'], err['error']['message'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--period', default=60, help="seconds to sleep between checking operation status")
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    args.staging_bucket = f'dicom_store_import_staging_v{settings.CURRENT_VERSION}'

    breakpoint() # Not yet tested
    import_buckets(args)
