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

# Load data in some GCS buckets into a DICOM store.


import argparse
import sys
import os
import json
from time import sleep
from googleapiclient.errors import HttpError
from google.cloud import storage
from utilities.gch_helpers import get_dicom_store, get_dataset, create_dicom_store, create_dataset
import logging
from logging import INFO
from googleapiclient import discovery

_BASE_URL = "https://healthcare.googleapis.com/v1"


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


def wait_done(response, args, sleep_time):
    operation = response['name'].split('/')[-1]
    while True:
        result = get_dataset_operation(args.project, args.region, args.gch_dataset_name, operation)
        print("{}".format(result))

        if 'done' in result:
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
    print("Imported DICOM instance: {}".format(content_uri))

    return response


# def import_collection(args):
#     try:
#         print('Importing {}'.format(args.src_bucket))
#         content_uri = '{}/*'.format(args.src_bucket)
#         response = import_dicom_instance(args.project, args.region, args.gch_dataset_name,
#                         args.gch_dicomstore_name, content_uri)
#         result = wait_done(response, args, args.period)
#         return result
#     except HttpError as e:
#         err = json.loads(e.content)
#         print('Error loading {}; code: {}, message: {}'.format(args.src_bucket, err['error']['code'], err['error']['message']))
#         if 'resolves to zero GCS objects' in err['error']['message']:
#             # An empty collection bucket throws an error
#             return


def import_bucket(args):
    # client = storage.Client()
    try:
        dataset = get_dataset(args.project, args.region, args.gch_dataset_name)
    except HttpError:
        # Dataset doesn't exist. Create it.
        response = create_dataset(args.project, args.region, args.gch_dataset_name)

    try:
        datastore = get_dicom_store(args.project, args.region, args.gch_dataset_name, args.gch_dicomstore_name)
    except HttpError:
        # Datastore doesn't exist. Create it
        datastore = create_dicom_store(args.project, args.region, args.gch_dataset_name, args.gch_dicomstore_name)
    pass

    # result = import_collection(args)
    for bucket in args.src_buckets:
        try:
            print('Importing {}'.format(bucket))
            content_uri = '{}/*'.format(bucket)
            response = import_dicom_instances(args.project, args.region, args.gch_dataset_name,
                            args.gch_dicomstore_name, content_uri)
            print(f'Response: {response}')
            result = wait_done(response, args, args.period)
        except HttpError as e:
            err = json.loads(e.content)
            print('Error loading {}; code: {}, message: {}'.format(bucket, err['error']['code'], err['error']['message']))

if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--src_buckets', default=['idc_dev'], help="List of buckets from which to import")
    parser.add_argument('--project', default='idc-dev-etl')
    parser.add_argument('--region', default='us-central1', help='Dataset region')
    parser.add_argument('--gch_dataset_name', default='idc', help='Dataset name')
    parser.add_argument('--gch_dicomstore_name', default='v3', help='Datastore name')
    parser.add_argument('--period', default=60, help="seconds to sleep between checking operation status")
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)

    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/dicomstore_import_log_2020_3_25.log'.format(os.environ['PWD']))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/dicomstore_import_err_2020_3_25.log'.format(os.environ['PWD']))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    import_bucket(args)

