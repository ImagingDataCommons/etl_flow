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

from python_settings import settings

import psycopg2
from psycopg2.extras import DictCursor, RealDictCursor


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
        result = get_dataset_operation(args.dst_project, args.dataset_region, args.gch_dataset_name, operation)
        print("{}".format(result))

        if 'done' in result:
            break
        sleep(sleep_time)
    return result


def import_dicom_instance(project_id, cloud_region, dataset_id, dicom_store_id, content_uri):
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
    print("Initiated DICOM import,response: {}".format(response))

    return response

def copy_blobs(args):
    conn = psycopg2.connect(dbname='idc_v4', user=settings.CLOUD_USERNAME, port=settings.CLOUD_PORT,
                            password=settings.CLOUD_PASSWORD, host=settings.CLOUD_HOST)
    with conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            query = """
                SELECT * 
                FROM instance
                where timestamp > '2021-09-01'
                """
            cur.execute(query)
            instances = cur.fetchall()

            for instance in instances:
                content_uri = f'{args.src_bucket}/{instance["uuid"]}.dcm'

                response = import_dicom_instance(args.dst_project, args.dataset_region, args.gch_dataset_name,
                            args.gch_dicomstore_name, content_uri)
                result = wait_done(response, args, args.period)


if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--src_bucket', default='idc_v5_nlst', help="List of buckets from which to import")
    parser.add_argument('--dst_project', default='idc-dev-etl')
    parser.add_argument('--dataset_region', default='us-central1', help='Dataset region')
    parser.add_argument('--gch_dataset_name', default='idc', help='Dataset name')
    parser.add_argument('--gch_dicomstore_name', default='v4', help='Datastore name')
    parser.add_argument('--period', default=2, help="seconds to sleep between checking operation status")
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

    copy_blobs(args)
