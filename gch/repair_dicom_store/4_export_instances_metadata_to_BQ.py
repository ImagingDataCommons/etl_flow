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

# Export metadata from a DICOM store to BQ. Instance whose metadata os to be exported are listed in a
# "filter file"; see https://cloud.google.com/healthcare-api/docs/how-tos/dicom-export-bigquery#exporting_dicom_metadata_using_filters

import argparse
import sys
import os
import json
import requests
import shlex
import subprocess
import time
from subprocess import PIPE
from google.cloud import bigquery
from googleapiclient.errors import HttpError
from google.api_core.exceptions import NotFound
from utilities.bq_helpers import create_BQ_dataset, copy_BQ_table


def export_dicom_metadata(args):
    # Get an access token
    results = subprocess.run(['gcloud', 'auth', 'application-default', 'print-access-token'], stdout=PIPE, stderr=PIPE)
    bearer = str(results.stdout,encoding='utf-8').strip()

    # BQ table to which to export metadata
    destination = f'bq://{args.dst_project}.{args.bqdataset}.{args.bqtable}'
    data = {
        'bigqueryDestination': {
            'tableUri': destination,
            'writeDisposition': 'WRITE_APPEND',
        },
        'filterConfig': {
            'resourcePathsGcsUri': f'{args.filter}'
        },

    }


    headers = {
        'Authorization': f'Bearer {bearer}',
        'Content-Type': 'application/json; charset=utf-8'
    }
    url = f'https://healthcare.googleapis.com/v1beta1/projects/{args.src_project}/locations/{args.src_region}/datasets/{args.dcmdataset_name}/dicomStores/{args.dcmdatastore_name}:export'
    results = requests.post(url, headers=headers, json=data)

    # Get the operation ID so we can track progress
    operation_id = results.json()['name'].split('/')[-1]
    print("Operation ID: {}".format(operation_id))

    while True:
        # Get an access token. This can be a long running job. Just get a new one every time.
        results = subprocess.run(['gcloud', 'auth', 'application-default', 'print-access-token'], stdout=PIPE, stderr=PIPE)
        bearer = str(results.stdout, encoding='utf-8').strip()

        headers = {
            'Authorization': f'Bearer {bearer}'
        }
        url = f'https://healthcare.googleapis.com/v1/projects/{args.src_project}/locations/{args.src_region}/datasets/{args.dcmdataset_name}/operations/{operation_id}'
        results = requests.get(url, headers=headers)

        details = results.json()

        # The result is JSON that will include a "done" element with status when the op us complete
        if 'done' in details and details['done']:
            if 'error' in details:
                print('Done with errorcode: {}, message: {}'.format(details['error']['code'], details['error']['message']))
            else:
                print('Done')
            break
        else:
            print(details)
            time.sleep(args.period)

def get_job(args):
    results = subprocess.run(['gcloud', 'auth', 'application-default', 'print-access-token'], stdout=PIPE, stderr=PIPE)
    bearer = str(results.stdout, encoding='utf-8').strip()

    headers = {
        'Authorization': f'Bearer {bearer}'
    }
    url = f'https://healthcare.googleapis.com/v1/projects/{args.src_project}/locations/{args.src_region}/datasets/{args.dcmdataset_name}/operations'
    results = requests.get(url, headers=headers)
    # Get the operation ID so we can track progress
    operation_id = results.json()['operations'][0]['name'].split('/')[-1]
    print("Operation ID: {}".format(operation_id))

    while True:
        # Get an access token. This can be a long running job. Just get a new one every time.
        results = subprocess.run(['gcloud', 'auth', 'application-default', 'print-access-token'], stdout=PIPE, stderr=PIPE)
        bearer = str(results.stdout, encoding='utf-8').strip()

        headers = {
            'Authorization': f'Bearer {bearer}'
        }
        url = f'https://healthcare.googleapis.com/v1/projects/{args.src_project}/locations/{args.src_region}/datasets/{args.dcmdataset_name}/operations/{operation_id}'
        results = requests.get(url, headers=headers)

        details = results.json()

        # The result is JSON that will include a "done" element with status when the op us complete
        if 'done' in details and details['done']:
            if 'error' in details:
                print('Done with errorcode: {}, message: {}'.format(details['error']['code'], details['error']['message']))
            else:
                print('Done')
            break
        else:
            print(details)
            time.sleep(5*60)

def export_metadata(args):
    client = bigquery.Client(project=args.dst_project)
    # Create the BQ dataset if it does not already exist
    try:
        dst_dataset = client.get_dataset(args.bqdataset)
    except NotFound:
        dst_dataset = create_BQ_dataset(client, args.bqdataset, args.dataset_description)

    try:
        start = time.time()
        response=export_dicom_metadata(args)
        finished = time.time()
        elapsed = finished - start
        print('Elapsed time: {}'.format(elapsed))

    except HttpError as e:
        err=json.loads(e.content)
        print(f'Error {e}')


if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--version', default=8, help="IDC version")
    args = parser.parse_args()
    # DICOM store parameters
    parser.add_argument('--src_project', default='canceridc-data', help='Project of the DICOM store')
    parser.add_argument('--src_region', default='us', help='DICOM dataset region')
    parser.add_argument('--dcmdataset_name', default='idc', help='DICOM dataset name')
    parser.add_argument('--dcmdatastore_name', default=f'v{args.version}', help='DICOM datastore name')
    # BQ target dataset
    parser.add_argument('--dst_project', default='idc-dev-etl', help='BQ dataset project')
    parser.add_argument('--dst_region', default='us', help='Dataset region')
    parser.add_argument('--bqdataset', default=f'idc_v{args.version}_pub', help="BQ dataset name")
    # parser.add_argument('--bqdataset', default=f'whc_dev', help="BQ dataset name")
    parser.add_argument('--bqtable', default='dicom_metadata', help="BQ table name")
    parser.add_argument('--filter', default='gs://whc_dev/tcga_brca_filter.csv', help='List of instances to export')
    parser.add_argument('--dataset_description', default = f'IDC V{args.version} BQ tables and views')
    parser.add_argument('--period', default=60)

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    # get_job(args)
    export_metadata(args)