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



# Export metadata from a DICOM store to BQ

import argparse
import sys
import os
import json
import requests
import shlex
import subprocess
import time
from subprocess import PIPE
from googleapiclient.errors import HttpError

# from helpers.dicom_helpers import get_dataset, get_dicom_store, create_dicom_store, import_dicom_instance

def export_dicom_metadata(args):
    # Get an access token
    results = subprocess.run(['gcloud', 'auth', 'application-default', 'print-access-token'], stdout=PIPE, stderr=PIPE)
    bearer = str(results.stdout,encoding='utf-8').strip()

    # BQ table to which to export metadata
    destination = f'bq://{args.dst_project}.{args.bqdataset}.{args.bqtable}'
    data = {
        'bigqueryDestination': {
            'tableUri': destination,
            'force': False
        }
    }

    headers = {
        'Authorization': f'Bearer {bearer}',
        'Content-Type': 'application/json; charset=utf-8'
    }
    url = f'https://healthcare.googleapis.com/v1/projects/{args.src_project}/locations/{args.src_region}/datasets/{args.dcmdataset_name}/dicomStores/{args.dcmdatastore_name}:export'
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
            time.sleep(5*60)

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
    # try:
    #     dataset = get_dataset(args.SA, args.project, args.region, args.dcmdataset_name)
    # except HttpError:
    #     print("Can't access dataset")
    #     exit(-1)
    #
    # try:
    #     datastore = get_dicom_store(args.project, args.region, args.dcmdataset_name, args.dcmdatastore_name)
    # except HttpError:
    #     # Datastore doesn't exist. Create it
    #     datastore = create_dicom_store(args.project, args.region, args.dcmdataset_name, args.dcmdatastore_name)
    # pass

    try:
        start = time.time()
        response=export_dicom_metadata(args)
        finished = time.time()
        elapsed = finished - start
        print('Elapsed time: {}'.format(elapsed))

    except HttpError as e:
        err=json.loads(e.content)
        print('Error loading {}; code: {}, message: {}'.format(bucket.name, err['error']['code'], err['error']['message']))


if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--version', default=5, help="IDC version")
    args = parser.parse_args()
    parser.add_argument('--src_project', default='idc-nlst')
    parser.add_argument('--dst_project', default='idc-nlst')
    parser.add_argument('--src_region', default='us-central1', help='Dataset region')
    parser.add_argument('--dst_region', default='us', help='Dataset region')
    parser.add_argument('--dcmdataset_name', default='idc', help='DICOM dataset name')
    parser.add_argument('--dcmdatastore_name', default=f'v{args.version}', help='DICOM datastore name')
    parser.add_argument('--bqdataset', default=f'idc_v{args.version}', help="BQ dataset name")
    parser.add_argument('--bqtable', default='dicom_metadata', help="BQ table name")
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    # get_job(args)
    export_metadata(args)

