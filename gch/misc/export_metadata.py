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
import json
import requests
import subprocess
import time
from subprocess import PIPE
from google.cloud import bigquery
from googleapiclient.errors import HttpError
from google.api_core.exceptions import NotFound
from utilities.bq_helpers import create_BQ_dataset, copy_BQ_table
import settings

import logging
from utilities.logging_config import successlogger, progresslogger, errlogger


def export_dicom_metadata(args):
    # Get an access token
    results = subprocess.run(['gcloud', 'auth', 'application-default', 'print-access-token'], stdout=PIPE, stderr=PIPE)
    bearer = str(results.stdout,encoding='utf-8').strip()

    # BQ table to which to export metadata
    destination = f'bq://{args.bq_project}.{args.bq_dataset}.{args.bq_table}'
    data = {
        'bigqueryDestination': {
            'tableUri': destination,
            'writeDisposition': 'WRITE_TRUNCATE'
        }
    }

    headers = {
        'Authorization': f'Bearer {bearer}',
        'Content-Type': 'application/json; charset=utf-8'
    }
    url = f'https://healthcare.googleapis.com/v1/projects/{args.gch_project}/locations/{args.gch_region}/datasets/{args.gch_dataset}/dicomStores/{args.gch_dicomstore}:export'
    results = requests.post(url, headers=headers, json=data)

    # Get the operation ID so we can track progress
    if results.status_code == 200:
        operation_id = results.json()['name'].split('/')[-1]
        progresslogger.info(f"Metadata export initiated. Operation ID: {operation_id}")

        while True:
            # Get an access token. This can be a long running job. Just get a new one every time.
            results = subprocess.run(['gcloud', 'auth', 'application-default', 'print-access-token'], stdout=PIPE, stderr=PIPE)
            bearer = str(results.stdout, encoding='utf-8').strip()

            headers = {
                'Authorization': f'Bearer {bearer}'
            }
            url = f'https://healthcare.googleapis.com/v1/projects/{args.gch_project}/locations/{args.gch_region}/datasets/{args.gch_dataset}/operations/{operation_id}'
            results = requests.get(url, headers=headers)

            details = results.json()

            # The result is JSON that will include a "done" element with status when the op us complete
            if 'done' in details and details['done']:
                if 'error' in details:
                    errlogger.error(f"Done with errorcode: {details['error']['code']}, message: {details['error']['message']}")
                else:
                    progresslogger.info(details)
                break
            else:
                progresslogger.info(details)
                time.sleep(5*60)

        return 0
    else:
        return -1


def export_metadata(args):
    client = bigquery.Client(project=args.bq_project)
    # Create the BQ dataset if it does not already exist
    try:
        dst_dataset = client.get_dataset(args.bq_dataset)
    except NotFound:
        dst_dataset = create_BQ_dataset(client, args.bq_dataset, args.dataset_description)

    start = time.time()
    response=export_dicom_metadata(args)
    finished = time.time()
    elapsed = finished - start
    progresslogger.info('Elapsed time: {}'.format(elapsed))

    if response == 0:
        progresslogger.info(f"Metadata export successful")
    else:
        progresslogger.info(f"Metadata export failed")


if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    # parser.add_argument('--bq_project', default = settings.DEV_PROJECT)
    # parser.add_argument('--bq_dataset', default=settings.BQ_DEV_INT_DATASET)
    # parser.add_argument('--bq_table', default='dicom_pathology_metadata')
    # parser.add_argument('--gch_project', default=settings.DEV_PROJECT)
    # parser.add_argument('--gch_region', default='us-central1')
    # parser.add_argument('--gch_dataset', default='idc')
    # parser.add_argument('--gch_dicomstore', default='idc_v10_pathology')
    #
    # parser.add_argument('--dataset_description', default = f'IDC V{settings.CURRENT_VERSION} pathology metadata')
    parser.add_argument('--billing_project', default = 'idc-etl-processing')
    parser.add_argument('--bq_project', default = 'idc-etl-processing')
    parser.add_argument('--bq_dataset', default="whc_dev")
    parser.add_argument('--bq_table', default='dicom_metadata')
    parser.add_argument('--gch_project', default='chc-tcia')
    parser.add_argument('--gch_region', default='us-central1')
    parser.add_argument('--gch_dataset', default='idc')
    parser.add_argument('--gch_dicomstore', default='idc-store')

    parser.add_argument('--dataset_description', default = f'PDP DICOM store metadata')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    export_metadata(args)

