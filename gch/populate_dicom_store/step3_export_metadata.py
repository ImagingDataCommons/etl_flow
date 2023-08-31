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

# Export metadata from a DICOM store to BQ. If the table currently exists, it is truncated.

import argparse
import sys
import json
import requests
import subprocess
import time
import settings
from subprocess import PIPE
from google.cloud import bigquery
from googleapiclient.errors import HttpError
from google.api_core.exceptions import NotFound
from utilities.bq_helpers import create_BQ_dataset, copy_BQ_table
from utilities.logging_config import successlogger, progresslogger, errlogger


def export_dicom_metadata(args):
    # Get an access token
    results = subprocess.run(['gcloud', 'auth', 'application-default', 'print-access-token'], stdout=PIPE, stderr=PIPE)
    bearer = str(results.stdout,encoding='utf-8').strip()

    # BQ table to which to export metadata
    destination = f'bq://{args.bq_project}.{args.bq_dataset}.dicom_metadata'
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


def get_job(args):
    results = subprocess.run(['gcloud', 'auth', 'application-default', 'print-access-token'], stdout=PIPE, stderr=PIPE)
    bearer = str(results.stdout, encoding='utf-8').strip()

    headers = {
        'Authorization': f'Bearer {bearer}'
    }
    url = f'https://healthcare.googleapis.com/v1/projects/{args.gch_dicomstore}/locations/{args.gch_region}/datasets/{args.gch_dataset}/operations'
    results = requests.get(url, headers=headers)
    # Get the operation ID so we can track progress
    operation_id = results.json()['operations'][0]['name'].split('/')[-1]
    progresslogger.info("Operation ID: {}".format(operation_id))

    while True:
        # Get an access token. This can be a long running job. Just get a new one every time.
        results = subprocess.run(['gcloud', 'auth', 'application-default', 'print-access-token'], stdout=PIPE, stderr=PIPE)
        bearer = str(results.stdout, encoding='utf-8').strip()

        headers = {
            'Authorization': f'Bearer {bearer}'
        }
        url = f'https://healthcare.googleapis.com/v1/projects/{args.gch_dicomstore}/locations/{args.gch_region}/datasets/{args.gch_dataset}/operations/{operation_id}'
        results = requests.get(url, headers=headers)

        details = results.json()

        # The result is JSON that will include a "done" element with status when the op us complete
        if 'done' in details and details['done']:
            if 'error' in details:
                errlogger.error('Done with errorcode: {}, message: {}'.format(details['error']['code'], details['error']['message']))
            else:
                progresslogger.info('Done')
            break
        else:
            progresslogger.info(details)
            time.sleep(5*60)


def export_metadata(args):
    client = bigquery.Client(project=args.bq_project)
    # Create the BQ dataset if it does not already exist
    try:
        dst_dataset = client.get_dataset(args.bq_dataset)
    except NotFound:
        dst_dataset = create_BQ_dataset(client, args.bq_dataset, args.dataset_description)

    try:
        start = time.time()
        response=export_dicom_metadata(args)
        finished = time.time()
        elapsed = finished - start
        progresslogger.info('Elapsed time: {}'.format(elapsed))

    except HttpError as e:
        err=json.loads(e.content)
        errlogger.error(f'Error {e}')


if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--dataset_description', default = f'IDC V{settings.CURRENT_VERSION} BQ tables and views')
    parser.add_argument('--bq_project', default=settings.DEV_PROJECT)
    parser.add_argument('--bq_dataset', default=settings.BQ_DEV_EXT_DATASET)
    parser.add_argument('--gch_project', default=settings.PUB_PROJECT)
    parser.add_argument('--gch_region', default=settings.GCH_REGION)
    parser.add_argument('--gch_dataset', default=settings.GCH_DATASET)
    parser.add_argument('--gch_dicomstore', default=settings.GCH_DICOMSTORE)
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    export_metadata(args)

