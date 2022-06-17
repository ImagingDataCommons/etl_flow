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

# This script validates whether the patients in some set of collections are
# or are not in some DICOM store. The intent is mostly to ensure that (the patients)
# in the collections listed in the redacted_collections BQ table are not in
# the v<x>-no-redacted DICOM store. We cannot query collections in a DICOM store or
# directly for patients in a collection. So we must query for studies in patient in
# a collection.
# The script can be parameterized to test for the presence or absence of patients
# in any xxx_collections BQ tables.


import argparse
import os
import logging
from logging import INFO
from google.cloud import bigquery
rootlogger = logging.getLogger('root')
successlogger = logging.getLogger('success')
errlogger = logging.getLogger('root.err')
import google

import json
from time import sleep
from googleapiclient.errors import HttpError
from utilities.gch_helpers import get_dicom_store, get_dataset, create_dicom_store, create_dataset
from googleapiclient import discovery
from google.auth.transport import requests

def get_patients_in_collections(args):
    client = bigquery.Client()
    query = f"""
        SELECT
          distinct submitter_case_id
        FROM
          `idc-dev-etl.idc_v5.auxiliary_metadata` AS aux
        JOIN
          `{args.project}.{args.bqdataset}.{args.collection_table}` AS o
        ON
          aux.tcia_api_collection_id = o.tcia_api_collection_id
        UNION ALL
        SELECT
          distinct submitter_case_id
        FROM
          `idc-dev-etl.idc_v5.retired` AS r
        JOIN
          `{args.project}.{args.bqdataset}.{args.collection_table}` AS o
        ON
          r.collection_id = o.tcia_api_collection_id
    """
    patients = [i[0] for i in client.query(query)]
    return patients




# Return the status from searching for a patient ID
def dicomweb_search_studies(project_id, location, dataset_id, dicom_store_id, patient_name):
    """Handles the GET requests specified in the DICOMweb standard.

    See https://github.com/GoogleCloudPlatform/python-docs-samples/tree/master/healthcare/api-client/v1/dicom
    before running the sample."""
    scoped_credentials, project = google.auth.default(
        ["https://www.googleapis.com/auth/cloud-platform"]
    )
    # Creates a requests Session object with the credentials.
    session = requests.AuthorizedSession(scoped_credentials)

    # URL to the Cloud Healthcare API endpoint and version
    base_url = "https://healthcare.googleapis.com/v1"

    url = "{}/projects/{}/locations/{}".format(base_url, project_id, location)

    dicomweb_path = "{}/datasets/{}/dicomStores/{}/dicomWeb/studies".format(
        url, dataset_id, dicom_store_id
    )

    params = {"PatientID": patient_name}

    response = session.get(dicomweb_path, params=params)
    return response.status_code

def validate_patients(args):
    try:
        psql_patients = json.load(open(args.patient_ids))
    except:
        psql_patients = get_patients_in_collections(args)
        json.dump(psql_patients, open(args.patient_ids, 'w'))

    psql_patients.sort()
    n=0
    patients=len(psql_patients)
    if args.included:
        for patient in psql_patients:
            result = dicomweb_search_studies(args.project, args.dataset_location, args.gch_dataset_id, args.dicom_store_id, patient)
            if result==200:
                if n % 100 == 0:
                    print(f'{n}:{patients}')
            else:
                print(f'{n}:{patients}   Not found: {patient}')
            n += 1
    else:
        for patient in psql_patients:
            result = dicomweb_search_studies(args.project, args.dataset_location, args.gch_dataset_id,
                                             args.dicom_store_id, patient)
            if result != 200:
                if n%100 == 0:
                    print(f'{n}:{patients}')
            else:
                print(f'{n}:{patients}   Found: {patient}')
            n += 1


if __name__ == '__main__':
    version = 8

    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default = version)
    parser.add_argument('--project', default = 'idc-dev-etl')
    parser.add_argument('--dataset_location', default='us')
    parser.add_argument('--gch_dataset_id', default='idc')
    parser.add_argument('--dicom_store_id', default=f'v{version}')
    parser.add_argument('--bqdataset', default=f'idc_v{version}_dev')
    parser.add_argument('--collection_table', default='redacted_collections', help='BQ table containing list of collections')
    parser.add_argument('--patient_ids', default='./logs/patient_ids.txt', help='List of patient_ids in above collections')
    parser.add_argument('--included', default=False, help='If True, test whether patients are included, otherwise if excluded')
    parser.add_argument('--processes', default=1, help="Number of concurrent processes")
    parser.add_argument('--batch', default=100, help='Size of batch assigned to each process')
    parser.add_argument('--log_dir', default=f'/mnt/disks/idc-etl/logs/validate_dicom_store')

    args = parser.parse_args()

    if not os.path.exists('{}'.format(args.log_dir)):
        os.mkdir('{}'.format(args.log_dir))

    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler(f'{os.environ["PWD"]}/logs/bucket.log')
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    successlogger = logging.getLogger('success')
    successlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')



    validate_patients(args)
