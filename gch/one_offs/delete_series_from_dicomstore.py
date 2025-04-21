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

# Delete selected series from some specified IDC DICOM store
# This is a single use script having hardcoded values for the project, location, dataset and dicom store

import sys
import json
import argparse
from fnmatch import fnmatch
from time import sleep
from google.cloud import storage, bigquery
from googleapiclient.errors import HttpError

from python_settings import settings

from utilities.gch_helpers import get_dicom_store, get_dataset, create_dicom_store, create_dataset
from googleapiclient import discovery

from utilities.logging_config import progresslogger, successlogger, errlogger


def dicomweb_delete_series(project_id, location, dataset_id, dicom_store_id, study_uid, series_uid):
    """Handles DELETE requests equivalent to the GET requests specified in
    the WADO-RS standard.

    See https://github.com/GoogleCloudPlatform/python-docs-samples/tree/main/healthcare/api-client/v1/dicom
    before running the sample."""

    # Imports the google.auth.transport.requests transport
    from google.auth.transport import requests

    # Imports a module to allow authentication using Application Default Credentials (ADC)
    import google.auth

    # Gets credentials from the environment. google.auth.default() returns credentials and the
    # associated project ID, but in this sample, the project ID is passed in manually.
    credentials, _ = google.auth.default()

    scoped_credentials = credentials.with_scopes(
        ["https://www.googleapis.com/auth/cloud-platform"]
    )
    # Creates a requests Session object with the credentials.
    session = requests.AuthorizedSession(scoped_credentials)

    # URL to the Cloud Healthcare API endpoint and version
    base_url = "https://healthcare.googleapis.com/v1"

    url = f"{base_url}/projects/{project_id}/locations/{location}"

    # dicomweb_path = "{}/datasets/{}/dicomStores/{}/dicomWeb/studies/{}/series/{}".format(
    dicomweb_path="{}/datasets/{}/dicomStores/{}/dicomWeb/studies/{}/series/{}".format(
            url, dataset_id, dicom_store_id, study_uid, series_uid
    )

    # Sets the required application/dicom+json; charset=utf-8 header on the request
    headers = {"Content-Type": "application/dicom+json; charset=utf-8"}

    response = session.delete(dicomweb_path, headers=headers)
    response.raise_for_status()

    print(f"Deleted series {series_uid}.")

    return response

def get_series_to_delete(args):
    client = bigquery.Client()
    query = f"""
    SELECT DISTINCT 
    study_instance_uid, series_instance_uid
    FROM `idc-dev-etl.idc_v21_dev.all_joined_public_and_current`
    WHERE versioned_source_doi = '10.5281/zenodo.14041167'
    """

    series = client.query(query).result().to_dataframe()

    return series

def delete_selected_series(args):
    series = get_series_to_delete(args)
    for index, row in series.iterrows():
        dicomweb_delete_series('canceridc-data', 'us', 'idc', 'v21',
                               row['study_instance_uid'], row['series_instance_uid'])

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--period', default=60, help="seconds to sleep between checking operation status")
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)

    delete_selected_series(args)
