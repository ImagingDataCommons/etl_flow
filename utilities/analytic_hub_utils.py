#
# Copyright 2023, Institute for Systems Biology
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
import json

import requests
import logging
import subprocess

import google.oauth2.id_token
import google.auth.transport.requests as google_requests




# from http.client import HTTPConnection
# HTTPConnection.debuglevel = 0
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# rootlogger = logging.getLogger('root')
# errlogger = logging.getLogger('root.err')

# from python_settings import settings
import settings
import logging
logging.getLogger("requests").setLevel(logging.WARNING)

def get_token():
    gcloud_itoken = subprocess.check_output(["gcloud", "auth" ,"application-default", "print-access-token"])
    gcloud_itoken_str = gcloud_itoken.decode().strip()
    return gcloud_itoken_str

def create_exchange():
    url = 'https://analyticshub.googleapis.com/v1/projects/nci-idc-bigquery-data/locations/US/dataExchanges?dataExchangeId=nci_idc_bigquery_data_exchange'
    data = {
        "displayName": "nci-idc-bigquery-data-exchange",
        "description": "Exchange for publication of NCI IDC BQ datasets",
        "primaryContact": "bcliffor@systemsbiology.org"
    }

    headers = {
        "Authorization": f"Bearer {get_token()}" }
    result =  requests.post(url, data=json.dumps(data), headers=headers)
    response = result.json()
    if result.status_code != 200:
        raise RuntimeError('In get_url(): status_code=%s; url: %s', result.status_code, url)
    return (response)

def get_exchange_policy():
    url = 'https://analyticshub.googleapis.com/v1/projects/nci-idc-bigquery-data/locations/US/dataExchanges/nci_idc_bigquery_data_exchange:getIamPolicy'
    data = {}

    token = get_token()
    headers = {
        "Authorization": f"Bearer {token}" }

    result =  requests.post(url, headers=headers, data=json.dumps(data))
    response = result.json()
    if result.status_code != 200:
        raise RuntimeError('In get_url(): status_code=%s; url: %s', result.status_code, url)
    return (response)


def list_exchanges():
    url = 'https://analyticshub.googleapis.com/v1/projects/nci-idc-bigquery-data/locations/US/dataExchanges?dataExchangeId=nci_idc_bigquery_data_exchange'
    data = {
        "displayName": "nci-idc-bigquery-data-exchange",
        "description": "Exchange for publication of NCI IDC BQ datasets",
        "primaryContact": "bcliffor@systemsbiology.org"
    }

    headers = {
        "Authorization": f"Bearer {get_token()}" }
    result =  requests.post(url, data=json.dumps(data), headers=headers)
    response = result.json()
    if result.status_code != 200:
        raise RuntimeError('In get_url(): status_code=%s; url: %s', result.status_code, url)
    return (response)

if __name__ == "__main__":
    p = get_exchange_policy()
    # t = get_token()
    # r = create_exchange()

