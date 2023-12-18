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
from base64 import b64encode
import argparse
import settings

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


def get_exchange():
    url = 'https://analyticshub.googleapis.com/v1/projects/nci-idc-bigquery-data/locations/US/dataExchanges/nci_idc_bigquery_data_exchange'
    data = {}

    token = get_token()
    headers = {
        "Authorization": f"Bearer {token}" }

    result =  requests.get(url, headers=headers)
    response = result.json()
    if result.status_code != 200:
        raise RuntimeError('In get_url(): status_code=%s; url: %s', result.status_code, url)
    return (response)


def get_listing(listing):
    url = f'https://analyticshub.googleapis.com/v1/projects/nci-idc-bigquery-data/locations/US/dataExchanges/nci_idc_bigquery_data_exchange/listings/{listing}'
    data = {}

    token = get_token()
    headers = {
        "Authorization": f"Bearer {token}" }

    result =  requests.get(url, headers=headers)
    response = result.json()
    if result.status_code != 200:
        raise RuntimeError('In get_url(): status_code=%s; url: %s', result.status_code, url)
    return (response)


def list_exchanges():
    url = 'https://analyticshub.googleapis.com/v1/projects/nci-idc-bigquery-data/locations/US/dataExchanges'
    headers = {
        "Authorization": f"Bearer {get_token()}" }
    result =  requests.get(url, headers=headers)
    response = result.json()
    if result.status_code != 200:
        raise RuntimeError('In get_url(): status_code=%s; url: %s', result.status_code, url)
    return (response)

def create_listing(args):
    # url = 'https://analyticshub.googleapis.com/v1/projects/nci-idc-bigquery-data/locations/US/dataExchanges?dataExchangeId=nci_idc_bigquery_data_exchange'
    url = f'https://analyticshub.googleapis.com/v1/projects/{settings.AH_PROJECT}/locations/{settings.AH_EXCHANGE_LOCATION}/dataExchanges/{settings.AH_EXCHANGE_ID}/listings?listingId={args.listing_id}'

    with open(args.idc_icon, 'rb') as f:
        icon = f.read()
    # with open(args.idc_icon, 'r') as f:
    #     icon = f.read()
    icon_base64 = b64encode(icon).decode()

    data = {
        "displayName": args.display_name,
        "description": args.description,
        "documentation": args.documentation,
        "primaryContact": "https://discourse.canceridc.dev",
        "icon": icon_base64,
        "dataProvider": {
            "name": "National Cancer Institute",
            "primaryContact": "https://imaging.datacommons.cancer.gov"
        },
        "categories": [
            "CATEGORY_HEALTHCARE_AND_LIFE_SCIENCE",
            "CATEGORY_SCIENCE_AND_RESEARCH"
        ],
        "publisher": {
            "name": "Imaging Data Commons team",
            "primaryContact": "https://discourse.canceridc.dev"
        },
        "bigqueryDataset": {
            "dataset": args.dataset
        }
    }

    headers = {
        "Authorization": f"Bearer {get_token()}" }
    try:
        result =  requests.post(url, data=json.dumps(data), headers=headers)
        if result.status_code != 200:
            raise RuntimeError('In get_url(): status_code=%s; url: %s', result.status_code, url)
        response = result.json()
        print(response)
    except Exception as e:
        print(f'Listing creation failed: {e}')
        raise e
    return (response)

def patch_listing(args):
    # url = 'https://analyticshub.googleapis.com/v1/projects/nci-idc-bigquery-data/locations/US/dataExchanges?dataExchangeId=nci_idc_bigquery_data_exchange'
    url = f'https://analyticshub.googleapis.com/v1/projects/{settings.AH_PROJECT}/locations/{settings.AH_EXCHANGE_LOCATION}/dataExchanges/{settings.AH_EXCHANGE_ID}/listings/{args.listing_id}'

    with open(args.idc_icon, 'rb') as f:
        icon = f.read()
    # with open(args.idc_icon, 'r') as f:
    #     icon = f.read()
    icon_base64 = b64encode(icon).decode()

    data = {
        "displayName": args.display_name,
        "description": args.description,
        "documentation": args.documentation,
        "primaryContact": "https://discourse.canceridc.dev",
        "icon": icon_base64,
        "dataProvider": {
            "name": "National Cancer Institute",
            "primaryContact": "https://imaging.datacommons.cancer.gov"
        },
        "categories": [
            "CATEGORY_HEALTHCARE_AND_LIFE_SCIENCE",
            "CATEGORY_SCIENCE_AND_RESEARCH"
        ],
        "publisher": {
            "name": "Imaging Data Commons team",
            "primaryContact": "https://discourse.canceridc.dev"
        },
        "bigqueryDataset": {
            "dataset": args.dataset
        }
    }

    headers = {
        "Authorization": f"Bearer {get_token()}" }
    try:
        result =  requests.patch(url, data=json.dumps(data), headers=headers, params={'updateMask': args.fieldMask})
        if result.status_code != 200:
            raise RuntimeError('In get_url(): status_code=%s; url: %s', result.status_code, url)
        response = result.json()
    except Exception as e:
        print(f'Listing creation failed: {e}')
        raise e
    return (response)


def delete_listing(listing_id):
    url = f'https://analyticshub.googleapis.com/v1/projects/{settings.AH_PROJECT}/locations/{settings.AH_EXCHANGE_LOCATION}/dataExchanges/{settings.AH_EXCHANGE_ID}/listings/{listing_id}'

    headers = {
        "Authorization": f"Bearer {get_token()}" }
    try:
        result =  requests.delete(url, headers=headers)
        if result.status_code != 200:
            raise RuntimeError('In get_url(): status_code=%s; url: %s', result.status_code, url)
        response = result.json()
    except Exception as e:
        print(f'Listing deletion failed: {e}')
        raise e
    return (response)


if __name__ == "__main__":
    version = settings.CURRENT_VERSION
    parser = argparse.ArgumentParser()

    parser.add_argument('--listing_id', default="idc_v17")
    parser.add_argument('--display_name', default=f"NCI Imaging Data Commons BigQuery Metadata Catalog v{settings.CURRENT_VERSION}: Imaging Metadata")
    parser.add_argument('--description', default="IDC BQ metadata from the V17 IDC release")
    parser.add_argument('--dataset', default="idc_v17")
    parser.add_argument('--idc_icon', default="./idc_icon.png")
    args = parser.parse_args()
    args.description = '<p>NCI Imaging Data Commons (IDC) is a cloud-based environment containing publicly available cancer imaging data co-located with analysis and exploration tools and resources. IDC is a node within the broader NCI <a target="_blank" rel="noopener noreferrer" href="https://datacommons.cancer.gov/" sandboxuid="0" extsuid="0">Cancer Research Data Commons (CRDC) (https://datacommons.cancer.gov) infrastructure that provides secure access to a large, comprehensive, and expanding collection of cancer research data. IDC curates and shares imaging data in DICOM format, available for download from public Google Cloud Storage buckets. This dataset provides the searchable index of the metadata accompanying the shared DICOM files. You can read about IDC in this publication.</p>'

    l = get_listing('idc_v17_clinical')
    l = delete_listing('idc_v17_clinical')
    # p = get_exchange()
    l = list_exchanges()
    # t = get_token()
    # r = create_exchange()

