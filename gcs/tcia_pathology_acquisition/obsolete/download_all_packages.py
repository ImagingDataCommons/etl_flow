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

import json
import os
from google.cloud import storage, bigquery
from utilities.logging_config import successlogger, progresslogger, errlogger
import settings
import argparse
from download_aspera_package_to_gcs import download_package
import time

def get_aspera_package_urls():
    client = bigquery.Client()
    query = f"""
    SELECT *
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.tcia_pathology_conversion_status` 
--     WHERE IDC_collection_id != ""
    """
    aspera_packages = client.query(query).to_dataframe()
    aspera_packages.sort_values("IDC_collection_id", inplace=True)
    return aspera_packages

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--download_slugs", default = [], help="Slugs to process; all if empty")
    parser.add_argument("--skip_slugs", default = ["bone-marrow-cytomorphology_mll_helmholtz_fraunhofer-da-path"], \
            help="Slugs to skip")
    args = parser.parse_args()
    print(f'args: {json.dumps(args.__dict__, indent=2)}')

    dones = open(successlogger.handlers[0].baseFilename).read().splitlines()

    aspera_packages = get_aspera_package_urls().sort_values("Download_size_GB")
    for _, package in aspera_packages.iterrows():
        if package['Download_slug'] not in args.skip_slugs and (args.download_slugs == [] or package['Download_slug'] in args.download_slugs):
            if package['IDC_collection_id'] == "":
                collection_id = package['TCIA_collection_id']
                # Form the GCS bucket name from the collection ID. Must be 63 characters or less.
                gcs_bucket_name = f'{collection_id[0:(63-len("_pathology_data"))]}_pathology_data'
                aspera_url = package['Aspera_URL']
                collection_version = f'v{package["TCIA_collection_version"]}'
                download_slug = package['Download_slug']
                gcs_path = f'{gcs_bucket_name}/{collection_id}/{collection_version}/{download_slug}'
                if not gcs_path in dones:
                    start = time.time()
                    progresslogger.info(f'Downloading {gcs_path}')
                    download_package(dones, gcs_path,  collection_id, gcs_bucket_name, aspera_url, collection_version, download_slug)
                    successlogger.info(f'{gcs_path}')
                    elapsed = time.time() - start
                    progresslogger.info(f'Finished downloading {gcs_path} in {elapsed:.2f} seconds')
                else:
                    progresslogger.info(f'Skipping {gcs_path}, already downloaded')

