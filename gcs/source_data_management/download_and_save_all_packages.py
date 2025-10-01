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
import shutil

from google.cloud import storage, bigquery
from utilities.logging_config import successlogger, progresslogger, errlogger
import argparse
import time
from subprocess import run



def get_aspera_package_data():
    client = bigquery.Client()
    query = f"""
    SELECT * 
    FROM `idc-dev-etl.idc_v22_dev.tcia_pathology_conversion_status`
    WHERE IDC_collection_id = ""
    ORDER BY Download_size_GB
    """
    aspera_packages = client.query(query).to_dataframe()
    return aspera_packages


def download_package_to_disk(aspera_url, package_target_folder):
    try:
        cmmd = ['ascli', '--format=json', f'--to-folder={package_target_folder}', 'faspex5', 'packages', 'receive',
                f'--url={aspera_url}']
        res = run(cmmd)
        if res.stderr:
            errlogger.error(f'Aspera download failed: {res.stderr}')
            return
        else:
            progresslogger.info(f'Completed aspera download')
    except Exception as exc:
        errlogger.error(
            f'Aspera download failed: {exc}')
        return

# Find the directory into which ascli downloaded files. These directory names have embedded spaces, which
# s5cmd cannot handle. So change the directory name to one without spaces and return it.
def get_directory_name(collection_id, target_folder):
    comp_collection_id = collection_id.lower().replace(" ", "_").replace("-", "_")
    dirs = os.listdir(target_folder)
    for dir in dirs:
        new_dir_name = dir.lower().replace(" ", "_").replace("-", "_")
        if new_dir_name.find(comp_collection_id) >= 0:
            old_path = os.path.join(target_folder, dir)
            new_path = os.path.join(target_folder, new_dir_name)
            os.rename(old_path, new_path)
            return new_dir_name
    errlogger.error(f'Package name not found for {collection_id}')
    exit(1)

def copy_disk_to_gcs(gcs_bucket_name, dir_name, collection_version, \
                     download_slug):
    try:
        # cmmd = ['gsutil', '-m', 'cp', '-c', '-R', '-L', f'{dir_name}.log',
        #         f'{args.target_folder}/{dir_name}/*',
        #         f'gs://{gcs_bucket_name}/{collection_version}/{download_slug}/']

        cmmd = ['gcloud', 'storage', 'cp', '--recursive', f'--manifest-path={args.target_folder}/log.log',
                '--no-clobber',
                f'{args.target_folder}/{dir_name}/*',
                f'gs://{gcs_bucket_name}/{collection_version}/{download_slug}/']

        # cmmd = ["s5cmd", "--endpoint-url", "https://storage.googleapis.com", "cp",
        #         f'{args.target_folder}/{dir_name}/',
        #         f's3://{gcs_bucket_name}/{collection_version}/{download_slug}/']

        res = run(cmmd)
        if res.stderr:
            errlogger.error(f'Aspera download failed: {res.stderr}')
            return
        else:
            successlogger.info(f'{gcs_path}')
    except Exception as exc:
        errlogger.error(
            f'Aspera download failed: {exc}')
        return


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--download_slugs", default = [], help="Slugs to process; all if empty")
    parser.add_argument("--skip_slugs", default = ['upenn-gbm-da-path'], \
            help="Slugs to skip")
    parser.add_argument("--target_folder", default="/mnt/disks/aspera-downloads-whc/packages")
    args = parser.parse_args()
    print(f'args: {json.dumps(args.__dict__, indent=2)}')

    dones = open(successlogger.handlers[0].baseFilename).read().splitlines()
    client = storage.Client()

    aspera_packages = get_aspera_package_data().sort_values("Download_size_GB")
    for _, package in aspera_packages.iterrows():
        collection_id = package['TCIA_collection_id']
        download_slug = package['Download_slug']
        if download_slug not in args.skip_slugs and \
                (args.download_slugs == [] or package['Download_slug'] in args.download_slugs) and \
                not collection_id in dones:
            aspera_url = package['Aspera_URL']
            download_package_to_disk(aspera_url, args.target_folder)

            dir_name = get_directory_name(collection_id, args.target_folder)
            # Form the GCS bucket name from the collection ID. Must be 63 characters or less.
            gcs_bucket_name = f'{collection_id[0:(63-len("_pathology_data"))]}_pathology_data'
            collection_version = f'v{package["TCIA_collection_version"]}'
            download_slug = package['Download_slug']
            gcs_path = f'{gcs_bucket_name}/{collection_id}/{collection_version}/{download_slug}'
            if not gcs_path in dones:
                start = time.time()
                progresslogger.info(f'Downloading {gcs_path}')
                if not client.bucket(gcs_bucket_name).exists():
                    client.create_bucket(gcs_bucket_name, location="US-CENTRAL1", project='idc-source-data', )
                else:
                    progresslogger.info(f'Bucket {gcs_bucket_name} exists')
                copy_disk_to_gcs(gcs_bucket_name, dir_name, collection_version, download_slug)
                elapsed = time.time() - start
                progresslogger.info(f'Finished downloading {gcs_path} in {elapsed:.2f} seconds')
                shutil.rmtree(f'{args.target_folder}/{dir_name}')
                successlogger.info(collection_id)
            else:
                progresslogger.info(f'Skipping {gcs_path}, already downloaded')

