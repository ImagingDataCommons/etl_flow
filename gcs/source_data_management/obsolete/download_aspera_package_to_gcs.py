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
# A disjoint process is writing files to a directory.
# This script scans the directory for files that have been fully written,
# It copies each file to a GCS bucket, and deletes the file from the directory.

import os
from google.cloud import storage
from multiprocessing import Process, Queue
from utilities.logging_config import successlogger, errlogger, progresslogger
from time import sleep, time
from subprocess import run
from google.api_core.exceptions import Conflict
import json
from io import StringIO
import settings

# This worker process scans a directory for files, and writes their paths to a queue.
def get_aspera_package_directories(url):
    MAX_TRIES = 10
    LIMIT = 1000
    offset = 0
    tries = 0
    while True:
        cmmd = 'ascli --format=json faspex5 packages browse --query=@json:\'{"limit":' + str(LIMIT) + ',"offset":' + str(offset) + "}'" + \
               f' --url={url}'
        result = run(cmmd, capture_output=True, shell=True)
        if not result.stderr.startswith(b'ERROR'):
            some_files = json.load(StringIO(result.stdout.decode()))
            break
        else:
            errlogger.error(f'Failed to get new file list')
            tries += 1
            if tries == MAX_TRIES:
                errlogger.info(f"Failed to download aspera package, attempt {tries}")
                break
    return some_files

def download_aspera_package(aspera_url, download_path, subdir):
    # Download the file to disk
    progresslogger.info(f'Starting aspera download')
    try:
        # aspera_start = time()
        cmmd = ["ascli", "--progress-bar=no", "--format=json", f'--to-folder={download_path}', "faspex5",
             "packages", "receive", f"--url={aspera_url}", f'/{subdir}']
        res = run(cmmd)
        # aspera_delta = time.time() - aspera_start
        if res.stderr:
            errlogger.error(f'Aspera download failed: {res.stderr}')
            return
        else:
            progresslogger.info(f'Completed aspera download')
    except Exception as exc:
        errlogger.error(
            f'Aspera download failed: {exc}')
        return

def scan_directory(queue, directory, bucket, collection_id, download_slug):
    while True:
        blobs = []
        with open("commands.txt", "w") as f:
            for root, dirs, files in os.walk(directory):
                # print(root, dirs, files)
                # sub_path = root.replace(f'{directory}/','')
                sub_path = root.replace(f'{directory}/','').replace(" ", "\ ") # Get the last part of the path
                for file in files:
                    if not file.endswith('partial') and not file.endswith('.sums'):
                        blob_name = f'{sub_path}/{file}'.replace("\ ","")
                        blobs.append(blob_name)
                        file_path = f'{root}/{file}'
                        cmd = f"mv --destination-region us-central1 {file_path} s3://{bucket.name}/{blob_name}\n"
                        f.write(cmd)
        if blobs:
            real_path = os.path.realpath("commands.txt")
            cmmd = [" s5cmd", "--endpoint-url", "https://storage.googleapis.com", "run", f"{real_path}"]
            result = run(cmmd, capture_output=True)
            if result.stderr:
                errlogger.error(f's5cmd mv failed')
            else:
                for blob in blobs:
                    successlogger.info(blob)
        # sleep(1)  # Wait before scanning again
        if not queue.empty():
            break


# Launch a worker that scans a directory for files and uploads them to a GCS bucket
# Launch an Aspera download of a specified package to a specified directory
# When the download is complete, terminate the worker
def download_package(dones, gcs_path, collection_id, gcs_bucket_name, aspera_url, collection_version, download_slug):
    storage_client = storage.Client()
    os.putenv("AWS_ACCESS_KEY_ID", settings.AWS_ACCESS_KEY_ID)
    os.putenv("AWS_SECRET_ACCESS_KEY", settings.AWS_SECRET_ACCESS_KEY)
    download_base = '/mnt/disks/idc-etl/aspera'  # Change this to your download folder
    download_path = f'{download_base}/{collection_id}/{collection_version}/{download_slug}'

    try:
        bucket = storage_client.create_bucket(gcs_bucket_name, project='idc-source-data', location='US-CENTRAL1')
    except Conflict:
        # Bucket exists
        pass
        bucket = storage_client.bucket(gcs_bucket_name)
    except Exception as e:
        # Bucket creation failed somehow
        errlogger.error("Error creating bucket %s: %s",gcs_bucket_name, e)
        exit(-1)

    # Create the download directory if it does not exist
    if not os.path.exists(download_path):
        os.makedirs(download_path, exist_ok=True)

    queue = Queue()
    worker = Process(target=scan_directory, args=(queue, f'{download_base}', bucket, collection_version, download_slug))
    worker.start()

    # files = get_aspera_package_directories(aspera_url)
    # for file in files:
    #     if file['type'] == 'directory':
    #         subdir = file['basename']
    #         if not f'{gcs_path}/{subdir}' in dones:
    #             progresslogger.info(f"Downloading {subdir}")
    #             download_aspera_package(aspera_url, download_path, subdir)
    #             successlogger.info(f'{gcs_path}/{subdir}')
    #         else:
    #             progresslogger.info(f'{gcs_path}/{subdir} already processed, skipping download.')

    download_aspera_package(aspera_url, download_path, "")


    # Wait for all files to have been deleted:
    while sum([len(files) for r, d, files in os.walk(download_path)]) > 0:
        sleep(10)
    queue.put(None)  # Send exit signal to the worker
    worker.join()  # Wait for the worker to finish
    successlogger.info("Worker terminated gracefully.")


if __name__ == "__main__":
    dones = open(successlogger.handlers[0].baseFilename).read().splitlines()

    collection_id = 'mimm_sbilab'
    gcs_bucket_name = 'mimm_sbilab_pathology_data'  # Change this to your GCS bucket name
    aspera_url = 'https://faspex.cancerimagingarchive.net/aspera/faspex/public/package?context=eyJyZXNvdXJjZSI6InBhY2thZ2VzIiwidHlwZSI6ImV4dGVybmFsX2Rvd25sb2FkX3BhY2thZ2UiLCJpZCI6Ijc0MiIsInBhc3Njb2RlIjoiZDJkOTY4MTFhM2M3N2IxNmZkYWRjOGMzNDgyNTgzNzEyNzg1ZTJiZiIsInBhY2thZ2VfaWQiOiI3NDIiLCJlbWFpbCI6ImhlbHBAY2FuY2VyaW1hZ2luZ2FyY2hpdmUubmV0In0='
    collection_version= 'v3'
    download_slug = 'mimm_sbilab-da-path'
    gcs_path = f'{gcs_bucket_name}/{collection_id}/{collection_version}/{download_slug}'

    download_package(dones, gcs_path, collection_id, gcs_bucket_name, aspera_url, collection_version, download_slug)