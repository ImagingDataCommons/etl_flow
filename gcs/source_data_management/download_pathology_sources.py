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
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import os
import argparse
import json
from google.cloud  import storage
import settings
from document_and_download_unconverted_tcia_pathology import bucket_collection_id, get_collection, get_aspera_package_urls
from utilities.logging_config import successlogger, progresslogger, errlogger
from time import strftime, gmtime
from multiprocessing import Process, Queue
import time
from subprocess import run


ASPERA_DOWNLOAD_FOLDER = '/mnt/disks/aspera'

def download_a_file(args, aspera_url, file, dst_bucket, TCIA_collection_version, slug, tag):
    # Download the file to disk
    progresslogger.info(f'p{args.id}: Starting aspera download of {file["path"]}')
    try:
        aspera_start = time.time()
        res = run(
            ["ascli", "--progress-bar=no", "--format=json", f'--to-folder={ASPERA_DOWNLOAD_FOLDER}', "faspex5",
             "packages", "receive", f"--url={aspera_url}", file["path"]])
        aspera_delta = time.time() - aspera_start
        if res.stderr:
            errlogger.error(f'p{args.id}: Aspera download failed: {tag}/v{TCIA_collection_version}/{file["basename"]}, {res.stderr}')
            return
        else:
            progresslogger.info(f'p{args.id}: Completed aspera download of {file["path"]}')
    except Exception as exc:
        errlogger.error(
            f'p{args.id}: Aspera download failed: {tag}/v{TCIA_collection_version}/{file["basename"]}, {exc}')
        return

    # Copy to GCS
    try:
        gcs_start = time.time()
        progresslogger.info(f'p{args.id}: Starting GCS transfer of {file["path"]}')
        if tag:
            res = run(['gsutil', '-m', '-q', 'cp', f'{ASPERA_DOWNLOAD_FOLDER}/{file["basename"]}',
                       f'gs://{dst_bucket.name}/{tag}/v{TCIA_collection_version}/{slug}/{file["basename"]}'])
        else:
            res = run(['gsutil', '-m', '-q', 'cp', f'{ASPERA_DOWNLOAD_FOLDER}/{file["basename"]}',
                       f'gs://{dst_bucket.name}/v{TCIA_collection_version}/{slug}/{file["basename"]}'])
        gcs_delta = time.time() - gcs_start

        file_size = os.path.getsize(f'{ASPERA_DOWNLOAD_FOLDER}/{file["basename"]}')
    except Exception as exc:
        errlogger.error(
            f'p{args.id}: Copy to FCS failed: {tag}/v{TCIA_collection_version}/{file["basename"]}, {exc}')
        # Try to delete the file
        run(["rm", f'{ASPERA_DOWNLOAD_FOLDER}/{file["basename"]}'])
        return

    # Delete the file
    try:
        run(["rm", f'{ASPERA_DOWNLOAD_FOLDER}/{file["basename"]}'])
        if res.stderr is None:
            successlogger.info(f'{file["path"]}')
            progresslogger.info(
                f'p{args.id}: {file["path"]}, size: {round(file_size / pow(10, 9), 2)} GB, aspera rate: {round(file_size / aspera_delta / pow(10, 6), 2)} MB/s, GCS rate: {round(file_size / gcs_delta / pow(10, 6), 2)} MB/s')
        else:
            errlogger.error(
                f'p{args.id}: Transfer to gcs failed: {tag}/v{TCIA_collection_version}/{file["basename"]}, {res.stderr}')
    except Exception as exc:
        errlogger.error(f'p{args.id}: Transfer to gcs failed: {tag}/v{TCIA_collection_version}/{file["basename"]}, {exc}')
    return

def worker(input, args, aspera_url, dst_bucket, TCIA_collection_version, slug, tag):
    client = storage.Client()
    for file in iter(input.get, 'STOP'):
        try:
            download_a_file(args, aspera_url, file, dst_bucket, TCIA_collection_version, slug, tag)
        except Exception as exc3:
            errlogger.error(f'p{args.id}: worker, exception type: {repr(exc3)} exception {exc3}')
    return

def download_from_aspera(args, aspera_files, dones, conversion_source_names, aspera_url, bucket_tag, TCIA_collection_version, slug, tag):
    client = storage.Client(args.dst_project)
    dst_bucket = client.bucket(f"{args.dst_bucket_prefix}{bucket_tag}{args.dst_bucket_suffix}")
    if not dst_bucket.exists():
        # bucket = client.create_bucket(f"whc_dev_{bucket_tag}_data", location='us-central1')
        dst_bucket.create(location='us-central1')
    num_processes = min(args.processes, len(aspera_files))
    processes = []
    task_queue = Queue()
    # Start worker processes
    for process in range(num_processes):
        args.id = process + 1
        processes.append(
            Process(group=None, target=worker, args=(task_queue, args, aspera_url, dst_bucket, TCIA_collection_version, slug, tag)))
        processes[-1].start()

    in_aspera_not_idc_source_data = []
    for file in aspera_files:
        aspera_file_name_slashed = '/'.join(file['path'].split('/')[2:]).replace('/', '_').replace('-', '_')
        if not next((name for name in conversion_source_names if
                     name.replace('/', '_').replace('-', '_').find(aspera_file_name_slashed) >= 0),
                    False):
            in_aspera_not_idc_source_data.append(file)
    for file in in_aspera_not_idc_source_data:
        if file["path"] not in dones:
            task_queue.put((file))
        else:
            successlogger.info(f'{file["basename"]} previously processed')

    # Tell child processes to stop
    for i in range(num_processes):
        task_queue.put('STOP')

    # Wait for process to terminate
    for process in processes:
        progresslogger.info(f'Joining process: {process.name}, {process.is_alive()}')
        process.join()


def main(args, download_slugs=[]):
    client = storage.Client(project='idc-dev-etl')
    download = False
    idc_has = False
    gen_manifest = True
    dones = open(successlogger.handlers[0].baseFilename).read().splitlines()

    aspera_package_urls = get_aspera_package_urls()
    for _, package in aspera_package_urls.iterrows():
        if args.download_slugs == [] or package['slug'] in args.download_slugs:
            idc_collection_id = package['IDC_collection_id']
            bucket_tag = bucket_collection_id(idc_collection_id)

            manifest_params = get_collection(args, package)
            aspera_files = manifest_params["aspera_files"]
            conversion_source_names = manifest_params["conversion_source_names"]

            TCIA_collection_version = package['TCIA_collection_version']
            IDC_collection_name = package['IDC_collection_name']
            aspera_url = package['Aspera_URL']
            slug = package['Download_slug']

            if IDC_collection_name in ['NLST', 'ICDC-Glioma']:
                tag = ''
            elif IDC_collection_name == 'CPTAC-CCRCC':
                tag = 'CCRCC'
            else:
                tag = aspera_files[0]['path'].split('/')[1]
            progresslogger.info(manifest_params["logger_string"])
            download_from_aspera(
                args, aspera_files, dones, conversion_source_names,
                aspera_url, bucket_tag,
                TCIA_collection_version, slug, tag)



if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--version", default=21)
    parser.add_argument('--processes', default=1)
    parser.add_argument('--mode', default='download')
    parser.add_argument("--dst_bucket_prefix", default="", help="dst_bucket ID prefix")
    parser.add_argument("--dst_bucket_suffix", default="_pathology_data", help="dst_bucket ID suffix")
    parser.add_argument("--dst_project", default='idc-source-data', help="Project in which to create bucket")
    parser.add_argument("--google_drive_folder", default="", help="Google Drive folder ID")
    parser.add_argument("--save_result", default=True, help="Save result to a Drive file if True")
    parser.add_argument("--download_slugs", default = [], help="Slugs to process; all if empty")
    parser.add_argument("--manifest_file_name", default= f'manifest_{strftime("%Y%m%d_%H%M%S", gmtime())}.txt')
    args = parser.parse_args()
    print(f'args: {json.dumps(args.__dict__, indent=2)}')

    main(args)
