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
import os
import argparse
import json
from google.cloud  import storage
import settings
from get_tcia_pathology_metadata import bucket_collection_id, get_blob_metadata_from_package, get_aspera_package_urls
from utilities.logging_config import successlogger, progresslogger, errlogger
from time import strftime, gmtime
from multiprocessing import Process, Queue
import time
from subprocess import run
from tcia_sourced_pathology_files import tcia_sourced_pathology_files
from io import StringIO
from ingestion.utilities.utils import md5_hasher
from base64 import b64decode


ASPERA_DOWNLOAD_FOLDER = '/mnt/disks/aspera'

def download_a_file_from_aspera(args, aspera_url, file, TCIA_collection_version="", tag=""):
    # Download the file to disk
    args_id = args.id if 'id' in args else 0
    progresslogger.info(f'p{args_id}: Starting aspera download of {file["path"]}')
    try:
        aspera_start = time.time()
        res = run(
            ["ascli", "--progress-bar=no", "--format=json", f'--to-folder={ASPERA_DOWNLOAD_FOLDER}', "faspex5",
             "packages", "receive", f"--url={aspera_url}", file["path"]])
        aspera_delta = time.time() - aspera_start
        if res.stderr:
            errlogger.error(f'p{args.id}: Aspera download failed: {tag}/v{TCIA_collection_version}/{file["basename"]}, {res.stderr}')
            return 0
        else:
            progresslogger.info(f'p{args_id}: Completed aspera download of {file["path"]}')
            return aspera_delta

    except Exception as exc:
        errlogger.error(
            f'p{args_id}: Aspera download failed: {tag}/v{TCIA_collection_version}/{file["basename"]}, {exc}')
        return 0

    # Copy to GCS
def copy_file_to_gcs(args, file, dst_bucket, TCIA_collection_version, slug, tag, aspera_delta):
    try:
        gcs_start = time.time()
        progresslogger.info(f'p{args.id}: Starting GCS transfer of {file["path"]}')
        if tag:
            res = run(['gsutil', '-m', '-q', 'cp', f'{ASPERA_DOWNLOAD_FOLDER}/{file["path"]}',
                       f'gs://{dst_bucket.name}/{tag}/v{TCIA_collection_version}/{slug}/{file["path"]}'])
        else:
            res = run(['gsutil', '-m', '-q', 'cp', f'{ASPERA_DOWNLOAD_FOLDER}/{file["path"]}',
                       f'gs://{dst_bucket.name}/v{TCIA_collection_version}/{slug}/{file["path"]}'])
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
            successlogger.info(f'p{args.id}: {file["path"]}')
            progresslogger.info(
                f'p{args.id}: {file["path"]}, size: {round(file_size / pow(10, 9), 2)} GB, aspera rate: {round(file_size / aspera_delta / pow(10, 6), 2)} MB/s, GCS rate: {round(file_size / gcs_delta / pow(10, 6), 2)} MB/s')
        else:
            errlogger.error(
                f'p{args.id}: Transfer to gcs failed: {tag}/v{TCIA_collection_version}/{file["basename"]}, {res.stderr}')
    except Exception as exc:
        errlogger.error(f'p{args.id}: Transfer to gcs failed: {tag}/v{TCIA_collection_version}/{file["basename"]}, {exc}')
    return

def download_aspera_file_to_gcs(args, aspera_url, file, dst_bucket, TCIA_collection_version, slug, tag):
    duration = download_a_file_from_aspera(args, aspera_url, file, TCIA_collection_version, tag)
    copy_file_to_gcs(args, file, dst_bucket, TCIA_collection_version, slug, tag, duration)
    return

# If the Aspera
def sums_files_match(args, package, dst_bucket, dst_blob_path) -> bool:
    MAX_TRIES = 10
    tries = 0
    aspera_url = package["Aspera_URL"]

    while True:
        cmmd = 'ascli --format=json faspex5 packages browse ' + \
               f' --url={aspera_url}'
        result = run(cmmd, capture_output=True, shell=True)
        # if not result.stderr.startswith(b'ERROR'):
        if not result.stderr:
            some_files = json.load(StringIO(result.stdout.decode()))
            if len(some_files) > 0:
                for file in some_files:
                    if file["basename"].endswith("sums"):
                        try:
                            result = download_a_file_from_aspera(args, aspera_url, file)
                            if result!=0:
                                sums_file_hash = md5_hasher(f'{ASPERA_DOWNLOAD_FOLDER}/{file["path"]}')
                                run(["rm", f'{ASPERA_DOWNLOAD_FOLDER}/{file["basename"]}'])
                                sums_blob = dst_bucket.blob(f'{dst_blob_path}/{file["basename"]}')
                                if sums_blob.exists():
                                    sums_blob.reload()
                                    sums_blob_hash = b64decode(sums_blob.md5_hash).hex()
                                    return sums_file_hash == sums_blob_hash
                                else:
                                    return False
                            else:
                                errlogger.error(f'Failed to download sums file, {file}')
                                exit
                        except Exception as exc:
                            errlogger.error(exc)
            break
        else:
            errlogger.error(f'Failed to get new file list, {result.stderr}')
            tries += 1
            if tries == MAX_TRIES:
                errlogger.info(f"Failed to download aspera package, attempt {tries}, {result.stderr}")
                break

    return False


def worker(input, args, aspera_url, dst_bucket, TCIA_collection_version, slug, tag):
    client = storage.Client()
    for file in iter(input.get, 'STOP'):
        try:
            download_aspera_file_to_gcs(args, aspera_url, file, dst_bucket, TCIA_collection_version, slug, tag)
        except Exception as exc3:
            errlogger.error(f'p{args.id}: worker, exception type: {repr(exc3)} exception {exc3}')
    return

def get_conversion_source_names(dst_bucket):
    storage_client = storage.Client(project='idc-source-data')
    buckets = storage_client.list_buckets()

    sources = []
    dst_bucket_name = dst_bucket.name.lower().replace('-', '_')
    for bucket in buckets:
        if dst_bucket_name in bucket.name.lower().replace('-', '_'):
            progresslogger.debug(f'Adding bucket {bucket.name}')
            blobs = bucket.list_blobs()
            for blob in blobs:
                sources.append(f"{bucket.name}/{blob.name}")

    conversion_source_names = set(name.replace('/', '_').replace('-', '_') for name in sources),
    # df = pd.DataFrame(sources)
    return conversion_source_names



def download_from_aspera(args, aspera_files, dones, aspera_url, bucket_tag, TCIA_collection_version, slug, tag):
    client = storage.Client(args.dst_project)
    dst_bucket = client.bucket(f"{args.dst_bucket_prefix}{bucket_tag}{args.dst_bucket_suffix}")
    if not dst_bucket.exists():
        # bucket = client.create_bucket(f"whc_dev_{bucket_tag}_data", location='us-central1')
        dst_bucket.create(location='us-central1')
    conversion_source_names = get_conversion_source_names(dst_bucket)
    num_processes = min(args.processes, len(aspera_files))
    processes = []
    task_queue = Queue()
    # Start worker processes
    for process in range(num_processes):
        args.id = process + 1
        processes.append(
            Process(group=None, target=worker, args=(task_queue, args, aspera_url, dst_bucket, TCIA_collection_version, slug, tag)))
        processes[-1].start()

    strt = time.time()
    in_aspera_not_idc_source_data = []
    for file in aspera_files:
         aspera_file_name_slashed = '/'.join(file['path'].split('/')[2:]).replace('/', '_').replace('-', '_')
         if not next((name for name in conversion_source_names if aspera_file_name_slashed in name), False):
                in_aspera_not_idc_source_data.append(file)
    duration = time.time() - strt
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
    dones = open(successlogger.handlers[0].baseFilename).read().splitlines()
    aspera_package_urls = get_aspera_package_urls()
    for _, package in aspera_package_urls.iterrows():
        if package["TCIA_collection_id"] in args.skip:
            progresslogger.info(f'Skipping {package["TCIA_collection_id"]} in args.skips')
        else:
            if package["Download_slug"] not in dones:
                if args.download_slugs == [] or package['Download_slug'] in args.download_slugs:
                    idc_collection_id = package['IDC_collection_id']
                    if idc_collection_id or args.only_idc_collections==False:
                        TCIA_collection_version = package['TCIA_collection_version']
                        IDC_collection_name = package['IDC_collection_name']
                        aspera_url = package['Aspera_URL'].split('&')[0] if '&' in package['Aspera_URL'] \
                            else package['Aspera_URL']
                        slug = package['Download_slug']
                        if IDC_collection_name in ['NLST', 'ICDC-Glioma']:
                            tag = ''
                        elif IDC_collection_name == 'CPTAC-CCRCC':
                            tag = 'CCRCC'
                        else:
                            tag = ""
                        bucket_tag = bucket_collection_id(
                            idc_collection_id if idc_collection_id else package["TCIA_collection_id"])
                        progresslogger.info(f'Processing {bucket_tag}')
                        client = storage.Client(args.dst_project)
                        dst_bucket = client.bucket(f"{args.dst_bucket_prefix}{bucket_tag}{args.dst_bucket_suffix}")
                        dst_blob_path= f'{tag}/v{TCIA_collection_version}/{slug}' if tag else f'v{TCIA_collection_version}/{slug}'
                        if dst_bucket.exists and sums_files_match(args, package, dst_bucket, dst_blob_path):
                            # If there is a sums file in GCS that matches the file in the Aspera package, skip
                            progresslogger.info((f'Skipping package {package["Download_slug"]}. Sums files match'))
                            successlogger.info(package["Download_slug"])
                        else:

                            if args.load_aspera_files:
                                with open(f"{slug}_files.json") as f:
                                    aspera_file = json.load(f)
                            else:
                                manifest_params = get_blob_metadata_from_package(args, package, args.version)
                                aspera_files = manifest_params["aspera_files"]
                                progresslogger.info(manifest_params["logger_string"])
                            download_from_aspera(
                                args, aspera_files, dones,
                                aspera_url, bucket_tag,
                                TCIA_collection_version, slug, tag)
                            successlogger.info(package["Download_slug"])
                    else:
                        progresslogger.info(f'Skipping TCIA collection {package["TCIA_collection_id"]}')
            else:
                progresslogger.info(f'TCIA collection {package["TCIA_collection_id"]} previously processed')

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--version", default=settings.CURRENT_VERSION-1)
    parser.add_argument('--processes', default=4)
    parser.add_argument('--mode', default='download')
    parser.add_argument("--dst_bucket_prefix", default="", help="dst_bucket ID prefix")
    parser.add_argument("--dst_bucket_suffix", default="_pathology_data", help="dst_bucket ID suffix")
    parser.add_argument("--dst_project", default='idc-source-data', help="Project in which to create bucket")
    parser.add_argument("--google_drive_folder", default="", help="Google Drive folder ID")
    parser.add_argument("--save_result", default=True, help="Save result to a Drive file if True")
    parser.add_argument("--download_slugs", default = [], help="Slugs to process; all if empty")
    parser.add_argument("--manifest_file_name", default= f'manifest_{strftime("%Y%m%d_%H%M%S", gmtime())}.txt')
    parser.add_argument("--only_idc_collections", default=False, help="Only include a collection if IDC already has it")
    # parser.add_argument("--skip", default=['aurora-metastatic-breast-multiomics',"hancock", 'histologyhsi-bc-recurrence'], help="TCIA_collection_ids to be skipped")
    parser.add_argument("--skip", default=["hancock"], help="TCIA_collection_ids to be skipped")
    parser.add_argument("--load_aspera_files", default=True)
    args = parser.parse_args()
    print(f'args: {json.dumps(args.__dict__, indent=2)}')

    main(args)
