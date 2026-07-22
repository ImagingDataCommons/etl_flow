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

# download_pathology_sources.py downloads files in Aspera packages that are not already in idc-source-data.
# The resulting files are copied to buckets in the idc-source-data project
import os
import argparse
import json
from google.cloud  import storage
import settings
from get_tcia_pathology_metadata import bucket_collection_id, get_blob_metadata_from_package, get_aspera_package_urls, \
    download_files_from_aspera, download_aspera_package
from utilities.logging_config import successlogger, progresslogger, errlogger
from ingestion.utilities.utils import get_merkle_hash
from time import strftime, gmtime
from multiprocessing import Process, Queue
import time
from subprocess import run
import logging
import contextlib

ASPERA_DOWNLOAD_FOLDER = '/mnt/disks/idc-etl/aspera'

def copy_files_to_gcs(args, files, dst_bucket, TCIA_collection_version, slug, tag, aspera_delta):
    try:
        gcs_start = time.time()

        # Configure gsutil so that it will not do multipart uploads
        res = run(['gcloud', 'config', 'set', 'storage/parallel_composite_upload_enabled', 'False'])

        for file in files:
            progresslogger.info(f'p{args.id}: Starting GCS transfer of {file["path"]}')
            if tag:
                if tag.startswith('CMB') or tag in ('AML', 'BRCA', 'CCRCC', 'CM', 'COAD', 'GBM', 'HNSCC', 'LSCC', 'LUAD', 'OV', 'PDA', 'SAR', 'STAD', 'UCEC'):
                    cmmd = ' '.join(['gcloud', 'storage', '-q', 'cp',
                                     f'{ASPERA_DOWNLOAD_FOLDER}/{slug}/p{args.id}{file["path"]}',
                                     f'gs://{dst_bucket.name}/{tag}/v{TCIA_collection_version}/{slug}/'])
                    res = run(['gcloud', 'storage', '-q', 'cp',
                               f'{ASPERA_DOWNLOAD_FOLDER}/{slug}/p{args.id}{file["path"]}',
                               f'gs://{dst_bucket.name}/{tag}/v{TCIA_collection_version}/{slug}/'], check = True)

                else:
                        cmmd = ' '.join(['gcloud', 'storage', '-q', 'cp',
                                         f'{ASPERA_DOWNLOAD_FOLDER}/{slug}/p{args.id}{file["path"]}',
                                         f'gs://{dst_bucket.name}/{tag}/v{TCIA_collection_version}/{slug}{file["path"].rsplit("/", 1)[0]}/'])
                        res = run(['gcloud', 'storage', '-q', 'cp',
                                   f'{ASPERA_DOWNLOAD_FOLDER}/{slug}/p{args.id}{file["path"]}',
                                   f'gs://{dst_bucket.name}/{tag}/v{TCIA_collection_version}/{slug}{file["path"].rsplit("/", 1)[0]}/'], check = True)

            else:
                cmmd = ' '.join(['gcloud', 'storage', '-q', 'cp',
                                 f'{ASPERA_DOWNLOAD_FOLDER}/{slug}/p{args.id}{file["path"]}',
                                 f'gs://{dst_bucket.name}/v{TCIA_collection_version}/{slug}{file["path"].rsplit("/", 1)[0]}/'])
                res = run(['gcloud', 'storage', '-q', 'cp',
                           f'{ASPERA_DOWNLOAD_FOLDER}/{slug}/p{args.id}{file["path"]}',
                           f'gs://{dst_bucket.name}/v{TCIA_collection_version}/{slug}{file["path"].rsplit("/", 1)[0]}/'], check = True)
            if res.stderr is not None:
                errlogger.error(f'p{args.id}: Copy to GCS failed: {tag}/v{TCIA_collection_version}/{file["path"]}, {res.stderr}')
                return
        gcs_delta = time.time() - gcs_start

        files_size = sum(os.path.getsize(f'{ASPERA_DOWNLOAD_FOLDER}/{slug}/p{args.id}{file["path"]}') for file in files)
    except Exception as exc:
        errlogger.error(
            f'p{args.id}: Copy to GCS failed: {tag}/v{TCIA_collection_version}/{file["path"]}, {exc}')
        # Try to delete the file
        run(["rm", f'{ASPERA_DOWNLOAD_FOLDER}/{slug}{file["path"]}'])
        return

    # Delete the file
    try:
        for file in files:
            res = run(["rm", f'{ASPERA_DOWNLOAD_FOLDER}/{slug}/p{args.id}{file["path"]}'])
            if res.stderr is None:
                successlogger.info(f'p{args.id}: {file["path"]}')
            else:
                errlogger.error(
                    f'p{args.id}: Transfer to gcs failed: {tag}/v{TCIA_collection_version}/{file["path"]}, {res.stderr}')
        progresslogger.info(
            f'p{args.id}: {file["path"]}, {len(files)} files, size: {round(files_size / pow(10, 9), 2)} GB, aspera rate: {round(files_size / aspera_delta / pow(10, 6), 2)} MB/s, GCS rate: {round(files_size / gcs_delta / pow(10, 6), 2)} MB/s')
    except Exception as exc:
        errlogger.error(f'p{args.id}: Transfer to gcs failed: {tag}/v{TCIA_collection_version}/{file["path"]}, {exc}')
    return


# Download a list of files from an Aspera package and copy to a specified GCS bucket
def download_aspera_files_to_gcs(args, aspera_url, files, dst_bucket, slug, TCIA_collection_version, tag):
    duration = download_files_from_aspera(args, aspera_url, files, slug, TCIA_collection_version, tag)
    if duration:
        copy_files_to_gcs(args, files, dst_bucket, TCIA_collection_version, slug, tag, duration)
    return


def worker(input, args, aspera_url, dst_bucket, TCIA_collection_version, slug, tag):
    client = storage.Client()
    for files in iter(input.get, 'STOP'):
        try:
            download_aspera_files_to_gcs(args, aspera_url, files, dst_bucket, slug, TCIA_collection_version, tag)
        except Exception as exc3:
            errlogger.error(f'p{args.id}: worker, exception type: {repr(exc3)} exception {exc3}')
    return

# Get a list of the files already in idc-source-data for a particular collection/bucket
def get_conversion_source_names(IDC_collection_name, dst_bucket, bucket_tag):
    storage_client = storage.Client(project='idc-source-data')
    buckets = storage_client.list_buckets()

    sources = []
    dst_bucket_name = dst_bucket.name.lower().replace('-', '_')
    for bucket in buckets:
        if dst_bucket_name in bucket.name.lower().replace('-', '_') or \
                bucket_tag and bucket_tag in bucket.name.lower().replace('-', '_'):
            if bucket.name in ('cmb_pathology_data', 'idc-source-data-cmb'):
                subfolder = f'{IDC_collection_name}/'
            elif bucket.name == 'cptac_pathology_data':
                subfolder = f'{IDC_collection_name.split("-")[1]}/'
            elif bucket.name == 'cptac_pathology_source_data':
                subfolder = f'v1/{IDC_collection_name.split("-")[1]}/'
            else:
                subfolder = ""

            progresslogger.debug(f'Adding bucket {bucket.name}')
            blobs = bucket.list_blobs(prefix=subfolder)
            for blob in blobs:
                sources.append(f"{bucket.name}/{blob.name}")

    # conversion_source_names = set(name.replace('/', '_').replace('-', '_') for name in sources)
    return sources

# Download files that are not already in idc-source-data from an aspera package
def download_from_aspera(args, aspera_files, dones, aspera_url, bucket_tag, \
                         TCIA_collection_version, IDC_collection_name, slug, tag, dst_bucket):
    if not dst_bucket.exists():
        dst_bucket.create(location='us-central1')
    # Get the names of existing source files for the collection of this package
    source_names = get_conversion_source_names(IDC_collection_name, dst_bucket, bucket_tag)
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

    if bucket_tag in ('cmb', 'cptac', 'icdc-glioma', 'nlst'):
        # Contents of these aspera packages are spread across multiple buckets with different
        # naming conventions
        conversion_source_names = []
        if bucket_tag == 'cmb':
            for source_name in source_names:
                if source_name.startswith('cmb_pathology_data'):
                    if source_name.endswith('.svs'):
                        conversion_source_names.append(f'/{tag}/{source_name.split("/",4)[4]}')
                    else:
                        conversion_source_names.append(f'/{source_name.split("/",4)[4]}')
                else:
                    conversion_source_names.append(f'/{source_name.split("/", 1)[1]}')
        elif bucket_tag == 'cptac':
            for source_name in source_names:
                if source_name.startswith('cptac_pathology_data'):
                    if source_name.endswith('.svs'):
                        conversion_source_names.append(f'/{tag}/{source_name.split("/",4)[4]}')
                    else:
                        conversion_source_names.append(f'/{source_name.split("/",4)[4]}')

                else:
                    conversion_source_names.append(f'/{tag}/{source_name.split("/", 3)[3]}')
        else:
            breakpoint() # Add code for NLST and ICDC-Glioma if it's ever needed

        for aspera_file in aspera_files:
            # aspera_file_name_slashed = '/'.join(file['path'].split('/')[2:]).replace('/', '_').replace('-', '_')
            if not next((name for name in conversion_source_names if aspera_file['path'] in name), False):
            # if not next((name for name in conversion_source_names if aspera_files in name), False):
                # in_aspera_not_idc_source_data.append(file)
                task_queue.put([aspera_file])
            else:
                successlogger.info(f'{aspera_file["path"]} previously downloaded')
    else:
        aspera_files_paths = [file["path"] for file in aspera_files]
        source_file_paths = [f"/{source.split('/',3)[3]}" for source in source_names]
        in_aspera_and_not_in_sources = list(set(aspera_files_paths) - set(source_file_paths))
        in_aspera_and_not_in_sources.sort()
        if in_aspera_and_not_in_sources:
            progresslogger.info(f'Files in Aspera and not in idc-source-data: {len(in_aspera_and_not_in_sources)}')
            aspera_files_paths_dict = {file["path"]: file for file in aspera_files}
            file_path = in_aspera_and_not_in_sources[0].rsplit('/',1)[0]
            files = []
            max_files = 64
            file_count = 1
            for file_id in in_aspera_and_not_in_sources:
                file = aspera_files_paths_dict[file_id]
                if file["path"].rsplit('/',1)[0] == file_path and file_count < max_files:
                    files.append(file)
                    file_count += 1
                else:
                    # Get here if file_count==max_files or the first level subdirectory changes with the next file
                    task_queue.put(files)
                    file_count = 1
                    file_path = file["path"].rsplit('/', 1)[0]
                    files = [file]
            if files:
                task_queue.put(files)
        pass
    for i in range(num_processes):
        task_queue.put('STOP')

    # Wait for process to terminate
    for process in processes:
        progresslogger.info(f'Joining process: {process.name}, {process.is_alive()}')
        process.join()
    return

@contextlib.contextmanager
def temp_logger(aspera_url_hash):

    for hdlr in successlogger.handlers[:]:
        successlogger.removeHandler(hdlr)
    os.makedirs(f'{settings.LOG_DIR}/{aspera_url_hash}', exist_ok=True)

    success_fh = logging.FileHandler(f'{settings.LOG_DIR}/{aspera_url_hash}/success.log')
    successlogger.addHandler(success_fh)
    successformatter = logging.Formatter('%(message)s')
    success_fh.setFormatter(successformatter)

    for hdlr in progresslogger.handlers[:]:
        progresslogger.removeHandler(hdlr)
    progress_fh = logging.FileHandler(f'{settings.LOG_DIR}/{aspera_url_hash}/progress.log')
    progresslogger.addHandler(progress_fh)
    successformatter = logging.Formatter('%(message)s')
    progress_fh.setFormatter(successformatter)

    # Always empty the error file
    with open(f'{settings.LOG_DIR}/{aspera_url_hash}/error.log', 'w') as f:
        pass
    for hdlr in errlogger.handlers[:]:
        errlogger.removeHandler(hdlr)
    err_fh = logging.FileHandler(f'{settings.LOG_DIR}/{aspera_url_hash}/error.log')
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    try:
        yield
    finally:
        successlogger.removeHandler(success_fh)
        progress_fh = logging.FileHandler(f'{settings.LOG_DIR}/{"success.log"}')
        progresslogger.addHandler(progress_fh)
        successformatter = logging.Formatter('%(message)s')
        progress_fh.setFormatter(successformatter)

        progresslogger.removeHandler(progress_fh)
        progress_fh = logging.FileHandler('{}/progress.log'.format(settings.LOG_DIR))
        progresslogger.addHandler(progress_fh)
        successformatter = logging.Formatter('%(message)s')
        progress_fh.setFormatter(successformatter)

        errlogger.removeHandler(err_fh)
        for hdlr in errlogger.handlers[:]:
            errlogger.removeHandler(hdlr)
        err_fh = logging.FileHandler('{}/error.log'.format(settings.LOG_DIR))
        errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
        errlogger.addHandler(err_fh)
        err_fh.setFormatter(errformatter)
    return


def main(args, download_slugs=[]):
    aspera_package_urls = get_aspera_package_urls().sort_values(by="Download_slug")
    for _, package in aspera_package_urls.iterrows():
        aspera_url_hash = f"{package['Download_slug']}-{get_merkle_hash([package['Aspera_URL']])}"
        # with temp_logger(package['Download_slug']):
        with temp_logger(aspera_url_hash):
            TCIA_collection_version = package['TCIA_collection_version']
            TCIA_collection_name = package['TCIA_collection_name']
            IDC_collection_name = package['IDC_collection_name']
            idc_collection_id = package['IDC_collection_id']
            client = storage.Client(args.dst_project)
            bucket_tag = bucket_collection_id(
                idc_collection_id if idc_collection_id else package["TCIA_collection_id"])
            # Bucket name length must be 63 or less, so we truncate the collection part
            dst_bucket = client.bucket(f"{args.dst_bucket_prefix}{bucket_tag}"[:63-len(args.dst_bucket_suffix)] +
                                       f"{args.dst_bucket_suffix}")
            if TCIA_collection_name.startswith('CPTAC'):
                tag = TCIA_collection_name.split('-')[1]
            elif TCIA_collection_name.startswith('CMB'):
                tag = TCIA_collection_name
            else:
                tag = ""
            aspera_download_slug = package['Download_slug']
            aspera_url = package['Aspera_URL'].split('&')[0] if '&' in package['Aspera_URL'] \
                else package['Aspera_URL']
            aspera_id = aspera_url.rsplit('context=', 1)[1]
            if tag:
                aspera_blob_id = f'{tag}/v{TCIA_collection_version}/{aspera_download_slug}/{aspera_id}'
            else:
                aspera_blob_id = f'v{TCIA_collection_version}/{aspera_download_slug}/{aspera_id}'
            aspera_id_blob = dst_bucket.blob(aspera_blob_id)

            # If we already have this Aspera ID in this TCIA version, then we are done.
            # Note that an Aspera package in a new TCIA version might well be unchanged and thus have the same Aspera ID
            if aspera_id_blob.exists() and aspera_download_slug not in args.download_slugs:
                progresslogger.info(f'Aspera package {aspera_download_slug} is up to date')
            else:
                if package["TCIA_collection_id"] in args.skip:
                    progresslogger.info(f'Skipping {package["Download_slug"]} in args.skips')
                else:
                    dones = open(successlogger.handlers[0].baseFilename).read().splitlines()
                    if aspera_download_slug in dones and not args.ignore_dones and not aspera_download_slug in args.download_slugs :
                        aspera_id_blob.upload_from_string('')
                        progresslogger.info(f'Aspera package {package["Download_slug"]} previously processed')
                    else:
                        if idc_collection_id or args.only_idc_collections==False:
                            progresslogger.info(f'Processing {aspera_download_slug}')
                            # download_aspera_package(args, aspera_url, aspera_url_hash, TCIA_collection_version, tag )
                            # If a json listing of the files in this package exists on disk:
                            if args.load_aspera_files and os.path.exists(f"{settings.LOG_DIR}/{aspera_url_hash}/{aspera_download_slug}_files.json"):
                                with open(f"{settings.LOG_DIR}/{aspera_url_hash}/{aspera_download_slug}_files.json") as f:
                                    aspera_files = json.load(f)
                            else:
                                manifest_params = get_blob_metadata_from_package(args, package, args.version)
                                aspera_files = manifest_params["aspera_files"]
                                with open(f"{settings.LOG_DIR}/{aspera_url_hash}/{aspera_download_slug}_files.json", 'w') as f:
                                    json.dump(aspera_files, f)
                                progresslogger.info(manifest_params["logger_string"])
                            download_from_aspera(
                                args, aspera_files, dones,
                                aspera_url, bucket_tag,
                                TCIA_collection_version, TCIA_collection_name, aspera_download_slug, tag, dst_bucket)
                            with open(errlogger.handlers[0].baseFilename) as f:
                                errors = f.read().splitlines()
                            if errors:
                                progresslogger.info(f'Non-zero errors.')
                            else:
                                # Create an empty blob that uses the Aspera URL as a name
                                # This can be compared to the Aspera URL in future executions of this script
                                # Existence of a matching blob should mean that the source data is up to date
                                aspera_id_blob.upload_from_string('')
                        else:
                            progresslogger.info(f'Skipping TCIA collection {package["TCIA_collection_id"]}')
    return

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--version", default=settings.CURRENT_VERSION-1)
    parser.add_argument('--processes', default=32)
    parser.add_argument('--mode', default='download')
    parser.add_argument("--dst_bucket_prefix", default="", help="dst_bucket ID prefix")
    parser.add_argument("--dst_bucket_suffix", default="_pathology_data", help="dst_bucket ID suffix")
    parser.add_argument("--dst_project", default='idc-source-data', help="Project in which to create bucket")
    parser.add_argument("--google_drive_folder", default="", help="Google Drive folder ID")
    parser.add_argument("--save_result", default=True, help="Save result to a Drive file if True")
    parser.add_argument("--download_slugs", default = [], help="Slugs to process; all if empty")
    parser.add_argument("--manifest_file_name", default= f'manifest_{strftime("%Y%m%d_%H%M%S", gmtime())}.txt')
    parser.add_argument("--only_idc_collections", default=False, help="Only include a collection if IDC already has it")
    parser.add_argument("--skip", default=["hungarian-colorectal-screening"], help="TCIA_collection_ids to be skipped")
    parser.add_argument("--load_aspera_files", default=False)
    args = parser.parse_args()
    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    # Default process ID
    args.id = 0


    main(args)
