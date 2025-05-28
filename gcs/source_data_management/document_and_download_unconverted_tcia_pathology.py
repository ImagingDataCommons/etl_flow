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
from io import StringIO
from subprocess import run
from tcia_sourced_pathology_files import tcia_sourced_pathology_files

import settings
from utilities.logging_config import successlogger, progresslogger, warninglogger, errlogger
from google.cloud import storage, bigquery
from multiprocessing import Process, Queue
import time

BATCH_SIZE = pow(2,20)

ASPERA_DOWNLOAD_FOLDER = '/mnt/disks/ssd/aspera'

def get_aspera_package_urls():
    client = bigquery.Client()
    query = f"""
    SELECT *
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.tcia_pathology_conversion_status` 
    WHERE IDC_collection_id != ""
    """
    aspera_packages = client.query(query).to_dataframe()
    aspera_packages.sort_values("IDC_collection_id", inplace=True)
    return aspera_packages


def get_aspera_package_files(files, directory, url):
    MAX_TRIES = 10
    LIMIT = 1000
    offset = 0
    tries = 0
    while True:
        cmmd = 'ascli --format=json faspex5 packages browse --query=@json:\'{"limit":' + str(LIMIT) + ',"offset":' + str(offset) + "}'" + \
               f' --url={url} {directory}'
        result = run(cmmd, capture_output=True, shell=True)
        if not result.stderr.startswith(b'ERROR'):
            some_files = json.load(StringIO(result.stdout.decode()))
            if len(some_files) > 0:
                for file in some_files:
                    if file['type'] == 'directory':
                        files = get_aspera_package_files(files, f"{directory}/{file['basename']}", url)
                    elif not file["basename"].endswith("sums"):
                        files.append(file)
                offset += len(some_files)
                if len(some_files) < LIMIT:
                    break
            else:
                break
        else:
            errlogger.error(f'Failed to get new file list')
            tries += 1
            if tries == MAX_TRIES:
                errlogger.info(f"Failed to download aspera package, attempt {tries}")
                break
    return files

def gen_manifest_of_idc_pathology_source(TCIA_collection_id, slug, aspera_file_names_dashed, ingested_urls, aspera_files):
    successlogger.info(f"TCIA {TCIA_collection_id}/{slug}")
    idc_has_slashed =sorted(list(aspera_file_names_dashed & ingested_urls))
    for i in idc_has_slashed:
        for j in aspera_files:
            if i == j["path"].split('/', 2)[2].replace('/', '_'):
                successlogger.info(j["path"])
    successlogger.info(" ")
    return

def gen_manifest_of_idc_missing_pathology_source(slug, aspera_file_names_slashed,
                                                 conversion_source_names, ingested_urls, aspera_files):
    in_aspera_not_idc_source_data = []
    for file in aspera_file_names_slashed:
        if not next((name for name in conversion_source_names if name.replace('/', '_').replace('-', '_').find(file.replace('/', '_').replace('-', '_')) >= 0), False):
            in_aspera_not_idc_source_data.append(file)
    successlogger.info(f"**** {len(in_aspera_not_idc_source_data)} files in Aspera package but not in idc-source-data")
    if in_aspera_not_idc_source_data:
        for file in in_aspera_not_idc_source_data:
            successlogger.info(file)

    in_aspera_and_in_idc_source_data = []
    for file in aspera_file_names_slashed:
        if next((name for name in conversion_source_names if name.replace('/', '_').replace('-', '_').find(file.replace('/', '_').replace('-', '_')) >= 0), False):
            in_aspera_and_in_idc_source_data.append(file)
    in_idc_source_data_not_ingested_urls = []
    for file in in_aspera_and_in_idc_source_data:
        if not next((name for name in ingested_urls if name.replace('/', '_').replace('-', '_').find(file.replace('/', '_').replace('-', '_')) >= 0), False):
            in_idc_source_data_not_ingested_urls.append(file)
    successlogger.info(f"**** {len(in_idc_source_data_not_ingested_urls)} files in idc-source-data but not in ingestion urls ****")
    for file in in_idc_source_data_not_ingested_urls:
        successlogger.info(file)

    in_ingested_urls_not_idc_source_data = []
    if slug not in ['cptac-ccrcc-da-path-nonccrcc', 'nlst-da-path-1', 'nlst-da-path-2']:
        for file in ingested_urls:
            if not next((name for name in ingested_urls if
                         name.replace('/', '_').replace('-', '_').find(file.replace('/', '_').replace('-', '_')) >= 0),
                        False):
                in_ingested_urls_not_idc_source_data.append(file)
        successlogger.info(
            f"**** {len(in_ingested_urls_not_idc_source_data)} files in ingestion urls but not in idc-source-data ****")
        for file in in_ingested_urls_not_idc_source_data:
            successlogger.info(file)
    else:
        successlogger.info(
            f"**** 0 files in ingestion urls but not in idc-source-data ****")
    return

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


def get_revised_blobs(args, package, bucket_tag, conversion_sources, ingested_conversion_results, download=True, gen_manifest=False, idc_has=False):
    client = storage.Client(project='idc-dev-etl')
    # Get list of (svs) aspera_files in package
    TCIA_collection_id = package['TCIA_collection_id']
    TCIA_collection_version = package['TCIA_collection_version']
    IDC_collection_name = package['IDC_collection_name']
    IDC_collection_version = package['IDC_collection_version']
    aspera_url = package['Aspera_URL']
    # package_date = package['Modified_download_date']
    aspera_files = get_aspera_package_files([], "", aspera_url)
    if IDC_collection_name in ['NLST', 'ICDC-Glioma']:
        tag = ''
    elif IDC_collection_name == 'CPTAC-CCRCC':
        tag = 'CCRCC'
    else:
        tag = aspera_files[0]['path'].split('/')[1]

    slug = package['Download_slug']
    ingested_urls = set(ingested_conversion_results['url'])
    # Drop initial directory name from aspera_files
    aspera_file_names_slashed = set(['/'.join(file['path'].split('/')[2:]) for file in aspera_files])
    successlogger.info(f"TCIA {TCIA_collection_id}:v{TCIA_collection_version}/{slug}-->{len(aspera_file_names_slashed)} Aspera package urls; IDC {IDC_collection_name}:v{IDC_collection_version}-->{len(ingested_urls)} ingested urls")

    if idc_has:
        aspera_file_names_dashed = set(file.replace("/", "_").replace('-', '_') for file in aspera_file_names_slashed)
        gen_manifest_of_idc_pathology_source(TCIA_collection_id, slug, aspera_file_names_dashed, ingested_urls, aspera_files)
    if gen_manifest:
        conversion_source_names = set(conversion_sources['name'])
        gen_manifest_of_idc_missing_pathology_source(slug, aspera_file_names_slashed,
                                                     conversion_source_names, ingested_urls, aspera_files)
    if download:
        dones = open(successlogger.handlers[0].baseFilename).read().splitlines()
        # download_from_aspera(args, aspera_files, dones, ingested_conversion_results, aspera_url, bucket_tag,
        #                      TCIA_collection_version, slug, tag)
        conversion_source_names = set(conversion_sources['name'])
        download_from_aspera(args, aspera_files, dones, conversion_source_names, aspera_url, bucket_tag,
                         TCIA_collection_version, slug, tag)
    successlogger.info(" ")
    return


def get_ingested_idc_converted_data_files(collection_id):
    client = bigquery.Client()
    query = f"""
SELECT DISTINCT ajp.collection_id, 
ingestion_url,
if(ENDS_WITH(ingestion_url, '.dcm'),  CONCAT(SPLIT(SPLIT(ingestion_url, '/')[ARRAY_LENGTH(SPLIT(ingestion_url, '/'))-1], '.dcm')[0], '.svs'),   CONCAT(SPLIT(ingestion_url, '/')[ARRAY_LENGTH(SPLIT(ingestion_url, '/'))-2], '.svs')) url
FROM `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.all_joined_public` ajp
JOIN `idc-dev-etl.{settings.BQ_DEV_EXT_DATASET}.dicom_all` da
ON ajp.i_uuid=da.crdc_instance_uuid
WHERE REPLACE(REPLACE(LOWER(ajp.collection_id), '-', '_'), ' ','_') = '{collection_id}' and i_source='idc' and modality='SM'
ORDER by collection_id
"""
    sources = client.query(query).result().to_dataframe()
    return sources


def get_collection(args, package, bucket_tag, download=True, gen_manifest=False, idc_has=False):
    idc_collection_id = package['IDC_collection_id']
    conversion_sources = tcia_sourced_pathology_files()
    ingested_conversion_results = get_ingested_idc_converted_data_files(idc_collection_id)
    get_revised_blobs(args, package, bucket_tag, conversion_sources, ingested_conversion_results, download=download, gen_manifest=gen_manifest, idc_has=idc_has)
    return


def bucket_collection_id(collection_id):
    bucket_collection_id = {
        'cmb': 'cmb',
        'cptac': 'cptac',
        'nlst': 'nlst',
        'icdc': 'icdc_glioma'
    }[collection_id.split('_')[0]]
    return bucket_collection_id


def main(args, download_slugs=[]):
    client = storage.Client(project='idc-dev-etl')
    download = False
    idc_has = False
    gen_manifest = False
    if args.mode =='idc_has':
        idc_has = True
    elif args.mode == 'gen_manifest':
        gen_manifest = True
    elif args.mode == 'download':
        download = True
    else:
        errlogger.error(f'Invalid mode {args.mode}')
        exit(1)
    aspera_package_urls = get_aspera_package_urls()
    if download_slugs:
        for download_slug in download_slugs:
            for _, package in aspera_package_urls.iterrows():
                if package['Download_slug'] == download_slug:
                    idc_collection_id = package['IDC_collection_id']
                    bucket_tag = bucket_collection_id(idc_collection_id)
                    get_collection(args, package, bucket_tag, download=download, idc_has=idc_has, gen_manifest=gen_manifest)
    else:
        for _, package in aspera_package_urls.iterrows():
            idc_collection_id = package['IDC_collection_id']
            bucket_tag = bucket_collection_id(idc_collection_id)
            get_collection(args, package, bucket_tag, download=download, idc_has=idc_has, gen_manifest=gen_manifest)



