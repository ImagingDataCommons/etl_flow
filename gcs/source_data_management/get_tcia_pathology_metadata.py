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

import settings
from utilities.logging_config import progresslogger, errlogger
from google.cloud import storage, bigquery
from tcia_sourced_pathology_files import tcia_sourced_pathology_files
import time

BATCH_SIZE = pow(2,20)

ASPERA_DOWNLOAD_FOLDER = '/mnt/disks/idc-etl/aspera'

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


def download_aspera_package(args, aspera_url, slug, TCIA_collection_version="", tag="", \
                               json_params=""):
    # Download the file to disk
    args_id = args.id if 'id' in args else 0
    progresslogger.info(f'p{args_id}: Starting aspera download of {slug} ')

    try:
        MAX_TRIES = 6
        tries = MAX_TRIES
        aspera_start = time.time()
        cmmd = ' '.join(["ascli",
                         # "--log-level=debug",
                         "--progress-bar=no",
                         "--format=json",
                         f'--to-folder={ASPERA_DOWNLOAD_FOLDER}/{slug}/p{args.id}',
                         "faspex5",
                         "packages",
                         "receive",
                         "--query=@json:'{\"recursive\": true}'",
                         f"--url={aspera_url}"])
        while True:
            res = run(cmmd, capture_output=True, shell=True)

            aspera_delta = time.time() - aspera_start
            if res.stderr:
                if tries:
                    tries -= 1
                    progresslogger.info(
                            f'p{args.id}: Aspera download failure {MAX_TRIES-tries}: {tag}/v{TCIA_collection_version}, stderr: {res.stderr}')
                    time.sleep(pow(2, MAX_TRIES - tries))
                    continue
                else:
                    errlogger.error(
                            f'p{args.id}: Aspera download failed: {tag}/v{TCIA_collection_version}, stderr: {res.stderr}')
                    errlogger.error(f'p{args.id}: cmmd: {cmmd}')
                    return 0

                return 0
            else:
                progresslogger.info(f'p{args_id}: Completed aspera download')
                return aspera_delta

    except Exception as exc:
        errlogger.error(
            f'p{args_id}: Aspera download failed: {tag}/v{TCIA_collection_version}, {exc}')
        errlogger.error(f'cmmd: {cmmd}')
        return 0

def download_files_from_aspera(args, aspera_url, files, slug, TCIA_collection_version="", tag="", \
                               json_params=""):
    # Download the file to disk
    args_id = args.id if 'id' in args else 0
    progresslogger.info(f'p{args_id}: Starting aspera download of {len(files)} files from {files[0]["path"]} ')

    try:
        MAX_TRIES = 6
        tries = MAX_TRIES
        aspera_start = time.time()
        escaped_file_paths = " ".join([file["path"].replace(" ", "\ " ).replace("&", "\&") for file in files])
        escaped_file_path = files[0]["path"].replace(" ", "\ " ).replace("&", "\&").rsplit("/", 1)[0]
        cmmd = ' '.join(["ascli",
                         # "--log-level=debug",
                         "--progress-bar=no",
                         "--format=json",
                         f'--to-folder={ASPERA_DOWNLOAD_FOLDER}/{slug}/p{args.id}{escaped_file_path}',
                         "faspex5",
                         "packages",
                         "receive",
                         json_params,
                         f"--url={aspera_url}",
                         escaped_file_paths])
        while True:
            res = run(cmmd, capture_output=True, shell=True)

            aspera_delta = time.time() - aspera_start
            if res.stderr:
                if tries:
                    tries -= 1
                    progresslogger.info(
                            f'p{args.id}: Aspera download failure {MAX_TRIES-tries}: {tag}/v{TCIA_collection_version}/{files[0]["path"]}, stderr: {res.stderr}')
                    time.sleep(pow(2, MAX_TRIES - tries))
                    continue
                else:
                    errlogger.error(
                            f'p{args.id}: Aspera download failed: {tag}/v{TCIA_collection_version}/{files[0]["path"]}, stderr: {res.stderr}')
                    errlogger.error(f'p{args.id}: cmmd: {cmmd}')
                    return 0

                return 0
            else:
                continue_retries = False
                for file in files:
                    if not os.path.exists(f'{ASPERA_DOWNLOAD_FOLDER}/{slug}/p{args.id}{file["path"]}'):
                        if tries:
                            tries -= 1
                            progresslogger.info(
                                f'p{args.id}: Aspera download failure {MAX_TRIES-tries}: {tag}/v{TCIA_collection_version}/{file["path"]}; file doesn\'t exist. Retrying')
                            time.sleep(pow(2,MAX_TRIES-tries))
                            continue_retries = True
                            break
                        else:
                            errlogger.error(
                                f'p{args.id}: Aspera download failed: {tag}/v{TCIA_collection_version}/{file["path"]}; file doesn\'t exist. Aborting.')
                            errlogger.error(f'p{args.id}: cmmd: {cmmd}')
                            return 0
                if continue_retries:
                    continue
                progresslogger.info(f'p{args_id}: Completed aspera download of {files[0]["path"]}')
                return aspera_delta

    except Exception as exc:
        errlogger.error(
            f'p{args_id}: Aspera download failed: {tag}/v{TCIA_collection_version}/{file["path"]}, {exc}')
        errlogger.error(f'cmmd: {cmmd}')
        return 0


def get_aspera_package_files(args, files, directory, url, indent=""):

    MAX_TRIES = 6
    tries = 0

    progresslogger.info(f'{indent}Directory {directory}')
    while True:
        # Escapes some characters that might be in the directory
        # escaped_directory = directory.replace(' ', '\ ').replace('&', '\&')
        cmmd = ('ascli --format=json faspex5 packages browse ' + \
                "--query=@json:'{\"per_page\": 10}'" + \
                f' --url={url} {directory}')
        result = run(cmmd, capture_output=True, shell=True)
        if not result.stderr:
            some_files = json.load(StringIO(result.stdout.decode()))
            # No sums file in this directory. Proceed to walk the rest of the subdirectories and files
            progresslogger.info(f'{indent}{directory}: {len(some_files)} files')
            if len(some_files) > 0:
                for file in some_files:
                    if file['type'] == 'directory':
                        try:
                            escaped_subdir = file['basename'].replace(' ', '\ ').replace('&', '\&')
                            files = get_aspera_package_files(args, files, f"{directory}/{escaped_subdir}", url, indent+"   ")
                        except Exception as exc:
                            errlogger.error(exc)
                    # elif not file["basename"].endswith("sums"):
                    else:
                        try:
                            files.append(file)
                        except Exception as exc:
                            errlogger.error(exc)
            break
        else:
            progresslogger.info(f'{indent}    Failing to get Aspera package file list, attempt {tries+1}, {result.stderr}')
            tries += 1
            time.sleep(min(64,pow(2, tries)))
            if tries == MAX_TRIES:
                errlogger.error(f"{indent}**  Failed to get Aspera package file list, {result.stderr}")
                break

    return files


def get_blob_metadata(args, package, ingested_conversion_results):
    client = storage.Client(project='idc-dev-etl')
    # Get list of (svs) aspera_files in package
    TCIA_collection_id = package['TCIA_collection_id']
    TCIA_collection_version = package['TCIA_collection_version']
    IDC_collection_name = package['IDC_collection_name']
    IDC_collection_last_update = package['IDC_collection_last_update']
    aspera_url = package['Aspera_URL'] if not '&' in package['Aspera_URL'] else package['Aspera_URL'].split('&')[0]
    aspera_files = get_aspera_package_files(args, [], "", aspera_url)

    slug = package['Download_slug']
    ingested_urls = set(ingested_conversion_results['url'])

    # Drop initial directory name from aspera_files
    aspera_file_names_slashed = set(['/'.join(file['path'].split('/')[2:]) for file in aspera_files])
    logger_string = f"TCIA {TCIA_collection_id}:TCIA v{TCIA_collection_version}/{slug}-->{len(aspera_file_names_slashed)} Aspera package urls; IDC {IDC_collection_name}:v{IDC_collection_last_update}-->{len(ingested_urls)} ingested urls"

    blob_metadata = dict(
        # conversion_source_names=set(conversion_sources['name']),
        # ingested_urls = ingested_urls,
        aspera_files = aspera_files,
        logger_string = logger_string
    )

    return blob_metadata


def get_ingested_idc_converted_data_files(args, collection_id, version=None):
    client = bigquery.Client()
    version = version if version else args.version

    query = f"""
    SELECT DISTINCT ajp.collection_id, 
    ingestion_url,
    if(ENDS_WITH(ingestion_url, '.dcm'),  CONCAT(SPLIT(SPLIT(ingestion_url, '/')[ARRAY_LENGTH(SPLIT(ingestion_url, '/'))-1], '.dcm')[0], '.svs'),   CONCAT(SPLIT(ingestion_url, '/')[ARRAY_LENGTH(SPLIT(ingestion_url, '/'))-2], '.svs')) url
    FROM `idc-dev-etl.idc_v{version}_dev.all_joined_public` ajp
    JOIN `idc-dev-etl.idc_v{version}_pub.dicom_all` da
    ON ajp.i_uuid=da.crdc_instance_uuid
    WHERE REPLACE(REPLACE(LOWER(ajp.collection_id), '-', '_'), ' ','_') = '{collection_id}' and i_source='idc' and modality='SM'
    ORDER by collection_id
    """
    sources = client.query(query).result().to_dataframe()
    return sources


def get_blob_metadata_from_package(args, package, version = None):
    idc_collection_id = package['IDC_collection_id']
    ingested_conversion_results = get_ingested_idc_converted_data_files(args, idc_collection_id, version)
    blob_metadata = get_blob_metadata(args, package, ingested_conversion_results)
    return blob_metadata


def bucket_collection_id(collection_id):
    try:
        bucket_collection_id = {
            'cmb': 'cmb',
            'cptac': 'cptac',
            'nlst': 'nlst',
            'icdc': 'icdc_glioma'
        }[collection_id.split('_')[0]]
        return bucket_collection_id
    except KeyError:
        return collection_id


def get_collection(args, package, conversion_sources = None, version = None):
    idc_collection_id = package['IDC_collection_id']
    if conversion_sources is None:
        conversion_sources = tcia_sourced_pathology_files()
    ingested_conversion_results = get_ingested_idc_converted_data_files(args, idc_collection_id, version)
    blob_metadata = get_blob_metadata(args, package, ingested_conversion_results)
    return blob_metadata

