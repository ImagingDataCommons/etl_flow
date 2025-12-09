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

ASPERA_DOWNLOAD_FOLDER = '/mnt/disks/aspera'

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


def get_aspera_package_files(files, directory, url):

    MAX_TRIES = 10
    tries = 0

    progresslogger.info(f'Directory {directory}')
    while True:
        # Escapes some characters that might be in the directory
        escaped_directory = directory.replace(' ', '\ ').replace('&', '\&')
        cmmd = 'ascli --format=json faspex5 packages browse ' + \
               f' --url={url} {escaped_directory}'
        result = run(cmmd, capture_output=True, shell=True)
        # if not result.stderr.startswith(b'ERROR'):
        if not result.stderr:
            some_files = json.load(StringIO(result.stdout.decode()))
            if len(some_files) > 0:
                for file in some_files:
                    if file['type'] == 'directory':
                        try:
                            files = get_aspera_package_files(files, f"{directory}/{file['basename']}", url)
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
            errlogger.error(f'Failed to get new file list, {result.stderr}')
            tries += 1
            if tries == MAX_TRIES:
                errlogger.info(f"Failed to download aspera package, attempt {tries}, {result.stderr}")
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
    aspera_files = get_aspera_package_files([], "", aspera_url)

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




