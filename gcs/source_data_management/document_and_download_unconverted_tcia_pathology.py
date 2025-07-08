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


def get_blob_metadata(args, package, conversion_sources, ingested_conversion_results):
    client = storage.Client(project='idc-dev-etl')
    # Get list of (svs) aspera_files in package
    TCIA_collection_id = package['TCIA_collection_id']
    TCIA_collection_version = package['TCIA_collection_version']
    IDC_collection_name = package['IDC_collection_name']
    IDC_collection_version = package['IDC_collection_version']
    aspera_url = package['Aspera_URL']
    aspera_files = get_aspera_package_files([], "", aspera_url)

    slug = package['Download_slug']
    ingested_urls = set(ingested_conversion_results['url'])

    # Drop initial directory name from aspera_files
    aspera_file_names_slashed = set(['/'.join(file['path'].split('/')[2:]) for file in aspera_files])
    logger_string = f"TCIA {TCIA_collection_id}:v{TCIA_collection_version}/{slug}-->{len(aspera_file_names_slashed)} Aspera package urls; IDC {IDC_collection_name}:v{IDC_collection_version}-->{len(ingested_urls)} ingested urls"

    blob_metadata = dict(
        conversion_source_names=set(conversion_sources['name']),
        ingested_urls = ingested_urls,
        aspera_files = aspera_files,
        logger_string = logger_string
    )

    return blob_metadata


def get_ingested_idc_converted_data_files(args, collection_id):
    client = bigquery.Client()
    query = f"""
SELECT DISTINCT ajp.collection_id, 
ingestion_url,
if(ENDS_WITH(ingestion_url, '.dcm'),  CONCAT(SPLIT(SPLIT(ingestion_url, '/')[ARRAY_LENGTH(SPLIT(ingestion_url, '/'))-1], '.dcm')[0], '.svs'),   CONCAT(SPLIT(ingestion_url, '/')[ARRAY_LENGTH(SPLIT(ingestion_url, '/'))-2], '.svs')) url
FROM `idc-dev-etl.idc_v{args.version}_dev.all_joined_public` ajp
JOIN `idc-dev-etl.idc_v{args.version}_pub.dicom_all` da
ON ajp.i_uuid=da.crdc_instance_uuid
WHERE REPLACE(REPLACE(LOWER(ajp.collection_id), '-', '_'), ' ','_') = '{collection_id}' and i_source='idc' and modality='SM'
ORDER by collection_id
"""
    sources = client.query(query).result().to_dataframe()
    return sources


def get_collection(args, package):
    idc_collection_id = package['IDC_collection_id']
    conversion_sources = tcia_sourced_pathology_files()
    ingested_conversion_results = get_ingested_idc_converted_data_files(args, idc_collection_id)
    blob_metadata = get_blob_metadata(args, package, conversion_sources, ingested_conversion_results)
    return blob_metadata


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



