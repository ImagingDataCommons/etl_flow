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


def copy_file_to_drive(args, file_path, drive_folder_id, credentials_path):
    """Copies a file to a specified Google Drive folder.
    Args:
       file_path: Path to the file on your local system.
       drive_folder_id: The ID of the Google Drive folder to copy to.
       credentials_path: Path to your credentials.json file.
    """
    creds = service_account.Credentials.from_service_account_file(credentials_path)
    service = build('drive', 'v3', credentials=creds)
    file_name = os.path.basename(file_path)
    media = MediaFileUpload(file_path, mimetype='text/plain')
    # media = MediaFileUpload(file_path, mimetype='*/*')
    file_metadata = {
        # 'name': file_name,
        'name': args.manifest_file_name,
        'parents': [drive_folder_id]
    }

    try:
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f"File '{file_name}' copied successfully. File ID: {file.get('id')}")
    except Exception as e:
        print(f"An error occurred: {e}")


def main(args, download_slugs=[]):
    client = storage.Client(project='idc-dev-etl')
    download = False
    idc_has = False
    gen_manifest = True

    aspera_package_urls = get_aspera_package_urls()
    for _, package in aspera_package_urls.iterrows():
        if args.download_slugs == '' or package['Download_slug'] in args.download_slugs:
            idc_collection_id = package['IDC_collection_id']
            bucket_tag = bucket_collection_id(idc_collection_id)

            manifest_params = get_collection(args, package)
            aspera_files = manifest_params["aspera_files"]
            conversion_source_names = manifest_params["conversion_source_names"]
            ingested_urls = manifest_params["ingested_urls"]

            slug = package['Download_slug']
            aspera_file_names_slashed = set(['/'.join(file['path'].split('/')[2:]) for file in aspera_files])
            successlogger.info(manifest_params["logger_string"])
            gen_manifest_of_idc_missing_pathology_source(
                slug, aspera_file_names_slashed,
                conversion_source_names,
                ingested_urls, aspera_files
            )
            successlogger.info("")


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
    parser.add_argument("--download_slugs", default = ["cmb-aml-da-path"], help="Slugs to process; all if empty")
    parser.add_argument("--manifest_file_name", default= f'manifest_{strftime("%Y%m%d_%H%M%S", gmtime())}.txt')
    args = parser.parse_args()
    print(f'args: {json.dumps(args.__dict__, indent=2)}')

    main(args)

    drive_folder_id = '1T8VA3RaO65rMAoWYKEIiBwTwbsQYW2jP'  # Replace with your Google Drive folder ID
    if args.save_result:
        copy_file_to_drive(args, successlogger.handlers[0].baseFilename,  drive_folder_id, settings.CREDENTIALS_PATH)


