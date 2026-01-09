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
import pandas as pd
import numpy as np
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import os
import argparse
import json
from google.cloud  import bigquery, storage
import settings
from get_tcia_pathology_metadata import bucket_collection_id, get_aspera_package_urls, get_collection
from utilities.logging_config import successlogger, progresslogger
from tcia_sourced_pathology_files import tcia_sourced_pathology_files
from pandas import read_csv
from utilities.logging_config import successlogger, progresslogger, errlogger

from time import strftime, gmtime

import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials


def create_google_drive_folder(parent_folder_id, folder_name):
    """
    Creates a folder in Google Drive.
    """
    # Define the scope (full access to Google Drive)
    SCOPES = ['https://www.googleapis.com/auth/drive']

    # --- Authentication Flow ---
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    # --- End Authentication Flow ---

    try:
        service = build('drive', 'v3', credentials=creds)
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_folder_id]  # Specify the parent folder
        }

        # Create the folder
        file = service.files().create(body=file_metadata,
                                      fields='id, name').execute()  # Request specific fields in the response

        print(f"Folder '{file.get('name')}' created with ID: {file.get('id')}")
        return file.get('id')

    except Exception as error:
        print(f'An error occurred: {error}')
        return None


def gen_manifest_of_idc_missing_pathology_source(slug, aspera_file_names_slashed,
                                                 conversion_source_names_endswith, ingested_urls, aspera_files):
    in_aspera_not_idc_source_data = []
    for file in aspera_file_names_slashed:
        if not file.replace('-', '_') in conversion_source_names_endswith:
            in_aspera_not_idc_source_data.append(file)

    successlogger.info(f"**** {len(in_aspera_not_idc_source_data)} files in Aspera package but not in idc-source-data")
    if in_aspera_not_idc_source_data:
        for file in in_aspera_not_idc_source_data:
            successlogger.info(file)

    # Generate a list of files that are in idc-source-data
    in_aspera_and_in_idc_source_data = []
    for file in aspera_file_names_slashed:
        if file.replace('/', '_').replace('-', '_') in conversion_source_names_endswith:
            in_aspera_and_in_idc_source_data.append(conversion_source_names_endswith[file.replace('-', '_')])

    # Find which files in idc-source-data have not been ingested
    in_idc_source_data_not_ingested_urls = []
    if slug in ['nlst-da-path-1', 'nlst-da-path-2']:
        for file in in_aspera_and_in_idc_source_data:
            if not next((name for name in ingested_urls if
                         # file.replace('/', '_').replace('-', '_').endswith(name.rsplit('/', 1)[1].replace('-', '_'))),
                        name.replace('-','_').endswith(file.rsplit('/',1)[1].replace('-', '_'))),
                        False):
                in_idc_source_data_not_ingested_urls.append(file)
    else:
        for file in in_aspera_and_in_idc_source_data:
            if not next((name for name in ingested_urls if
                         file.replace('/', '_').replace('-', '_').endswith(name.replace('/', '_').replace('-', '_'))),
                        False):
                in_idc_source_data_not_ingested_urls.append(file)

    successlogger.info(f"**** {len(in_idc_source_data_not_ingested_urls)} files in idc-source-data but not in ingestion urls ****")
    for file in in_idc_source_data_not_ingested_urls:
        successlogger.info(file)

    # in_ingested_urls_not_idc_source_data = []
    # if slug not in ['cptac-ccrcc-da-path-nonccrcc', 'nlst-da-path-1', 'nlst-da-path-2']:
    #     for file in ingested_urls:
    #         if not next((name for name in ingested_urls if
    #                      name.replace('/', '_').replace('-', '_').find(file.replace('/', '_').replace('-', '_')) >= 0),
    #                     False):
    #             in_ingested_urls_not_idc_source_data.append(file)
    #     successlogger.info(
    #         f"**** {len(in_ingested_urls_not_idc_source_data)} files in ingestion urls but not in idc-source-data ****")
    #     for file in in_ingested_urls_not_idc_source_data:
    #         successlogger.info(file)
    # else:
    #     successlogger.info(
    #         f"**** 0 files in ingestion urls but not in idc-source-data ****")
    return


# def create_google_drive_folder(parent_folder_id, folder_name, credentials_path):
#     """Copies a file to a specified Google Drive folder.
#     Args:
#        file_path: Path to the file on your local system.
#        drive_folder_id: The ID of the Google Drive folder to copy to.
#        credentials_path: Path to your credentials.json file.
#     """
#     creds = service_account.Credentials.from_service_account_file(credentials_path)
#     service = build('drive', 'v3', credentials=creds)
#     # file_name = os.path.basename(file_path)
#     # media = MediaFileUpload(file_path, mimetype='text/plain')
#     file_metadata = {
#         'name': folder_name,
#         'mimeType': 'application/vnd.google-apps.folder',
#         'parents': [parent_folder_id]
#     }
#
#     try:
#         # file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
#         # Create the folder
#         file = service.files().create(body=file_metadata,
#                                       fields='id, name').execute()
#         print(f"Folder '{file.get('name')}' created with ID: {file.get('id')}")
#         return file
#     except Exception as e:
#         print(f"An error occurred: {e}")



# def copy_file_to_drive(args, file_path, drive_folder_id, credentials_path):
#     """Copies a file to a specified Google Drive folder.
#     Args:
#        file_path: Path to the file on your local system.
#        drive_folder_id: The ID of the Google Drive folder to copy to.
#        credentials_path: Path to your credentials.json file.
#     """
#     creds = service_account.Credentials.from_service_account_file(credentials_path)
#     service = build('drive', 'v3', credentials=creds)
#     file_name = os.path.basename(file_path)
#     media = MediaFileUpload(file_path, mimetype='text/plain')
#     # media = MediaFileUpload(file_path, mimetype='*/*')
#     file_metadata = {
#         # 'name': file_name,
#         'name': args.manifest_file_name,
#         'parents': [drive_folder_id]
#     }
#
#     try:
#         file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
#         print(f"File '{file_name}' copied successfully. File ID: {file.get('id')}")
#     except Exception as e:
#         print(f"An error occurred: {e}")


def get_idc_dicom_metadata():
    client = bigquery.Client()
    query = f"""
    SELECT submitter_case_id patientID, study_instance_uid StudyInstanceUID, series_instance_uid SeriesInstanceUID,
        sop_instance_uid SOPInstanceUID, ingestion_url, source_file_hash
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.idc_all_joined`
    """

    all_idc_dicom_metadata = client.query(query).to_dataframe()
    return all_idc_dicom_metadata


def get_sums_file(src_bucket, prefix):
    blobs = src_bucket.list_blobs(prefix=prefix)
    for blob in blobs:
        if blob.name.endswith('.sums'):
            source_hashes = blob.download_as_text().split('\n')
            source_hashes_list = [source_hash.split(' ',1) for source_hash in source_hashes]
            return source_hashes_list
    errlogger.error(f'No sums file found for {src_bucket.name}')
    return []

def get_hash_map(src_bucket, prefix):
    blobs = src_bucket.list_blobs(prefix=prefix, delimiter='/')
    for blob in blobs:
        if blob.name.endswith('hash.map'):
            source_hashes = blob.download_as_text().split('\n')
            # source_hashes_list = [source_hash.split(' ',1) for source_hash in source_hashes]
            source_hashes_list = json.loads(source_hashes[0])
            return source_hashes_list
    errlogger.error(f'No hash map found for {src_bucket.name}')
    return []

def main(args, download_slugs=[]):
    client = storage.Client(project='idc-dev-etl')
    bq_client = bigquery.Client()
    download = False
    idc_has = False
    gen_manifest = True

    # conversion_sources = tcia_sourced_pathology_files()
    # with open(f"{settings.LOG_DIR}/../conversion_source_names_endswith.csv", 'r') as f:
    #     conversion_source_names_endswith = json.load(f)

    aspera_package_urls = get_aspera_package_urls() .sort_values('Download_slug')
    all_idc_dicom_metadata = get_idc_dicom_metadata()
    source_file_hashes = []
    for _, package in aspera_package_urls.iterrows():
        if args.download_slugs == [] or package['Download_slug'] in args.download_slugs:

            progresslogger.info(f"Processing package: {package['Download_slug']}")
            idc_collection_id = package['IDC_collection_id']
            if idc_collection_id or args.only_idc_collections==False:
                IDC_collection_name = package['IDC_collection_name'] if package['IDC_collection_name'] else \
                    package['TCIA_collection_id'].upper()
                if IDC_collection_name.startswith('CPTAC'):
                    tag = f"{IDC_collection_name.split('-')[1]}/"
                elif IDC_collection_name.startswith('CMB'):
                    tag = f"{IDC_collection_name}/"
                else:
                    tag = ""
                bucket_tag = bucket_collection_id(
                    idc_collection_id if idc_collection_id else package["TCIA_collection_id"])
                src_bucket = client.bucket(f"{args.dst_bucket_prefix}{bucket_tag}"[:63 - len(args.dst_bucket_suffix)] +
                                           f"{args.dst_bucket_suffix}")
                prefix = f'{tag}v{package["TCIA_collection_version"]}/{package["Download_slug"]}/'
                sums_file = get_hash_map(src_bucket, prefix)
                for row in sums_file:
                    row["collection_name"] = IDC_collection_name
                source_file_hashes.extend(sums_file)
                progresslogger.info(f'Adding hash map: {len(sums_file)} rows; Running total={len(source_file_hashes)}')
    columns = ["collection_name", "hash", "source_url"]
    source_file_hashes_df = pd.DataFrame(source_file_hashes)
    source_status = source_file_hashes_df.merge(all_idc_dicom_metadata, left_on='hash',\
                right_on='source_file_hash', how='left')
    source_status.drop('source_file_hash', axis='columns', inplace=True)
    source_status.fillna('', inplace=True)

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    table_id = f'{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.tcia_pathology_source_data_status'
    job = bq_client.load_table_from_dataframe(source_status, table_id, job_config=job_config)
    result = job.result()

    return


            #     bucket_tag = bucket_collection_id(idc_collection_id)
            #
            #     manifest_params = get_collection(args, package, conversion_sources, args.version)
            #     aspera_files = manifest_params["aspera_files"]
            #     conversion_source_names = manifest_params["conversion_source_names"]
            #     ingested_urls = manifest_params["ingested_urls"]
            #
            #     slug = package['Download_slug']
            #     # aspera_file_names_slashed = set(['/'.join(file['path'].split('/')[2:]) for file in aspera_files])
            #     aspera_file_names_slashed = set(file['path'].rsplit('/',1)[1] for file in aspera_files)
            #     successlogger.info(manifest_params["logger_string"])
            #     gen_manifest_of_idc_missing_pathology_source(
            #         slug, aspera_file_names_slashed,
            #         # conversion_source_names,
            #         conversion_source_names_endswith,
            #         ingested_urls, aspera_files
            #     )
            #     successlogger.info("")
            # else:
            #     successlogger.info(f'Skipping TCIA collection {package["TCIA_collection_id"]}\n')


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--version", default=settings.CURRENT_VERSION-1)
    parser.add_argument('--processes', default=1)
    parser.add_argument('--mode', default='download')
    parser.add_argument("--dst_bucket_prefix", default="", help="dst_bucket ID prefix")
    parser.add_argument("--dst_bucket_suffix", default="_pathology_data", help="dst_bucket ID suffix")
    parser.add_argument("--dst_project", default='idc-source-data', help="Project in which to create bucket")
    parser.add_argument("--google_drive_folder", default="", help="Google Drive folder ID")
    parser.add_argument("--save_result", default=True, help="Save result to a Drive file if True")
    parser.add_argument("--download_slugs", default = [], help="Slugs to process; all if empty")
    parser.add_argument("--manifest_folder_name", default= f'manifest_{strftime("%Y%m%d_%H%M%S", gmtime())}')
    # parser.add_argument("--manifest_file_name", default= f'manifest_{strftime("%Y%m%d_%H%M%S", gmtime())}')
    parser.add_argument("--only_idc_collections", default=False, help="Only include a collection if IDC already has it")
    args = parser.parse_args()
    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    # drive_folder_id = '1T8VA3RaO65rMAoWYKEIiBwTwbsQYW2jP'  # Replace with your Google Drive folder ID
    # folder = create_google_drive_folder(drive_folder_id, args.manifest_folder_name, settings.CREDENTIALS_PATH)

    main(args)


