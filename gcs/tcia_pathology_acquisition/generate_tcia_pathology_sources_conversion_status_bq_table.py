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
import os
import argparse
import json
from google.cloud  import bigquery, storage
import settings
from get_tcia_pathology_metadata import bucket_collection_id, get_aspera_package_urls
from utilities.logging_config import successlogger, progresslogger, errlogger
from utilities.bq_helpers import delete_BQ_Table

from time import strftime, gmtime

import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

def gen_conversion_status(client):
    query = f"""
CREATE OR REPLACE TABLE `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.tcia_pathology_sources_ingestion_status` AS
    WITH
    tcia_pathology_collection_names AS (
      SELECT DISTINCT collection_name
      FROM `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.tcia_pathology_source_file_metadata`
      ORDER BY collection_name
    ),
    
    # file_names_in_dicom_metadata is file names found in dicom metadata of instances in the TCIA pathology collections
    file_names_in_dicom_metadata AS (
    SELECT DISTINCT
      da.collection_name collection_name, PatientID, StudyInstanceUID, SeriesInstanceUID, SOPInstanceUID, gcs_url, aws_url,
      flattened_other_elements_data dicom_metadata_file_name
    FROM
      `idc-dev-etl.idc_v{settings.PREVIOUS_VERSION}_pub.dicom_all` da
      CROSS JOIN UNNEST(da.OtherElements) AS flattened_other_elements,
      UNNEST (flattened_other_elements.Data) AS flattened_other_elements_data
    JOIN tcia_pathology_collection_names tpcn
    ON da.collection_name = tpcn.collection_name
    GROUP BY collection_name, PatientID, StudyInstanceUID, SeriesInstanceUID, SOPInstanceUID, gcs_url, aws_url, flattened_other_elements.Tag, flattened_other_elements_data, SOPInstanceUID, tpcn.collection_name
    HAVING
      flattened_other_elements.Tag = 'Tag_00091001'  AND ENDS_WITH(flattened_other_elements_data, '.svs')
    ORDER by dicom_metadata_file_name
    )
    
    # source_files_having_names_in_dicom_metadata AS (
    # SELECT DISTINCT tpm.* except(source_file_name), dmfn.* except(collection_name, dicom_metadata_file_name)
    SELECT DISTINCT IF(tpm.collection_name IS NOT NULL, tpm.collection_name, dmfn.collection_name) collection_name,
        tpm.aspera_package_name, tpm.source_url, tpm.hash md5_hash,
        dmfn.* except(collection_name, dicom_metadata_file_name)
    FROM `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.tcia_pathology_source_file_metadata` tpm
    FULL OUTER JOIN file_names_in_dicom_metadata dmfn
    ON tpm.collection_name = dmfn.collection_name AND STRPOS(source_url, ARRAY_REVERSE(SPLIT(dicom_metadata_file_name, '/'))[0]) >0
    ORDER BY collection_name, dmfn.PatientID, dmfn.StudyInstanceUID, dmfn.SeriesInstanceUID, dmfn.SOPInstanceUID
"""
    query_job = client.query(query)
    query_job.result()
    return


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

    # Create a table of metadata about the TCIA pathology source files
    aspera_package_urls = get_aspera_package_urls() .sort_values('Download_slug')
    source_file_metadata = []
    # Add the content of every
    for _, package in aspera_package_urls.iterrows():
        if package['Download_slug'] not in args.skip:
            if args.download_slugs == [] or package['Download_slug'] in args.download_slugs:

                progresslogger.info(f"Processing package: {package['Download_slug']}")
                idc_collection_id = package['IDC_collection_id']
                if idc_collection_id or args.only_idc_collections==False:
                    TCIA_collection_name = package['TCIA_collection_name']
                    if TCIA_collection_name.startswith('CPTAC'):
                        tag = f"{TCIA_collection_name.split('-')[1]}/"
                    elif TCIA_collection_name.startswith('CMB'):
                        tag = f"{TCIA_collection_name}/"
                    else:
                        tag = ""
                    bucket_tag = bucket_collection_id(
                        idc_collection_id if idc_collection_id else package["TCIA_collection_id"])
                    src_bucket = client.bucket(f"{args.dst_bucket_prefix}{bucket_tag}"[:63 - len(args.dst_bucket_suffix)] +
                                               f"{args.dst_bucket_suffix}")
                    prefix = f'{tag}v{package["TCIA_collection_version"]}/{package["Download_slug"]}/'
                    hash_map = get_hash_map(src_bucket, prefix)
                    for row in hash_map:
                        row["collection_name"] = TCIA_collection_name
                        row["aspera_package_name"] = package["Download_slug"]
                        # row["source_file_name"] = row["source_url"].split('/')[-1].rsplit('.', 1)[0]

                    source_file_metadata.extend(hash_map)
                    progresslogger.info(f'Adding hash map: {len(hash_map)} rows; Running total={len(source_file_metadata)}')
            else:
                pass
        else:
            progresslogger.info(f'Skipped package {package["Download_slug"]}')
    # Upload the list to a BQ table
    source_file_metadata_df = pd.DataFrame(source_file_metadata)
    source_file_metadata_df.to_gbq(
        destination_table=f'idc_v{args.version}_dev.tcia_pathology_source_file_metadata',
        project_id='idc-dev-etl',
        if_exists='replace'
    )

    gen_conversion_status(bq_client)

    # Delete the temporary tcia_pathology_source_file_metadata table
    delete_BQ_Table(bq_client, 'idc-dev-etl', settings.BQ_DEV_INT_DATASET, 'tcia_pathology_source_file_metadata')

    return

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--version", default=settings.CURRENT_VERSION)
    parser.add_argument("--previous_version", default=settings.PREVIOUS_VERSION)
    parser.add_argument("--dst_bucket_prefix", default="", help="dst_bucket ID prefix")
    parser.add_argument("--dst_bucket_suffix", default="_pathology_data", help="dst_bucket ID suffix")
    parser.add_argument("--dst_project", default='idc-source-data', help="Project in which to create bucket")
    parser.add_argument("--download_slugs", default = [], help="Slugs to process; all if empty")
    parser.add_argument("--only_idc_collections", default=False, help="Only include a collection if IDC already has it")
    parser.add_argument("--skip", default=[], help="TCIA_collection_ids to be skipped")
    args = parser.parse_args()
    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    main(args)


