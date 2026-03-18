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
from google.cloud  import storage, bigquery
import settings
from get_tcia_pathology_metadata import bucket_collection_id, get_aspera_package_urls
from utilities.logging_config import successlogger, progresslogger, errlogger
from time import strftime, gmtime
from base64 import b64decode
from idc.models import Base, Version, Collection, Instance, All_Joined, IDC_All_Joined
from utilities.sqlalchemy_helpers import sa_session
from sqlalchemy import or_
import pandas as pd
import subprocess
from pydicom import dcmread


from ingestion.utilities.utils import md5_hasher

ASPERA_DOWNLOAD_FOLDER = '/mnt/disks/idc-etl/aspera'

# Get table of all original source instances derived from TCIA pathology data
# For each collection in idc_collection ,
#   get .sums file data
#   for each instance in collection,
#       find correspond element in .sums file data
#       record corresponding .sums file hash as instances source_file_hash



def get_conversion_source_hashs(dst_bucket, bucket_tag, tag, slug):
    storage_client = storage.Client(project='idc-source-data')
    buckets = storage_client.list_buckets()

    sources = []
    dst_bucket_name = dst_bucket.name.lower().replace('-', '_')
    for bucket in buckets:
        if dst_bucket_name in bucket.name.lower().replace('-', '_') or \
                bucket_tag and bucket_tag in bucket.name.lower().replace('-', '_'):
            progresslogger.debug(f'Adding bucket {bucket.name}')
            blobs = bucket.list_blobs()
            prefix = f'{tag if not tag else (tag+"/" if bucket_tag != "cptac" else "CPTAC-"+tag+"/")}'
            for blob in blobs:
                if not tag or tag in blob.name:
                    try:
                        sources.append(f'{b64decode(blob.md5_hash).hex()} {prefix}{blob.name.split(slug+"/")[-1]}')
                    except Exception as exc:
                        pass
    if len([i.split(' ')[1] for i in sources]) != len(set([i.split(' ')[1] for i in sources])):
        from collections import Counter
        counts = Counter([i.split(' ')[1] for i in sources])
        dups = [(item,count) for item, count in counts.items() if count > 1]

    # conversion_source_names = set(name.replace('/', '_').replace('-', '_') for name in sources)

    return sources

# Get a table of all the instances that have an ingestion_url, and are potentially original collection pathology
# Some of these will be analysis result instances, in which case no hash will be assigned.
def get_ingested_instance():
    client = bigquery.Client()
    query = f"""
    SELECT collection_id, sop_instance_uid, ingestion_url, pub_gcs_bucket, se_uuid, i_uuid
--     FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.idc_all_joined` iaj
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_public_and_current` 
    WHERE
        ingestion_url LIKE '%cmb_pathology%' OR
        ingestion_url LIKE '%cptac_pathology%' OR
        ingestion_url LIKE '%icdc_glioma_pathology%' OR
        ingestion_url LIKE '%nlst_pathology%' OR
        ingestion_url LIKE '%nlst_pathology_additional%'
    """

    query_job = client.query(query)
    source_instances = query_job.to_dataframe()

    return source_instances

def get_source_hashes(collection_id):
    client = storage.Client()
    sums_file = {
        'CMB-AML': ['cmb_pathology_data', 'CMB-AML/v7/cmb-aml-da-path/generated.sums'],
        'CMB-BRCA': ['cmb_pathology_data', 'CMB-BRCA/v4/cmb-brca-da-path/generated.sums'],
        'CMB-CRC': ['cmb_pathology_data', 'CMB-CRC/v9/cmb-crc-da-path/generated.sums'],
        'CMB-GEC': ['cmb_pathology_data', 'CMB-GEC/v7/cmb-gec-da-path/generated.sums'],
        'CMB-LCA': ['cmb_pathology_data', 'CMB-LCA/v10/cmb-lca-da-path/generated.sums'],
        'CMB-MEL': ['cmb_pathology_data', 'CMB-MEL/v10/cmb-mel-da-path/generated.sums'],
        'CMB-MML': ['cmb_pathology_data', 'CMB-MML/v9/cmb-mml-da-path/generated.sums'],
        'CMB-OV': ['cmb_pathology_data', 'CMB-OV/v3/cmb-ov-da-path/generated.sums'],
        'CMB-PCA': ['cmb_pathology_data', 'CMB-PCA/v10/cmb-pca-da-path/generated.sums'],

        'CPTAC-AML': ['cptac_pathology_data', 'AML/v5/cptac-aml-da-path/generated.sums'],
        'CPTAC-BRCA': ['cptac_pathology_data', 'BRCA/v1/cptac-brca-da-path/CPTAC-BRCA_v1.sums'],
        'CPTAC-CCRCC': ['cptac_pathology_data', 'CCRCC/v14/cptac-ccrcc-da-path/generated.sums'],
        'CPTAC-CM': ['cptac_pathology_data', 'CM/v11/cptac-cm-da-path/CPTAC-CM.sums'],
        'CPTAC-COAD': ['cptac_pathology_data', 'COAD/v1/cptac-coad-da-path/CPTAC-COAD.sums'],
        'CPTAC-GBM': ['cptac_pathology_data', 'GBM/v16/cptac-gbm-da-path/generated.sums'],
        'CPTAC-HNSCC': ['cptac_pathology_data', 'HNSCC/v19/cptac-hnscc-da-path/CPTAC-HNSCC.sums'],
        'CPTAC-LSCC': ['cptac_pathology_data', 'LSCC/v15/cptac-lscc-da-path/CPTAC-LSCC.sums'],
        'CPTAC-LUAD': ['cptac_pathology_data', 'LUAD/v13/cptac-luad-da-path/generated.sums'],
        'CPTAC-OV': ['cptac_pathology_data', 'OV/v1/cptac-ov-da-path/CPTAC-OV.sums'],
        'CPTAC-PDA': ['cptac_pathology_data', 'PDA/v15/cptac-pda-da-path/CPTAC-PDA.sums'],
        'CPTAC-SAR': ['cptac_pathology_data', 'SAR/v10/cptac-sar-da-path/CPTAC-SAR.sums'],
        'CPTAC-UCEC': ['cptac_pathology_data', 'UCEC/v13/cptac-ucec-da-path/generated.sums'],

        'ICDC-Glioma': ['icdc_glioma_pathology_data', 'v1/icdc-glioma-da-path/generated.sums'],
        
        'NLST': ['nlst_pathology_data', 'v3/nlst-da-path/generated.sums']

    }
    sums_file = sums_file[collection_id]
    bucket = client.bucket(sums_file[0])
    blob = bucket.blob(sums_file[1])
    source_hashes = blob.download_as_text().split('\n')
    return source_hashes
def main(args, download_slugs=[]):
    client = storage.Client()
    bq_client = bigquery.Client()
    src_bucket = client.bucket('idc-open-data')
    ingested_instances = get_ingested_instance()
    ingested_instances['source_file_hash'] = None
    for collection_id in ingested_instances['collection_id'].unique():
        instances = ingested_instances[ingested_instances['collection_id'] == collection_id]
        source_hashes = get_source_hashes(collection_id)
        if collection_id == 'NLST':
            source_hashes = [source_hash.replace('/', '_') for source_hash in source_hashes]
        for index, instance in instances.iterrows():
            try:
                source_hash = next(source_hash for source_hash in source_hashes if
                            instance['ingestion_url'].rsplit('/',3)[-2] in source_hash)
                try:
                    ingested_instances.at[index, 'source_file_hash'] = source_hash.split(' ')[0]
                except Exception as exc:
                    pass
            except Exception as exc:
                # errlogger.error(f'No hash found for {collection_id}: ingestion_url: {instance["ingestion_url"]}')
                blob_name = f'{instance.se_uuid}/{instance.i_uuid}.dcm'
                # blob = src_bucket.blob(blob_name)
                with src_bucket.blob(blob_name).open('rb') as f:
                    try:
                        r = dcmread(f, specific_tags=['ContainerIdentifier'], stop_before_pixels=True)
                        container_identifier = r.ContainerIdentifier
                        progresslogger.info(f'ContainerIdentifier: {container_identifier}')
                        try:
                            source_hash = next(source_hash for source_hash in source_hashes if container_identifier in source_hash)
                            ingested_instances.at[index, 'source_file_hash'] = source_hash.split(' ')[0]
                        except Exception as exc:
                            errlogger.error(f'Could not find source_hash for container_identifier: {container_identifier}')
                    except Exception as exc:
                        errlogger.error(f'Could not open {blob_name}')
                        continue

    table_id = f'{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.temp_source_file_hashes'
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # Overwrite table if it exists
        autodetect=True)
    job = bq_client.load_table_from_dataframe(ingested_instances, table_id, job_config=job_config)
    result = job.result()

    print(f"Loaded {job.output_rows} rows into {table_id}.")

    # To revise idc-instance with hashes in temp_source_file_hashes, it is recommended to download temp_source_file_hashes
    # to Cloud SQL and then update idc_instance. E.G:
    #   UPDATE idc_instance ii
    #   SET source_file_hash = tsfh.source_file_hash
    #   FROM temp_source_file_hashes tsfh
    #   WHERE ii.sop_instance_uid = tsfh.sop_instance_uid

    return

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
    parser.add_argument("--manifest_file_name", default= f'manifest_{strftime("%Y%m%d_%H%M%S", gmtime())}.txt')
    parser.add_argument("--only_idc_collections", default=False, help="Only include a collection if IDC already has it")
    parser.add_argument("--skip", default=['CMB-MML'], help="TCIA_collection_ids to be skipped")
    parser.add_argument("--load_aspera_files", default=True)
    args = parser.parse_args()
    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    # try:
    #     subprocess.run(['gcsfuse', '--implicit-dirs', "idc-open-data", "/mnt/disks/idc-etl/aspera/gcsfuse_mount_point"])
    #
    #     # Default process ID
    #     args.id = 0
    #
    #
    #     main(args)
    # finally:
    #     subprocess.run(['fusermount', '-u', "/mnt/disks/idc-etl/aspera/gcsfuse_mount_point"])

    # Default process ID
    args.id = 0


    main(args)

