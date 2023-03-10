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

# This script generates the BQ auxiliary_metadata table. It basically joins the BQ version, collection,
# patient, study, series, and instance tables. Typically these are uploaded from PostgreSQL to BQ using
# the upload_psql_to_bq.py script
import argparse
import json
import sys
from google.cloud import bigquery

from python_settings import settings
from time import sleep
from utilities.bq_helpers import load_BQ_from_json, query_BQ, create_BQ_table, delete_BQ_Table
from utilities.logging_config import successlogger,progresslogger
from bq.utils.gen_license_table import get_original_collection_licenses
from bq.gen_aux_metadata_table.schema import auxiliary_metadata_schema


def build_table(args):
    query = f"""
    SELECT
      collection_id AS tcia_api_collection_id,
      REPLACE(REPLACE(LOWER(collection_id),'-','_'), ' ','_') AS idc_webapp_collection_id,
      REPLACE(REPLACE(LOWER(collection_id),'-','_'), ' ','_') AS collection_id,
      c_min_timestamp as collection_timestamp,
      c_hashes.all_hash AS collection_hash,
      c_init_idc_version AS collection_init_idc_version,
      c_rev_idc_version AS collection_revised_idc_version,
    --
      submitter_case_id AS submitter_case_id,
      idc_case_id AS idc_case_id,
      p_hashes.all_hash AS patient_hash,
      p_init_idc_version AS patient_init_idc_version,
      p_rev_idc_version AS patient_revised_idc_version,
    --
      study_instance_uid AS StudyInstanceUID,
      st_uuid AS study_uuid,
      study_instances,
      st_hashes.all_hash AS study_hash,
      st_init_idc_version AS study_init_idc_version,
      st_rev_idc_version AS study_revised_idc_version,
--       st_final_idc_version AS study_final_idc_version,
    --
      series_instance_uid AS SeriesInstanceUID,
      se_uuid AS series_uuid,
      IF(collection_id='APOLLO', '', source_doi) AS source_doi,
      IF(source_url is Null OR source_url='', CONCAT('https://doi.org/', source_doi), source_url) AS source_url,
      series_instances,
      se_hashes.all_hash AS series_hash,
      'Public' AS access,
      se_init_idc_version AS series_init_idc_version,
      se_rev_idc_version AS series_revised_idc_version,
--       se_final_idc_version AS series_final_idc_version,
    --
      sop_instance_uid AS SOPInstanceUID,
      i_uuid AS instance_uuid,
      CONCAT('gs://',
        # If we are generating gcs_url for the public auxiliary_metadata table 
        if('{args.target}' = 'pub', 
            if( i_source='tcia', ac.pub_gcs_tcia_url, ac.pub_gcs_idc_url), 
        #else 
            # We are generating the dev auxiliary_metadata
            # If this instance is new in this version and we 
            # have not merged new instances into dev buckets
            if(i_rev_idc_version = {settings.CURRENT_VERSION} and not {args.merged},
                # We use the premerge url prefix
                CONCAT('idc_v', {settings.CURRENT_VERSION}, 
                    '_',
                    i_source,
                    '_',
                    REPLACE(REPLACE(LOWER(collection_id),'-','_'), ' ','_')
                    ),
    
            #else
                # This instance is not new so use the staging bucket prefix
                 if( i_source='tcia', ac.dev_tcia_url, ac.dev_idc_url)
                )
            ), 
        '/', se_uuid, '/', i_uuid, '.dcm') as gcs_url,
      
      # There are no dev S3 buckets, so populate the aws_url 
      # the same for both dev and pub versions of auxiliary_metadata
      CONCAT('s3://',
        if( i_source='tcia', ac.pub_aws_tcia_url, ac.pub_aws_idc_url),
            '/', se_uuid, '/', i_uuid, '.dcm') as aws_url,
      i_size AS instance_size,
      i_hash AS instance_hash,
--       i_source AS instance_source,
      i_init_idc_version AS instance_init_idc_version,
      i_rev_idc_version AS instance_revised_idc_version,
--       i_final_idc_version AS instance_final_idc_version,
      license_url,
      license_long_name,
      license_short_name
      FROM
        `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined` aj
      JOIN
        `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_collections` ac
      ON
        collection_id = ac.tcia_api_collection_id
      WHERE
        i_excluded is False
      AND
        ((i_source='tcia' AND ac.tcia_access='Public' AND (ac.tcia_metadata_sunset=0 OR ({args.version} <= ac.tcia_metadata_sunset))) 
        OR (i_source='idc' AND ac.idc_access='Public' AND (ac.idc_metadata_sunset=0 OR ({args.version} <= ac.idc_metadata_sunset))))
      AND
        idc_version = {args.version}
      ORDER BY
        tcia_api_collection_id, submitter_case_id
"""
    client = bigquery.Client(project=args.dst_project)
    result = delete_BQ_Table(client, args.dst_project, args.trg_bqdataset_name, args.bqtable_name)
    # Create a table to get the schema defined
    created_table = create_BQ_table(client, args.dst_project, args.trg_bqdataset_name, args.bqtable_name, auxiliary_metadata_schema, exists_ok=True)
    # Perform the query and save results in specified table
    results = query_BQ(client, args.trg_bqdataset_name, args.bqtable_name, query, write_disposition='WRITE_TRUNCATE')
    populated_table = client.get_table(f"{args.dst_project}.{args.trg_bqdataset_name}.{args.bqtable_name}")
    populated_table.schema = auxiliary_metadata_schema
    populated_table.description = "IDC version-related metadata"
    client.update_table(populated_table, fields=["schema", "description"])
    successlogger.info('Created auxiliary_metadata table')

def gen_aux_table(args):
    build_table(args)