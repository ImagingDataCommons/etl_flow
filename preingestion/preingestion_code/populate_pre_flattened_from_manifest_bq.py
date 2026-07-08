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
# Adds/replaces data to the idc_collection/_patient/_study/_series/_instance DB tables
# from a specified bucket.
#
# One or more (manifest_url, manifest_type) pairs are specified in args,
#  the manifest_url is relative to the GCS folder specified by args.subdir.
#  if a pair is like ("", manifest_type), then a manifest is generated from the bucket contents and applied
#  according to the manifest_type.
# Note: In the event that a ("", 'partial_deletion') pair is specified, the script will remove all instances
# found in the args.subdir folder.
##
# In the last case, how do we know whether the revision is 'complete' or 'partial'?
#
# The script walks the directory hierarchy from a specified subdirectory of the
# gcsfuse mount point

import settings
import argparse
import sys
from google.cloud import bigquery
# import json5
# from idc.models import Pre_Collection, Pre_Patient, Pre_Study, Pre_Series, Pre_Instance
# from utilities.logging_config import successlogger, errlogger, progresslogger
# from base64 import b64decode
# import pandas as pd
# from preingestion.validation_code.validate_analysis_result import validate_analysis_result
# from preingestion.validation_code.validate_original_collection import validate_original_collection
# from preingestion.preingestion_code.gen_hashes_sql import gen_hashes
# from preingestion.preingestion_code.gen_manifest_from_dicom_metadata import build_manifest
#
# import time
#
# from ingestion.utilities.utils import get_merkle_hash, streaming_md5_hasher
#
# from utilities.sqlalchemy_helpers import sa_session
# from google.cloud import storage
#
# from multiprocessing import Queue, Process
# from queue import Empty
#
# import requests
# import yaml
# from io import StringIO


def merge_manifest(args):
    client = bigquery.Client()
    query = f"""
BEGIN
# Load the manifest into a temporary table
CREATE OR REPLACE EXTERNAL TABLE `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.{args.manifest_name}` (
  collection_name STRING,
  patientID STRING,
  StudyInstanceUID STRING,
  SeriesInstanceUID STRING,
--   source_doi STRING,
--   versioned_source_doi STRING,
  SOPInstanceUID STRING,
  `hash` STRING,
  ingestion_url STRING,
  size INTEGER)
  OPTIONS (
    format = 'CSV',
    uris = ['{args.manifest_url}'],
    skip_leading_rows = 1,
    expiration_timestamp = TIMESTAMP_ADD(
      CURRENT_TIMESTAMP(), INTERVAL 60 MINUTE),
    field_delimiter = ',');

CREATE OR REPLACE TABLE `idc-dev-etl.idc_v0_dev.{args.dst_table}` AS (
WITH
  source_outer_join AS (
    SELECT
      manifest.*,
      i_hash,
      se_excluded,
      i_excluded,
      mitigation,
      source_file_hash,
      idc_version
    FROM idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.{args.manifest_name} manifest
    LEFT JOIN `idc-dev-etl.idc_v0_dev.{args.src_table}` src_table
      ON src_table.SOPInstanceUID = manifest.SOPInstanceUID
    -- WHERE 
    --    src_table.source_doi = '{args.source_doi}' 
    --    OR manifest.source_doi = '{args.source_doi}'
    ORDER BY src_table.patientID, src_table.SOPInstanceUID
  ),
  source_metadata AS (
    SELECT
      joined_manifest_and_src_table.collection_name,
      REPLACE(REPLACE(LOWER(joined_manifest_and_src_table.collection_name), " ", "_"), "-", "_")
        collection_id,
      '' c_hash,
      joined_manifest_and_src_table.patientID,
      '' p_hash,
      joined_manifest_and_src_table.StudyInstanceUID,
      '' st_hash,
      joined_manifest_and_src_table.SeriesInstanceUID,
      "" se_hash,
      
--       IF(joined_manifest_and_src_table.i_hash IS NULL OR joined_manifest_and_src_table.i_hash != joined_manifest_and_src_table.`hash`, FALSE, joined_manifest_and_src_table.se_excluded)
--         se_excluded,
      CASE
        WHEN joined_manifest_and_src_table.i_hash IS NULL OR joined_manifest_and_src_table.i_hash != joined_manifest_and_src_table.hash THEN false
        ELSE joined_manifest_and_src_table.se_excluded
      END AS se_excluded,      
      '{args.source_doi}' source_doi,

      'gs://doi.org/{args.source_doi}' source_url,
      '{args.versioned_source_doi}' versioned_source_doi,
      'gs://doi.org/{args.versioned_source_doi}' versioned_source_url,
      {args.analysis_result} analysis_result,
      joined_manifest_and_src_table.SOPInstanceUID,
      joined_manifest_and_src_table.`hash` i_hash,
      joined_manifest_and_src_table.ingestion_url,
      
--       IF(joined_manifest_and_src_table.i_hash IS NULL OR joined_manifest_and_src_table.i_hash != joined_manifest_and_src_table.`hash`, 0, joined_manifest_and_src_table.size) size,
      CASE
        WHEN joined_manifest_and_src_table.i_hash IS NULL OR joined_manifest_and_src_table.i_hash != joined_manifest_and_src_table.hash THEN 0
        ELSE joined_manifest_and_src_table.size
      END AS size,
      
--       IF(joined_manifest_and_src_table.i_hash IS NULL OR joined_manifest_and_src_table.i_hash != joined_manifest_and_src_table.`hash`, FALSE, joined_manifest_and_src_table.i_excluded) i_excluded,
      CASE
        WHEN joined_manifest_and_src_table.i_hash IS NULL OR joined_manifest_and_src_table.i_hash != joined_manifest_and_src_table.hash THEN false
        ELSE joined_manifest_and_src_table.i_excluded
      END AS i_excluded,
      
--       IF(joined_manifest_and_src_table.i_hash IS Null OR joined_manifest_and_src_table.i_hash != joined_manifest_and_src_table.`hash`, {settings.CURRENT_VERSION}, joined_manifest_and_src_table.idc_version) idc_version,
      CASE
        WHEN joined_manifest_and_src_table.i_hash IS Null OR joined_manifest_and_src_table.i_hash != joined_manifest_and_src_table.hash THEN {settings.CURRENT_VERSION}
        ELSE joined_manifest_and_src_table.idc_version 
      END AS idc_version,

--       IF(joined_manifest_and_src_table.i_hash IS NULL OR joined_manifest_and_src_table.i_hash != joined_manifest_and_src_table.`hash`, "", joined_manifest_and_src_table.mitigation) mitigation,
     CASE
        WHEN joined_manifest_and_src_table.i_hash IS NULL OR joined_manifest_and_src_table.i_hash != joined_manifest_and_src_table.hash THEN ''
        ELSE joined_manifest_and_src_table.mitigation
      END AS mitigation,

--       IF(joined_manifest_and_src_table.i_hash IS NULL OR joined_manifest_and_src_table.i_hash != joined_manifest_and_src_table.`hash`, "", joined_manifest_and_src_table.source_file_hash) source_file_hash
      CASE
        WHEN joined_manifest_and_src_table.i_hash IS NULL OR joined_manifest_and_src_table.i_hash != joined_manifest_and_src_table.hash THEN ''
        ELSE joined_manifest_and_src_table.source_file_hash
      END AS source_file_hash

    FROM source_outer_join joined_manifest_and_src_table
  ),
  non_source_metadata AS (
    SELECT src_table.*
    FROM `idc-dev-etl.idc_v0_dev.{args.src_table}` src_table
    WHERE source_doi != '{args.source_doi}'
    ORDER BY patientID, SOPInstanceUID
  )
SELECT * FROM source_metadata
UNION ALL
SELECT * FROM non_source_metadata
);

END;

"""

    query_job = client.query(query)
    query_job.result()
    return

def prebuild_from_manifests(args):
    # with sa_session(echo=False) as sess:
    #     merge_manifest(args)
    #     sess.commit()

    merge_manifest(args)



# if __name__ == '__main__':
#
#     parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
#     parser.add_argument('--version', default=settings.CURRENT_VERSION)
#     parser.add_argument("--src_table", default="all_data_snapshot", help="Source BQ table")
#     parser.add_argument("--dst_table", default="all_data_snapshot1", help="Destination BQ table")
#     parser.add_argument('--manifest_name', default='catch_manifest', help='csv/tsv complete manifest')
#     parser.add_argument('--manifest_url', default='gs://whc_etl_dev/catch_manifest.csv', help='csv/tsv complete manifest')
#     parser.add_argument('--source_doi', default='10.5281/zenodo.18526942', help='DOI of source')
#     parser.add_argument('--versioned_source_doi', default='10.5281/zenodo.18526943', help='Versioned DOI of source')
#     parser.add_argument('--analysis_result', default='False', help='True if source is an analysis result')
#     parser.add_argument('--validate', type=bool, default=True, help='True if validation is to be performed')
#     parser.add_argument('--gen_hashes', type=bool, default=True, help='True if hashes are to be generated')
#
#     args = parser.parse_args()
#     print("{}".format(args), file=sys.stdout)
#
#     prebuild_from_manifests(args)

