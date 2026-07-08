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
from subprocess import run
from utilities.sqlalchemy_helpers import sa_session

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


def create_manifest_table(args, sess):
    query = f"""
BEGIN;
    DROP TABLE IF EXISTS {args.temp_manifest_table};
    CREATE TABLE {args.temp_manifest_table} (
      collection_name varchar,
      patientID varchar,
      StudyInstanceUID varchar,
      SeriesInstanceUID varchar,
      SOPInstanceUID varchar,
      hash varchar,
      ingestion_url varchar,
      i_size bigint
    );
END;
"""
    sess.execute(query)
    field_terminator = '09' if args.manifest_url.endswith("tsv") else '2C'
    # cmd = [
    #     'gcloud',
    #     'sql',
    #     'import',
    #     'csv',
    #     args.cloud_sql_instance,
    #     args.manifest_url ,
    #     '--database=idc_v0',
    #     '--user=idc',
    #     f'--table={args.temp_manifest_table}',
    #     f'--fields-terminated-by={field_terminator}',
    #     '--quiet',
    #     '--project=idc-dev-etl']
    cmd = f"""
    gcloud storage cat {args.manifest_url} | psql "host=127.0.0.1 port=5432 dbname={args.db_name} user=idc password={settings.CLOUD_PASSWORD}" -c "\copy {args.temp_manifest_table} FROM STDIN WITH DELIMITER ',' CSV HEADER;"
    """
    cmd_string = " ".join(cmd)
    result = run(cmd, shell= True, capture_output=True)
    return

def merge_manifest(args, sess):
    query = f""" 
BEGIN;
    DROP TABLE IF EXISTS {args.all_data_snapshot_dst};
    CREATE TABLE {args.all_data_snapshot_dst} AS (
    WITH
      source_outer_join AS (
        SELECT
          -- Get collection_name, patientID, StudyInstanceUID, SeriesInstanceUID, SOPInstanceUID, hash, ingestion_url, size from manifest
          codm.*,
          -- Get the following fields from all_data_snapshot_src. The manifest will not have that columns
          -- but we want to preserve them for existing instances. For new/revised instances the will be ""
          i_hash,
          se_excluded,
          i_excluded,
          mitigation,
          source_file_hash,
          idc_version
        FROM {args.temp_manifest_table} codm
        LEFT JOIN {args.all_data_snapshot_src} pf
          ON pf.SOPInstanceUID = codm.SOPInstanceUID
        --WHERE 
        --    pf.source_doi = '{args.source_doi}' 
        --    OR codm.source_doi = '{args.source_doi}'
        ORDER BY pf.patientID, pf.SOPInstanceUID
      ),
      source_metadata AS (
        SELECT
          soj.collection_name,
          REPLACE(REPLACE(LOWER(soj.collection_name), ' ', '_'), '-', '_')
            collection_id,
          '' c_hash,
          soj.patientID,
          '' p_hash,
          soj.StudyInstanceUID,
          '' st_hash,
          soj.SeriesInstanceUID,
          '' se_hash,
          CASE
            WHEN soj.i_hash IS NULL OR soj.i_hash != soj.hash THEN false
            ELSE soj.se_excluded
          END AS se_excluded,
    --       soj.source_doi,
          '{args.source_doi}' source_doi,
          'gs://doi.org/{args.source_doi}' source_url,
    --       soj.versioned_source_doi,
          '{args.versioned_source_doi}' versioned_source_doi,
          'gs://doi.org/{args.versioned_source_doi}') versioned_source_url,
          {args.analysis_result} analysis_result,
          soj.SOPInstanceUID,
          soj.hash i_hash,
          soj.ingestion_url,
          --IF(soj.i_hash IS NULL OR soj.i_hash != soj.hash, 0, soj.i_size) size,
          CASE
            WHEN soj.i_hash IS NULL OR soj.i_hash != soj.hash THEN 0
            ELSE soj.i_size
          END AS size,
          --IF(soj.i_hash IS NULL OR soj.i_hash != soj.hash, FALSE, soj.i_excluded)
          --  i_excluded,
          CASE
            WHEN soj.i_hash IS NULL OR soj.i_hash != soj.hash THEN false
            ELSE soj.i_excluded
          END AS i_excluded,
          --IF(soj.i_hash IS Null OR soj.i_hash != soj.hash, '{settings.CURRENT_VERSION}', soj.idc_version) 
          --  idc_version,
          CASE
            WHEN soj.i_hash IS Null OR soj.i_hash != soj.hash THEN '{settings.CURRENT_VERSION}'
            ELSE soj.idc_version 
          END AS idc_version,
          --IF(soj.i_hash IS NULL OR soj.i_hash != soj.hash, '', soj.mitigation)
          --  mitigation,
          CASE
            WHEN soj.i_hash IS NULL OR soj.i_hash != soj.hash THEN ''
            ELSE soj.mitigation
          END AS mitigation,
          --IF(
          --  soj.i_hash IS NULL OR soj.i_hash != soj.hash, '', soj.source_file_hash) source_file_hash
          CASE
            WHEN soj.i_hash IS NULL OR soj.i_hash != soj.hash THEN ''
            ELSE soj.source_file_hash
          END AS source_file_hash
        FROM source_outer_join soj
      ),
      non_source_metadata AS (
        SELECT pf.*
        FROM {args.all_data_snapshot_src} pf
        WHERE source_doi != '{args.source_doi}'
        ORDER BY patientID, SOPInstanceUID
      )
    SELECT * FROM source_metadata
    UNION ALL
    SELECT * FROM non_source_metadata
    );
END;
"""

    sess.execute(query)
    return

def drop_manifest_table(args, sess):
    query = f"""
DROP TABLE IF EXISTS {args.temp_manifest_table};
"""
    sess.execute(query)
    return

def prebuild_from_manifests(args):
    with sa_session() as sess:
        create_manifest_table(args, sess)
        merge_manifest(args,sess)
        drop_manifest_table(args, sess)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--version', default=settings.CURRENT_VERSION)
    parser.add_argument('--cloud_sql_instance', default='idc-dev-etl-psql-whc', help="ID of Cloud SQL instance")
    parser.add_argument('--db_name', default=f'idc_v{settings.CURRENT_VERSION}')
    parser.add_argument("--temp_manifest_table", default="tmp_manifest", help="Table into which we load the manifest")
    parser.add_argument("--all_data_snapshot_src", default="all_data_snapshot2", help="Table into which to merge manifest")
    parser.add_argument("--all_data_snapshot_dst", default="all_data_snapshot1", help="Table into which to write merged manifest")
    parser.add_argument('--manifest_url', default='gs://whc_etl_dev/catch_manifest.csv', help='csv/tsv complete manifest')
    parser.add_argument('--source_doi', default='10.5281/zenodo.18526942', help='DOI of source')
    parser.add_argument('--versioned_source_doi', default='10.5281/zenodo.18526943', help='Versioned DOI of source')
    parser.add_argument('--analysis_result', default=False, help='True if source is an analysis result')
    parser.add_argument('--validate', type=bool, default=True, help='True if validation is to be performed')
    parser.add_argument('--gen_hashes', type=bool, default=True, help='True if hashes are to be generated')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)

    prebuild_from_manifests(args)

