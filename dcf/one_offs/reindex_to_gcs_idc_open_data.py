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


# Generate a manifest of new instance versions in the current (latest) IDC version

import argparse
import settings
from dcf.gen_instance_manifest.instance_manifest import gen_instance_manifest
import json
from utilities.logging_config import successlogger, progresslogger, errlogger

import argparse
import settings
from time import sleep
from google.cloud import bigquery
from utilities.bq_helpers import query_BQ, export_BQ_to_GCS, delete_BQ_Table

def gen_instance_manifest(args):
    BQ_client = bigquery.Client(project=args.project)
    query = f"""
    WITH
      ts AS (
      SELECT
        ARRAY_AGG(max_timestamp) timestamps
      FROM
        `{args.project}.idc_v{args.version}_dev.version` ),
      mult AS (
      SELECT
        DISTINCT
        sop_instance_uid,
        se_uuid,
        i_uuid,
        i_hash,
        i_size,
        i_rev_idc_version,
        ts.timestamps[ORDINAL(i_init_idc_version)] AS created,
        ts.timestamps[ORDINAL(i_rev_idc_version)] AS updated,
        CONCAT('gs://', pub_gcs_bucket,'/', se_uuid, '/', i_uuid, '.dcm', ', s3://', pub_aws_bucket,'/', se_uuid, '/', i_uuid, '.dcm') urls
      FROM
        `{args.project}.idc_v{args.version}_dev.all_joined_public` aj
      JOIN
        ts
      ON
        1=1
      WHERE idc_version = {args.version} AND pub_gcs_bucket = 'idc-open-data' AND i_rev_idc_version < 20
    )
    SELECT
      CONCAT('dg.4DFC/',i_uuid) GUID,
      i_hash md5,
      i_size size,
      '*' acl,
      CONCAT('[', STRING_AGG(urls, ', ' ORDER BY SPLIT(urls, '/')[offset(3) ]), ']') url,
      created content_created_timestamp,
      updated content_updated_timestamp,
      'DICOM instance' description,
      concat('IDC version: ', i_rev_idc_version ) version,
      sop_instance_uid name,
      CONCAT('[', STRING_AGG(REPLACE(urls, 'idc-open-data', 'public-datasets-idc'), ', ' ORDER BY SPLIT(REPLACE(urls, 'idc-open-data', 'public-datasets-idc'), '/')[offset(3) ]), ']') previous_url
    FROM
      mult
    GROUP BY
      sop_instance_uid,
      i_uuid,
      i_hash,
      i_size,
      created,
      updated,
      i_rev_idc_version
    ORDER BY GUID
      """

    results = query_BQ(BQ_client, args.temp_table_bqdataset, args.temp_table, query, write_disposition='WRITE_TRUNCATE')

    # Export the resulting table to GCS
    results = export_BQ_to_GCS(BQ_client, args.temp_table_bqdataset, args.temp_table, args.manifest_uri)

    while results.state == 'RUNNING':
        sleep(1)
        pass

    # delete_BQ_Table(BQ_client, args.project, args.dst_bqdataset, args.temp_table)


if __name__ == '__main__':
    version = 20
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', default=settings.DEV_PROJECT)
    parser.add_argument('--pub_bqdataset', default=settings.BQ_DEV_EXT_DATASET)
    parser.add_argument('--dev_bqdataset', default=settings.BQ_DEV_INT_DATASET)
    parser.add_argument('--version', default=version, \
            help= 'Version for which to generate manifest.')
    parser.add_argument('--manifest_uri', default=f'gs://indexd_manifests/dcf_input/pdp_hosting/idc_v{version}/idc_v{version}_reindexing_manifest_*.tsv',
            help="GCS blob in which to save results")
    parser.add_argument('--temp_table_bqdataset', default='whc_dev', \
            help='BQ dataset of temporary table')
    parser.add_argument('--temp_table', default=f'idc_v{version}_reindexing_manifest', \
            help='Temporary table in which to write query results')
    args = parser.parse_args()
    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    gen_instance_manifest(args)


