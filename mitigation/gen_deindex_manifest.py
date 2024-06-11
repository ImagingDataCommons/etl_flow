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


# Generate a manifest of new instance version in some set of IDC versions

import argparse
import settings
import json
from time import sleep
from google.cloud import bigquery
from utilities.bq_helpers import query_BQ, export_BQ_to_GCS, delete_BQ_Table
from utilities.logging_config import successlogger, progresslogger, errlogger

def gen_deindex_manifest(args):
    BQ_client = bigquery.Client(project=args.project)
    query = f"""
    WITH
      mult AS (
      SELECT
        DISTINCT 
        i_uuid,
        IF(i_source='tcia', 
          CONCAT('gs://', pub_gcs_bucket,'/', se_uuid, '/', i_uuid, '.dcm', ', s3://', pub_aws_bucket, '/', se_uuid, '/', i_uuid, '.dcm'),
          CONCAT('gs://', pub_gcs_bucket,'/', se_uuid, '/', i_uuid, '.dcm', ', s3://', pub_aws_bucket, '/', se_uuid, '/', i_uuid, '.dcm')
        ) urls
      FROM
        `idc-dev-etl.mitigation.redactions`    
    )
    SELECT
      CONCAT('dg.4DFC/',i_uuid) GUID,
      CONCAT('[', STRING_AGG(urls, ', ' ORDER BY SPLIT(urls, '/')[offset(3) ]), ']') url,
     FROM
      mult
    GROUP BY
      i_uuid
    ORDER BY GUID
      """

    results = query_BQ(BQ_client, args.temp_table_bqdataset, args.temp_table, query, write_disposition='WRITE_TRUNCATE')

    # Export the resulting table to GCS
    results = export_BQ_to_GCS(BQ_client, args.temp_table_bqdataset, args.temp_table, args.manifest_uri)

    while results.state == 'RUNNING':
        sleep(1)
        pass

if __name__ == '__main__':
    version = settings.CURRENT_VERSION
    mitigation_id = "m1"
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', default=settings.DEV_PROJECT)
    # parser.add_argument('--pub_bqdataset', default=settings.BQ_DEV_EXT_DATASET)
    # parser.add_argument('--dev_bqdataset', default=settings.BQ_DEV_INT_DATASET)
    # parser.add_argument('--version', default=settings.CURRENT_VERSION, \
    #                     help='Version for which to generate manifest.')
    parser.add_argument('--manifest_uri',
                        default=f'gs://indexd_manifests/dcf_input/pdp_hosting/idc_v{settings.CURRENT_VERSION}/idc_mitigation_{mitigation_id}_deindex_manifest_*.tsv',
                        help="GCS blob in which to save results")
    parser.add_argument('--temp_table_bqdataset', default='whc_dev', \
                        help='BQ dataset of temporary table')
    parser.add_argument('--temp_table', default=f'idc_v{settings.CURRENT_VERSION}_deindex_manifest', \
                        help='Temporary table in which to write query results')
    args = parser.parse_args()
    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    gen_deindex_manifest(args)
    # delete_BQ_Table(BQ_client, args.project, args.dst_bqdataset, args.temp_table)
