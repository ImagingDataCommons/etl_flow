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
        `{args.project}.{args.dev_bqdataset}.version` ),
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
        IF(i_source='tcia', 
          CONCAT('gs://', ac.pub_gcs_tcia_url,'/', se_uuid, '/', i_uuid, '.dcm', ', s3://', ac.pub_aws_tcia_url,'/', se_uuid, '/', i_uuid, '.dcm'),
          CONCAT('gs://', ac.pub_gcs_idc_url,'/', se_uuid, '/', i_uuid, '.dcm', ', s3://', ac.pub_aws_idc_url,'/', se_uuid, '/', i_uuid, '.dcm')
        ) urls
      FROM
        `{args.project}.{args.dev_bqdataset}.all_joined` aj
      JOIN
        `{args.project}.{args.dev_bqdataset}.all_collections` ac
      ON
        aj.collection_id = ac.tcia_api_collection_id
      JOIN
        ts
      ON
        1=1
      WHERE
        i_rev_idc_version IN {args.versions}
        AND 
            ((i_source='tcia'
            AND tcia_access='Public')
          OR (i_source='idc'
            AND idc_access='Public')) )
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
      sop_instance_uid name
    FROM
      mult
    JOIN
      ts
    ON
      1=1
    GROUP BY
      sop_instance_uid,
      i_uuid,
      i_hash,
      i_size,
      created,
      updated,
      i_rev_idc_version
    ORDER BY LENGTH(url) DESC
      """

    results = query_BQ(BQ_client, args.temp_table_bqdataset, args.temp_table, query, write_disposition='WRITE_TRUNCATE')

    # Export the resulting table to GCS
    results = export_BQ_to_GCS(BQ_client, args.temp_table_bqdataset, args.temp_table, args.manifest_uri)

    while results.state == 'RUNNING':
        sleep(1)
        pass

    # delete_BQ_Table(BQ_client, args.project, args.dst_bqdataset, args.temp_table)
