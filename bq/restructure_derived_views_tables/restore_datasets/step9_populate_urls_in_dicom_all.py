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

"""
Populate the gcs_url and aws_url columns of dicom_all
"""

import settings
import argparse
import json
import time
from google.cloud import bigquery
from utilities.logging_config import successlogger, progresslogger, errlogger

# args.trg_project: idc_pdp_staging
# args.trg_dataset: idc_vX
# args.dev_or_pub: 'dev' or 'pub'
def populate_urls_in_dicom_all(args, dones):
    if args.dataset_version < 10:
        progresslogger.info(f'Skipping add_aws_column_to_dicom_all_{args.trg_dataset}')
    if f'populate_urls_in_dicom_all_{args.trg_dataset}' not in dones:
        client = bigquery.Client()

        # Some versions of dicom_all have gcs_bucket column
        table_schema = client.get_table(f'{args.trg_project}.{args.trg_dataset}.dicom_all').schema  # Make an API request.
        # Determine whether this version of dicom_all has a gcs_bucket column
        has_gcs_bucket = next((item for item in table_schema if item.name == 'gcs_bucket'),-1) != -1

        if has_gcs_bucket:
            query = f"""
            UPDATE `{args.trg_project}.{args.trg_dataset}.dicom_all` am
            SET 
            am.gcs_url = IF('{args.dev_or_pub}'='dev', uum.dev_gcs_url, uum.pub_gcs_url), 
            am.aws_url = IF('{args.dev_or_pub}'='dev', uum.dev_aws_url, uum.pub_aws_url), 
            am.gcs_bucket = IF('{args.dev_or_pub}'='dev', uum.dev_gcs_bucket, uum.pub_gcs_bucket)
            FROM 
                (SELECT DISTINCT 
                        aj.idc_collection_id, aj.se_uuid, aj.i_uuid AS uuid,
                        IF(aj.i_source='tcia', ac.dev_tcia_url, ac.dev_idc_url) AS dev_gcs_bucket,
                        IF(aj.i_source='tcia', ac.pub_aws_tcia_url, ac.pub_aws_idc_url) AS dev_aws_bucket,
                        IF(aj.i_source='tcia', ac.pub_gcs_tcia_url, ac.pub_gcs_idc_url) AS pub_gcs_bucket,
                        IF(aj.i_source='tcia', ac.pub_aws_tcia_url, ac.pub_aws_idc_url) AS pub_aws_bucket,
                        CONCAT(
                          'gs://',
                          IF(aj.i_source='tcia', ac.dev_tcia_url, ac.dev_idc_url),
                          '/', aj.se_uuid, '/', aj.i_uuid, '.dcm')
                        AS dev_gcs_url,
                        CONCAT(
                          's3://',
                          IF(aj.i_source='tcia', ac.pub_aws_tcia_url, ac.pub_aws_idc_url),
                          '/', aj.se_uuid, '/', aj.i_uuid, '.dcm')
                        AS dev_aws_url,
                        CONCAT(
                          'gs://',
                          IF(aj.i_source='tcia', ac.pub_gcs_tcia_url, ac.pub_gcs_idc_url),
                          '/', aj.se_uuid, '/', aj.i_uuid, '.dcm')
                         AS pub_gcs_url,
                        CONCAT(
                          's3://',
                          IF(aj.i_source='tcia', ac.pub_aws_tcia_url, ac.pub_aws_idc_url),
                          '/', aj.se_uuid, '/', aj.i_uuid, '.dcm')
                        AS pub_aws_url,
                        IF(aj.i_source='tcia', ac.tcia_access , ac.idc_access) AS access, 
                        i_source AS source
                        FROM `{args.dev_project}.{args.dev_dataset}.all_joined` aj
                        JOIN `{args.dev_project}.{args.dev_dataset}.all_collections` ac
                        on aj.collection_id = ac.tcia_api_collection_id
                    ) as uum
            WHERE am.instance_uuid = uum.uuid
            AND am.series_uuid = uum.se_uuid
            """
        else:
            query = f"""
            UPDATE `{args.trg_project}.{args.trg_dataset}.dicom_all` am
            SET 
            am.gcs_url = IF('{args.dev_or_pub}'='dev', uum.dev_gcs_url, uum.pub_gcs_url), 
            am.aws_url = IF('{args.dev_or_pub}'='dev', uum.dev_aws_url, uum.pub_aws_url) 
            FROM 
                (SELECT DISTINCT 
                        aj.idc_collection_id, aj.se_uuid, aj.i_uuid AS uuid,
                        IF(aj.i_source='tcia', ac.dev_tcia_url, ac.dev_idc_url) AS dev_gcs_bucket,
                        IF(aj.i_source='tcia', ac.pub_aws_tcia_url, ac.pub_aws_idc_url) AS dev_aws_bucket,
                        IF(aj.i_source='tcia', ac.pub_gcs_tcia_url, ac.pub_gcs_idc_url) AS pub_gcs_bucket,
                        IF(aj.i_source='tcia', ac.pub_aws_tcia_url, ac.pub_aws_idc_url) AS pub_aws_bucket,
                        CONCAT(
                          'gs://',
                          IF(aj.i_source='tcia', ac.dev_tcia_url, ac.dev_idc_url),
                          '/', aj.se_uuid, '/', aj.i_uuid, '.dcm')
                        AS dev_gcs_url,
                        CONCAT(
                          's3://',
                          IF(aj.i_source='tcia', ac.pub_aws_tcia_url, ac.pub_aws_idc_url),
                          '/', aj.se_uuid, '/', aj.i_uuid, '.dcm')
                        AS dev_aws_url,
                        CONCAT(
                          'gs://',
                          IF(aj.i_source='tcia', ac.pub_gcs_tcia_url, ac.pub_gcs_idc_url),
                          '/', aj.se_uuid, '/', aj.i_uuid, '.dcm')
                         AS pub_gcs_url,
                        CONCAT(
                          's3://',
                          IF(aj.i_source='tcia', ac.pub_aws_tcia_url, ac.pub_aws_idc_url),
                          '/', aj.se_uuid, '/', aj.i_uuid, '.dcm')
                        AS pub_aws_url,
                        IF(aj.i_source='tcia', ac.tcia_access , ac.idc_access) AS access, i_source source
                        FROM `{args.dev_project}.{args.dev_dataset}.all_joined` aj
                        JOIN `{args.dev_project}.{args.dev_dataset}.all_collections` ac
                        on aj.collection_id = ac.tcia_api_collection_id
                    ) as uum
            WHERE am.instance_uuid = uum.uuid
            AND am.series_uuid = uum.se_uuid
            """

        job = client.query(query)
        while not job.done():
            print('Waiting for job done. Status: {}'.format(job.state))
            time.sleep(5)
        progresslogger.info(f'Populate urls in dicom_all; errors: {job.error_result}')
        successlogger.info(f'populate_urls_in_dicom_all_{args.trg_dataset}')
    else:
        progresslogger.info(f'Skipping populate_urls_in_dicom_all_{args.trg_dataset}')
        return
