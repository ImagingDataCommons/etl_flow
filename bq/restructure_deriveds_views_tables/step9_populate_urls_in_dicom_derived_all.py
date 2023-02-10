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
Populate the gcs_url and aws_url columns of dicom_derived_all
"""
import settings
import argparse
import json
import time
from google.cloud import bigquery
from utilities.logging_config import successlogger, progresslogger, errlogger

def revise_dicom_derived_all_urls(args):
    client = bigquery.Client()

    # Some versions of auxiliary_metadata have gcs_bucket_column
    table_schema = client.get_table(f'{args.project}.{args.trg_dataset}.dicom_derived_all').schema  # Make an API request.
    # Determine whether this version of auxiliary_metadata has a gcs_bucket column
    has_gcs_url = next((item for item in table_schema if item.name == 'gcs_url'),-1) != -1

    if has_gcs_url:
        # has_gcs_bucket = next((item for item in table_schema if item.name == 'gcs_bucket'), -1) != -1
        # if has_gcs_bucket:
        #     query = f"""
        #     UPDATE `{args.project}.{args.trg_dataset}.auxiliary_metadata` am
        #     SET
        #     am.gcs_url = IF('{args.dev_or_pub}'='dev', uum.dev_gcs_url, uum.pub_gcs_url),
        #     am.aws_url = IF('{args.dev_or_pub}'='dev', uum.dev_aws_url, uum.pub_aws_url),
        #     am.gcs_bucket = IF('{args.dev_or_pub}'='dev', uum.dev_gcs_bucket, uum.pub_gcs_bucket)
        #     FROM
        #         (SELECT DISTINCT
        #                 aj.idc_collection_id, aj.se_uuid, aj.i_uuid AS uuid,
        #                 IF(aj.i_source='tcia', ac.dev_tcia_url, ac.dev_idc_url) AS dev_gcs_bucket,
        #                 IF(aj.i_source='tcia', ac.pub_aws_tcia_url, ac.pub_aws_idc_url) AS dev_aws_bucket,
        #                 IF(aj.i_source='tcia', ac.pub_gcs_tcia_url, ac.pub_gcs_idc_url) AS pub_gcs_bucket,
        #                 IF(aj.i_source='tcia', ac.pub_aws_tcia_url, ac.pub_aws_idc_url) AS pub_aws_bucket,
        #                 CONCAT(
        #                   'gs://',
        #                   IF(aj.i_source='tcia', ac.dev_tcia_url, ac.dev_idc_url),
        #                   '/', aj.se_uuid, '/', aj.i_uuid, '.dcm')
        #                 AS dev_gcs_url,
        #                 CONCAT(
        #                   's3://',
        #                   IF(aj.i_source='tcia', ac.pub_aws_tcia_url, ac.pub_aws_idc_url),
        #                   '/', aj.se_uuid, '/', aj.i_uuid, '.dcm')
        #                 AS dev_aws_url,
        #                 CONCAT(
        #                   'gs://',
        #                   IF(aj.i_source='tcia', ac.pub_gcs_tcia_url, ac.pub_gcs_idc_url),
        #                   '/', aj.se_uuid, '/', aj.i_uuid, '.dcm')
        #                  AS pub_gcs_url,
        #                 CONCAT(
        #                   's3://',
        #                   IF(aj.i_source='tcia', ac.pub_aws_tcia_url, ac.pub_aws_idc_url),
        #                   '/', aj.se_uuid, '/', aj.i_uuid, '.dcm')
        #                 AS pub_aws_url,
        #                 IF(aj.i_source='tcia', ac.tcia_access , ac.idc_access) AS access, i_source source
        #                 FROM `{args.project}.{args.dev_dataset}.all_joined` aj
        #                 JOIN `{args.project}.{args.dev_dataset}.all_collections` ac
        #                 on aj.collection_id = ac.tcia_api_collection_id
        #             ) as uum
        #     WHERE am.instance_uuid = uum.uuid
        #     AND am.series_uuid = uum.se_uuid
        #     """
        # else:
        if True:
            query = f"""
            UPDATE `{args.project}.{args.trg_dataset}.dicom_derived_all` am
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
                        FROM `{args.project}.{args.dev_dataset}.all_joined` aj
                        JOIN `{args.project}.{args.dev_dataset}.all_collections` ac
                        on aj.collection_id = ac.tcia_api_collection_id
                    ) as uum
            WHERE am.instance_uuid = uum.uuid
            AND am.series_uuid = uum.se_uuid
            """

        job = client.query(query)
        while not job.done():
            print('Waiting for job done. Status: {}'.format(job.state))
            time.sleep(5)
        progresslogger.info(f'Populated urls in dicom_derived_all; errors: {job.error_result}')
    else:
        progresslogger.info(f'No url columns in dicom_derived_all')
    return


if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()

    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version number')
    parser.add_argument('--project', default="idc-dev-etl", help='Project in which tables live')
    parser.add_argument('--dev_dataset', default=f"idc_v{settings.CURRENT_VERSION}_dev", help="BQ source dataset")
    parser.add_argument('--trg_dataset', default=f"whc_dev_idc_v1", help="BQ targetdataset")
    # parser.add_argument('--uuid_url_map', default="idc-dev-etl.idc_v14_dev.uuid_url_map",
    #                     help="Table that maps instance uuids to URLS")
    parser.add_argument('--dev_or_pub', default='dev', help='Revising the dev or pub version of auxiliary_metadata')
    args = parser.parse_args()

    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    revise_dicom_derived_all_urls(args)
