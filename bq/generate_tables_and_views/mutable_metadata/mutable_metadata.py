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

# The values in this table (other than the uuids) are potential mutable.
# The table can be used, by, e.g. joining it with dicom_all to obtain
# the current values of these columns for data in previous IDC versions.

import argparse
import sys
import settings
from google.cloud import bigquery
from utilities.bq_helpers import create_BQ_table, delete_BQ_Table, query_BQ
from utilities.logging_config import successlogger
from schema import mutable_metadata_schema

def gen_blob_table(args):

    # query = f"""
    # SELECT
    #   DISTINCT
    #   st_uuid crdc_study_uuid,
    #   se_uuid crdc_series_uuid,
    #   i_uuid crdc_instance_uuid,
    #   CONCAT('gs://',
    #   IF
    #     (aj.i_source='tcia', pub_gcs_tcia_url, pub_gcs_idc_url), '/', se_uuid, '/', i_uuid, '.dcm') gcs_url,
    #   CONCAT('s3://',
    #   IF
    #     (aj.i_source='tcia', pub_aws_tcia_url, pub_aws_idc_url), '/', se_uuid, '/', i_uuid, '.dcm') aws_url,
    # IF
    #   (aj.i_source='tcia', ac.tcia_access, ac.idc_access) access,
    #   source_url,
    #   source_doi,
    #   license_long_name,
    #   license_short_name,
    #   license_url
    # FROM
    #   `{args.src_project}.{args.src_bqdataset_name}.all_joined` aj
    # JOIN
    #   `{args.src_project}.{args.src_bqdataset_name}.all_collections` ac
    # ON
    #   ac.idc_collection_id=aj.idc_collection_id
    # ORDER BY
    #   crdc_study_uuid,
    #   crdc_series_uuid,
    #   crdc_instance_uuid
    # """

    query = f"""
    SELECT
      DISTINCT 
      st_uuid crdc_study_uuid,
      se_uuid crdc_series_uuid,
      i_uuid crdc_instance_uuid,
      CONCAT('gs://',
          IF
            (i_source='tcia', pub_gcs_tcia_url, pub_gcs_idc_url), '/', se_uuid, '/', i_uuid, '.dcm') gcs_url,
      CONCAT('s3://',
           IF
            (i_source='tcia', pub_aws_tcia_url, pub_aws_idc_url), '/', se_uuid, '/', i_uuid, '.dcm') aws_url,
      IF
          (i_source='tcia', tcia_access, idc_access) access,
      source_url,
      source_doi,
      license_long_name,
      license_short_name,
      license_url
    FROM
      `{args.src_project}.{args.src_bqdataset_name}.all_joined`
    ORDER BY
      crdc_study_uuid,
      crdc_series_uuid,
      crdc_instance_uuid    
    """
    # client = bigquery.Client(project=args.dst_project)
    # result=query_BQ(client, args.trg_bqdataset_name, args.bqtable_name, query, write_disposition='WRITE_TRUNCATE')

    client = bigquery.Client(project=args.dst_project)
    result = delete_BQ_Table(client, args.dst_project, args.trg_bqdataset_name, args.bqtable_name)
    # Create a table to get the schema defined
    created_table = create_BQ_table(client, args.dst_project, args.trg_bqdataset_name, args.bqtable_name, mutable_metadata_schema, exists_ok=True)
    # Perform the query and save results in specified table
    results = query_BQ(client, args.trg_bqdataset_name, args.bqtable_name, query, write_disposition='WRITE_TRUNCATE')
    populated_table = client.get_table(f"{args.dst_project}.{args.trg_bqdataset_name}.{args.bqtable_name}")
    populated_table.schema = mutable_metadata_schema
    populated_table.description = "Current values of metadata that might change over time"
    client.update_table(populated_table, fields=["schema", "description"])
    successlogger.info('Created mutable_metadata table')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version for which to build the table')
    parser.add_argument('--src_project', default=f'{settings.DEV_PROJECT}')
    parser.add_argument('--dst_project', default=f'{settings.DEV_PROJECT}')
    parser.add_argument('--src_bqdataset_name', default=f'idc_v{settings.CURRENT_VERSION}_dev', help='BQ dataset name')
    parser.add_argument('--trg_bqdataset_name', default=f'idc_v{settings.CURRENT_VERSION}_pub', help='BQ dataset name')
    parser.add_argument('--bqtable_name', default=f'mutable_metadata', help='BQ table name')
    args = parser.parse_args()

    print("{}".format(args), file=sys.stdout)

    # args.sql = open(args.sql).read()

    gen_blob_table(args)
