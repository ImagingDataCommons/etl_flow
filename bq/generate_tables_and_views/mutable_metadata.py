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

# The values in this table (other than the uuids) are potentially mutable.
# The table can be used, by, e.g. joining it with dicom_all to obtain
# the current values of these columns for data in previous IDC versions.

import argparse
import sys
import settings
from google.cloud import bigquery
from utilities.bq_helpers import create_BQ_table, delete_BQ_Table, query_BQ
from utilities.logging_config import successlogger

mutable_metadata_schema = [
    bigquery.SchemaField('crdc_study_uuid', 'STRING', mode='NULLABLE', description='UUID of this version of the study containing this instance'),
    bigquery.SchemaField('crdc_series_uuid', 'STRING', mode='NULLABLE', description='UUID of this version of the series containing this instance'),
    bigquery.SchemaField('crdc_instance_uuid', 'STRING', mode='NULLABLE', description='UUID of this version of this instance'),
    bigquery.SchemaField('gcs_url', 'STRING', mode='NULLABLE', description='URL to this object containing the current version of this instance in Google Cloud Storage (GCS)'),
    bigquery.SchemaField('aws_url', 'STRING', mode='NULLABLE', description='URL to this object containing the current version of this instance in Amazon Web Services (AWS)'),
    bigquery.SchemaField('access', 'STRING', mode='NULLABLE', description='Collection access status: Public or Limited'),
    bigquery.SchemaField('source_doi', 'STRING', mode='NULLABLE', description='The DOI of a wiki page that describes the original collection or analysis result that includes this instance'),
    bigquery.SchemaField('source_url', 'STRING', mode='NULLABLE', description='The URL of a wiki page that describes the original collection or analysis result that includes this instance'),
    bigquery.SchemaField('versioned_source_doi', 'STRING', mode='NULLABLE', description='If present, the DOI of a wiki page that describes the original collection or analysis result that includes this version of this instance'),
    bigquery.SchemaField('license_url', 'STRING', mode='NULLABLE', description='URL of license of this analysis result'),
    bigquery.SchemaField('license_long_name', 'STRING', mode='NULLABLE', description='Long name of license of this analysis result'),
    bigquery.SchemaField('license_short_name', 'STRING', mode='NULLABLE', description='Short name of license of this analysis result'),
]

def gen_blob_table(args):

    query = f"""
    SELECT
      DISTINCT 
      st_uuid crdc_study_uuid,
      se_uuid crdc_series_uuid,
      i_uuid crdc_instance_uuid,
      CONCAT('gs://',
          pub_gcs_bucket, '/', se_uuid, '/', i_uuid, '.dcm') gcs_url,
      CONCAT('s3://',
          pub_aws_bucket, '/', se_uuid, '/', i_uuid, '.dcm') aws_url,
      access,
      ajc.source_url,
      ajc.source_doi,
      versioned_source_doi,
      license.license_long_name,
      license.license_short_name,
      license.license_url
    FROM
      `{args.src_project}.{args.src_bqdataset_name}.all_joined_public` ajc
    JOIN `{args.src_project}.{args.src_bqdataset_name}.licenses` l
    ON ajc.source_doi = l.source_doi
    ORDER BY
      crdc_study_uuid,
      crdc_series_uuid,
      crdc_instance_uuid    
    """

    client = bigquery.Client(project=args.dst_project)
    result = delete_BQ_Table(client, args.dst_project, args.trg_bqdataset_name, args.bqtable_name)
    # Create a table to get the schema defined
    created_table = create_BQ_table(client, args.dst_project, args.trg_bqdataset_name, args.bqtable_name, mutable_metadata_schema, exists_ok=True)
    # Perform the query and save results in specified table
    results = query_BQ(client, args.trg_bqdataset_name, args.bqtable_name, query, write_disposition='WRITE_TRUNCATE')
    populated_table = client.get_table(f"{args.dst_project}.{args.trg_bqdataset_name}.{args.bqtable_name}")
    populated_table.schema = mutable_metadata_schema
    populated_table.description = "Current values of public instance metadata that might have changed over time"
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
