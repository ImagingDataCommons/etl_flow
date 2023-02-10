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
Add an aws_url column to dicom_pivot_vX view
"""
import settings
import argparse
import json
import time
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import NotFound
from utilities.logging_config import successlogger, progresslogger, errlogger


def revise_dicom_all_schema(view):

    # Add aws_url to the schema
    schema = view.schema
    # Find the gcs_url field
    index = next(index for index, field in enumerate(schema) if field.name == 'gcs_url')
    if index+1 == len(schema) or schema[index+1].name != 'aws_url':
        # Build an identical field with the name 'aws_url'
        try:
            aws_description = schema[index].description.replace('Google Cloud Storage (GCS)', 'Amazon Cloud Services (AWS)')
        except:
            aws_description = None
        aws_field = SchemaField(
            'aws_url',
            field_type=schema[index].field_type,
            mode = schema[index].mode,
            description = aws_description
        )
        schema.insert(index+1, aws_field)
    return schema


# Add aws_url to the SQL
def revise_dicom_all_sql(arg, view_id, view):
    # We need to find the gcs_url element in the select; we
    # use it as a model for the aws_url

    original_sql = view.view_query # Make an API request.
    # Add aws_url if not already in the sql
    if original_sql.find('aws_url') == -1:
        # Find an index in the item
        index = original_sql.find('gcs_url')
        # Find the first comma that precedes the gcs_url item
        left_index = original_sql.rfind(',', 0, index)
        # Stitch together a new SQL that includes an AWS item,
        revised_sql = original_sql[:left_index+1] + original_sql[left_index+1:index+len('gcs_url')].replace('gcs_url', 'aws_url') + \
            ',' + original_sql[left_index+1:]
        return revised_sql
    else:
        return original_sql

def revise_dicom_derived(client, args, view_id, view, metadata):
    new_view = bigquery.Table(view_id)
    new_view.view_query = metadata['view_query']
    new_view.friendly_name = metadata['friendly_name']
    new_view.description = metadata['description']
    new_view.labels = metadata['labels']

    # Delete the existing view
    client.delete_table(view, not_found_ok=True)
    installed_view = client.create_table(new_view)

    # Update the schema after creating the view
    installed_view.schema = metadata['schema']
    client.update_table(installed_view, ['schema'])

    progresslogger.info(f'Added aws_url column to view dicom_pivot_v{args.dataset_version}')

    return


def add_aws_url_column_to_dicom_pivot(args):
    client = bigquery.Client()
    view_id = f'{args.project}.{args.trg_dataset}.dicom_pivot_v{args.dataset_version}'
    view = client.get_table(view_id)

    metadata = {}
    metadata['schema'] = revise_dicom_all_schema(view)
    metadata['view_query'] = revise_dicom_all_sql(args, view_id, view)
    metadata['friendly_name'] = view.friendly_name
    metadata['description'] = view.description
    metadata['labels'] = view.labels

    # Now create table/view pair as needed
    revise_dicom_derived(client, args, view_id, view, metadata)
    # revise_dicom_all_table(client, table_id, metadata)
    return


if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()

    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version number')
    parser.add_argument('--project', default="idc-dev-etl", help='Project in which tables live')
    parser.add_argument('--trg_dataset', default=f"whc_dev_idc_v5", help="BQ target dataset")
    args = parser.parse_args()

    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    add_aws_url_column_to_dicom_pivot(args)
