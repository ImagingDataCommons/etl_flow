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


def revise_dicom_pivot_schema(view):
    # Remove aws_url from the schema
    schema = view.schema
    # Find the aws_url field
    index = next(index for index, field in enumerate(schema) if field.name == 'aws_url')
    schema.pop(index)
    return schema


# Add aws_url to the SQL
def revise_dicom_pivot_sql(arg, view_id, view):
    # Remove aws_url from the SQL
    original_sql = view.view_query # Make an API request.
    if   ',  pivot.aws_url\n' in original_sql:
        revised_sql = original_sql.replace(',  pivot.aws_url\n', '')
    elif '  pivot.aws_url,\n' in original_sql:
        revised_sql = original_sql.replace('  pivot.aws_url,\n', '')
    elif '  pivot.aws_url,\r' in original_sql:
        revised_sql = original_sql.replace('  pivot.aws_url,\r\n', '')
    elif ', aws_url' in original_sql:
        revised_sql = original_sql.replace(', aws_url', '')
    elif '  dicom_all.aws_url, \n' in original_sql:
        revised_sql = original_sql.replace('  dicom_all.aws_url, \n', '')
    else:
        errlogger.error(f"Didn't find aws_url in {view_id}" )
    return revised_sql

def revise_dicom_pivot(client, args, view_id, view, metadata):
    new_view = bigquery.Table(view_id)
    new_view.view_query = metadata['view_query']
    new_view.friendly_name = metadata['friendly_name']
    new_view.description = metadata['description']
    new_view.labels = metadata['labels']

    # Delete the existing view
    client.delete_table(view, not_found_ok=True)
    installed_view = client.create_table(new_view)

    # # # No description in dicom_pivot_vX schema
    # # Update the schema after creating the view
    # installed_view.schema = metadata['schema']
    # client.update_table(installed_view, ['schema'])

    progresslogger.info(f'Added aws_url column to view dicom_pivot_v{args.dataset_version}')

    return


def remove_aws_url_column_from_dicom_pivot(args, dones):
    client = bigquery.Client()
    view_id = f'{args.trg_project}.{args.trg_dataset}.dicom_pivot_v{args.dataset_version}'
    view = client.get_table(view_id)
    if view_id not in dones:

        metadata = {}
        metadata['schema'] = revise_dicom_pivot_schema(view)
        metadata['view_query'] = revise_dicom_pivot_sql(args, view_id, view)
        metadata['friendly_name'] = view.friendly_name
        metadata['description'] = view.description
        metadata['labels'] = view.labels

        # Now create table/view pair as needed
        revise_dicom_pivot(client, args, view_id, view, metadata)
        successlogger.info(f'{view_id}')
    else:
        progresslogger.info(f'Skipping {view_id}')
    # revise_dicom_all_table(client, table_id, metadata)
    return
