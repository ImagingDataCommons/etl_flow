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
Add an aws_url column to dicom_all view and table
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
    # Remove aws_url from the schema
    schema = view.schema
    # Find the aws_url field
    index = next(index for index, field in enumerate(schema) if field.name == 'aws_url')
    schema.pop(index)

    return schema


def revise_dicom_all_sql(arg, view_id, view):
    # Remove aws_url from the SQL
    original_sql = view.view_query # Make an API request.
    if ',\n    aux.aws_url as aws_url' in original_sql:
        revised_sql = original_sql.replace(',\n    aux.aws_url as aws_url', '')
    else:
        errlogger.error(f"Didn't find aws_url in {view_id}" )
    return revised_sql


def revise_dicom_all_view(client, view_id, view, metadata):
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


    return


def remove_aws_url_column_from_dicom_all_view(args, dones):
    client = bigquery.Client()
    view_id = f'{args.trg_project}.{args.trg_dataset}.dicom_all'
    view = client.get_table(view_id)
    if view.table_type == 'TABLE':
        view_id = f'{args.trg_project}.{args.trg_dataset}.dicom_all_view'
        try:
            view = client.get_table(view_id)
        except NotFound:
            progresslogger.info(f'{view_id} not found')
            return
    if view_id not in dones:
        metadata = {}
        metadata['schema'] = revise_dicom_all_schema(view)
        metadata['view_query'] = revise_dicom_all_sql(args, view_id, view)
        metadata['friendly_name'] = view.friendly_name
        metadata['description'] = view.description
        metadata['labels'] = view.labels

        # Now create table/view pair as needed
        revise_dicom_all_view(client, view_id, view, metadata)
        successlogger.info(f'{view_id}')
    else:
        progresslogger.info(f'Skipping {view_id}')

    # Note, we revise dicom_all (table) schema when we populate its urls
    return


# if __name__ == '__main__':
#     # (sys.argv)
#     parser = argparse.ArgumentParser()
#
#     # parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version number')
#     # parser.add_argument('--project', default="idc-dev-etl", help='Project in which tables live')
#     # parser.add_argument('--trg_dataset', default=f"whc_dev_idc_v5", help="BQ target dataset")
#     args = parser.parse_args()
#
#     progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')
#
#     # remove_aws_url_column_from_dicom_all(args)
