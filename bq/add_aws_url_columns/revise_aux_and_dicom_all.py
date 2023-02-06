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
Revise the derived tables/views such that there is both a table
and a view version of each
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

def create_view_from_table(client, args, table, metadata):
    view_id = f'{table.project}.{args.dataset}.{table.table_id}_view'
    new_view = bigquery.Table(view_id)
    new_view.view_query = metadata['view_query']
    new_view.friendly_name = metadata['friendly_name']
    new_view.description = metadata['description']
    new_view.labels = metadata['labels']
    installed_view = client.create_table(new_view)

    # Update the schema after creating the view
    installed_view.schema = metadata['schema']
    client.update_table(installed_view,['schema'])

    progresslogger.info(f'Created view {view_id}_view')
    return

def create_table_from_view(client, args, table, metadata):
    # # We first create a view named like <table_id)_view
    # create_view_from_table(client, args, table, )

    view_id = f'{table.project}.{args.dataset}.{table.table_id}'
    new_table = bigquery.Table(view_id)
    # new_table.view_query = revised_sql
    new_table.friendly_name = metadata['friendly_name']
    new_table.description = metadata['description']
    new_table.labels = metadata['labels']
    view_query = metadata['view_query']
    schema = metadata['schema']
    client.delete_table(table)
    client.create_table(new_table)

    job_config = bigquery.QueryJobConfig(destination=view_id)
    job = client.query(view_query, job_config=job_config)
    # Wait for the query to complete
    result = job.result()

    installed_table = client.get_table(view_id)

    # Update the schema after creating the view
    installed_table.schema = schema
    client.update_table(installed_table,['schema'])

    progresslogger.info(f'Created view {view_id}')
    return


def clone_derived(args, table_id):
    client = bigquery.Client()
    table_id = f'{args.project}.{args.dataset}.{table_id}'
    try:
        table = client.get_table(table_id)
    except NotFound:
        # The table doesn't exist in this dataset
        return

    metadata = {}
    metadata['schema'] = table.schema
    metadata['view_query'] = table.view_query
    metadata['friendly_name'] = table.friendly_name
    metadata['description'] = table.description
    metadata['labels'] = table.labels
    if table.table_type == 'TABLE':
        # The table already exists; Check whether the view also exists
        view_id = f'{args.project}.{args.dataset}.{table_id}_view'
        try:
            table = client.get_table(view_id)
            # Both the table and view exist, so we are done
            return
        except NotFound:
            # The corresponding view does not exist
            create_view_from_table(client, args, table, metadata)
    else:
        # The view already exists; create the table
        create_table_from_view(client, args, table, metadata)
        # We deleted the view to reuse its name. Recreate it.
        create_view_from_table(client, args, table, metadata)

    return


def revise_dicom_all_schema(view):

    # Add aws_url to the schema
    schema = view.schema
    # Find the gcs_url field
    index = next(index for index, field in enumerate(schema) if field.name == 'gcs_url')
    # Build an identical field with the name 'aws_url'
    if schema[index].description:
        aws_description = schema[index].description.replace('Google Cloud Storage (GCS)', ' Amazon Cloud Services (AWS)')
    else:
        aws_description = None
    aws_field = SchemaField(
        'aws_url',
        field_type=schema[index].field_type,
        mode = schema[index].mode,
        description = aws_description
    )
    schema.insert(5, aws_field)
    return schema


def revise_dicom_all_sql(arg, view_id, view):
    # Add aws_url to the SQL
    original_sql = view.view_query # Make an API request.
    index = original_sql.find('    aux.gcs_url as gcs_url,\n') + len('    aux.gcs_url as gcs_url,\n')
    revised_sql = original_sql[:index] + '    aux.aws_url as aws_url,\n' + original_sql[index:]

    # Add a prefix to all datasets in the SQL. Used during development
    if args.dataset_prefix:
        temp_sql = revised_sql
        offset = 0
        while index := revised_sql.find('idc-dev-etl.', offset):
            if index != -1:
                revised_sql = revised_sql[:(index+len('idc-dev-etl.'))] + args.dataset_prefix + \
                        revised_sql[(index + len('idc-dev-etl.')):]
                offset = index + len('idc-dev-etl.')
            else:
                break
    return revised_sql


def create_dicom_all_table_from_view(client, view_id, view, metadata):
    new_table = bigquery.Table(view_id)
    # new_table.view_query = revised_sql
    new_table.friendly_name = metadata['friendly_name']
    new_table.description = metadata['description']
    new_table.labels = metadata['labels']
    # Delete the original view. We will use its name for the table
    client.delete_table(view)
    client.create_table(new_table)

    job_config = bigquery.QueryJobConfig(destination=view_id)
    job = client.query(metadata['revised_sql'], job_config=job_config)
    # Wait for the query to complete
    result = job.result()

    installed_table = client.get_table(view_id)

    # Update the schema after creating the view
    installed_table.schema = metadata['schema']
    client.update_table(installed_table,['schema'])

    progresslogger.info(f'Created table dicom_all')
    return


def create_dicom_all_view_from_table(client, view_id, table, metadata):
    new_view = bigquery.Table(view_id + '_view')
    new_view.view_query = metadata['revised_sql']
    new_view.friendly_name = metadata['friendly_name']
    new_view.description = metadata['description']
    new_view.labels = metadata['labels']
    installed_view = client.create_table(new_view)

    # Update the schema after creating the view
    installed_view.schema = metadata['schema']
    client.update_table(installed_view,['schema'])

    progresslogger.info(f'Created view dicom_all_view')

    return


def revise_dicom_all(args):
    client = bigquery.Client()
    view_id = f'{args.project}.{args.dataset}.dicom_all'
    view = client.get_table(view_id)

    metadata = {}
    metadata['schema'] = revise_dicom_all_schema(view)
    metadata['revised_sql'] = revise_dicom_all_sql(args, view_id, view)
    metadata['friendly_name'] = view.friendly_name
    metadata['description'] = view.description
    metadata['labels'] = view.labels
    # Now create table/view pair as needed
    if view.table_type == 'TABLE':
        # The table already exists; Check whether the view also exists
        view_view_id = f'{args.project}.{args.dataset}.{view_id}_view'
        try:
            client.get_table(view_view_id)
            # Both the table and view exist, so we are done
            return
        except NotFound:
            # The corresponding view does not exist
            create_dicom_all_view_from_table(client, view_id, view, metadata)
    else:
        # The view already exists; create the table
        create_dicom_all_table_from_view(client, view_id, view, metadata)
        # When we created the table, we deleted the view to reuse its name. Create it again.
        create_dicom_all_view_from_table(client, view_id, view, metadata)
    return


def revise_tables(args):
    # revise_dicom_all(args)
    clone_derived(args, 'dicom_all')
    clone_derived(args, 'measurement_groups')
    clone_derived(args, 'qualitative_measurements')
    clone_derived(args, 'quantitative_measurements')
    clone_derived(args, 'segmentations')
    clone_derived(args, 'dicom_metadata_curated')
    clone_derived(args, 'dicom_metadata_curated_series_level')

if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()

    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version number')
    parser.add_argument('--project', default="idc-dev-etl", help='Project in which tables live')
    # parser.add_argument('--dataset', default=f"idc_v{settings.CURRENT_VERSION}_pub", help="BQ dataset")
    parser.add_argument('--dataset', default=f"whc_dev_idc_v12_pub", help="BQ dataset")
    parser.add_argument('--uuid_url_map', default="idc-dev-etl.idc_v14_dev.uuid_url_map",
                        help="Table that maps instance uuids to URLS")
    parser.add_argument('--dev_or_pub', default='pub', help='Revising the dev or pub version of auxiliary_metadata')
    parser.add_argument('--dataset_prefix', default='whc_dev_')
    args = parser.parse_args()

    print(f'args: {json.dumps(args.__dict__, indent=2)}')

    revise_tables(args)
