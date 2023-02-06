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

# Recreate a view which we deleted in order to use its
# name for a new table.
def recreate_view(client, args, table, metadata):
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

# We have a view. Use the SQL and schema to create a table.
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

# We have a table but not a view. In thise case
# we have get the SQL and schema from the files
# used to create the table.
def create_view(client, args, table):
    with open(f'../derived_table_creation/BQ_Table_Building/derived_data_views/schema/{table.table_id}.json') as f:
        schema = json.load(f)
    with open(f'../derived_table_creation/BQ_Table_Building/derived_data_views/sql/{table.table_id}.sql') as f:
        sql = f'{f.read()}'.format(project=args.project, dataset=args.dataset)

    # For this step, we need to remove aws_url from the schema and sql.
    # They will be added later
    sql = sql.replace('    aux.aws_url as aws_url,\n', '')
    schema['schema']['fields'].pop(32)

    view_id = f'{table.project}.{args.dataset}.{table.table_id}_view'
    new_view = bigquery.Table(view_id)
    new_view.view_query = sql
    new_view.friendly_name = schema['friendlyName']
    new_view.description = schema['description']
    new_view.labels = schema['labels']
    installed_view = client.create_table(new_view)

    # Update the schema after creating the view
    installed_view.schema = schema['schema']['fields']
    client.update_table(installed_view,['schema'])

    progresslogger.info(f'Created view {view_id}_view')
    return



def clone_derived(args, table_id):
    client = bigquery.Client()
    table_id = f'{args.project}.{args.dataset}.{table_id}'
    try:
        table = client.get_table(table_id)
    except NotFound:
        # The table doesn't exist in this dataset
        return

    if table.table_type == 'TABLE':
        # The table already exists; Check whether the view also exists
        view_id = f'{table_id}_view'
        try:
            table = client.get_table(view_id)
            # Both the table and view exist, so we are done
            return
        except NotFound:
            # The corresponding view does not exist
            create_view(client, args, table)
    else:
        metadata = {}
        metadata['schema'] = table.schema
        metadata['view_query'] = table.view_query
        metadata['friendly_name'] = table.friendly_name
        metadata['description'] = table.description
        metadata['labels'] = table.labels
        # The view already exists; create the table
        create_table_from_view(client, args, table, metadata)
        # We deleted the view to reuse its name. Recreate it.
        recreate_view(client, args, table, metadata)

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
    parser.add_argument('--dataset', default=f"whc_dev_idc_v12_pub", help="BQ dataset")
    parser.add_argument('--uuid_url_map', default="idc-dev-etl.idc_v14_dev.uuid_url_map",
                        help="Table that maps instance uuids to URLS")
    parser.add_argument('--dev_or_pub', default='pub', help='Revising the dev or pub version of auxiliary_metadata')
    parser.add_argument('--dataset_prefix', default='whc_dev_')
    args = parser.parse_args()

    print(f'args: {json.dumps(args.__dict__, indent=2)}')

    revise_tables(args)
