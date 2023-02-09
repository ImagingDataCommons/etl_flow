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
from bq.derived_table_creation.BQ_Table_Building.publish_bq_derived_tables import annotate_schema, make_schema_list

# Recreate a view which we deleted in order to use its
# name for a new table.
def recreate_view(client, args, view_id, table_id):
    # view_id = f'{table.project}.{args.dataset}.{table.table_id}_view'
    old_view = client.get_table(table_id)
    new_view = bigquery.Table(view_id)
    new_view.view_query = old_view.view_query.replace(args.src_dataset,args.trg_dataset)
    new_view.friendly_name = old_view.friendly_name
    new_view.description = old_view.description
    new_view.labels = old_view.labels
    schema = old_view.schema
    # Make sure the view does not already exist
    client.delete_table(view_id, not_found_ok=True)
    installed_view = client.create_table(new_view)

    # Update the schema after creating the view
    installed_view.schema = schema
    client.update_table(installed_view,['schema'])

    progresslogger.info(f'Created view {view_id}')

    return

# We have a view. Use the SQL and schema to create a table.
def create_table_from_view(client, args, view_id, table_id):
    # # We first create a view named like <table_id)_view
    # create_view_from_table(client, args, table, )
    view = client.get_table(view_id)
    new_table = bigquery.Table(table_id)
    # new_table.view_query = revised_sql
    new_table.friendly_name = view.friendly_name
    new_table.description = view.description
    new_table.labels = view.labels
    view_query = view.view_query
    schema = view.schema
    # Ensure that the table does not already exist
    client.delete_table(table_id, not_found_ok=True)
    client.create_table(new_table)

    job_config = bigquery.QueryJobConfig(destination=table_id)
    job = client.query(view_query, job_config=job_config)
    # Wait for the query to complete
    result = job.result()

    installed_table = client.get_table(table_id)

    # Update the schema after creating the view
    installed_table.schema = schema
    client.update_table(installed_table,['schema'])

    progresslogger.info(f'Created table {table_id}')
    return

# We have a table but no view from which to get the SQL/Schema. In this case
# we have get the SQL and schema from the files
# used to create the table.
def recreate_table(client, args, table_id):
    old_table = client.get_table(table_id)
    with open(f'../derived_table_creation/BQ_Table_Building/derived_data_views/schema/{old_table.table_id}.json') as f:
        schema = json.load(f)
    with open(f'../derived_table_creation/BQ_Table_Building/derived_data_views/sql/{old_table.table_id}.sql') as f:
        sql = f'{f.read()}'.format(project=args.project, dataset=args.trg_dataset)

    # # For this step, if generating dicom_all_view, we need to remove aws_url from the schema and sql.
    # # They will be added later
    # if table.table_id == 'dicom_all':
    #     sql = sql.replace('    aux.aws_url as aws_url,\n', '')
    #     schema['schema']['fields'].pop(32)

    new_table = bigquery.Table(table_id)
    new_table.view_query = sql
    new_table.friendly_name = schema['friendlyName']
    new_table.description = schema['description']
    new_table.labels = schema['labels']
    client.delete_table(table_id, not_found_ok=True)
    installed_view = client.create_table(new_table)

    # Convert the dictionary into the tree of SchemaField objects:
    targ_schema = make_schema_list(schema["schema"]["fields"])
    # Convert the schema that was generated when we created the view into a tree of SchemaField objects
    generated_schema = installed_view.schema
    # Add annotations to the generated view
    annotated_schema = annotate_schema(generated_schema, targ_schema)
    # Update the schema after creation:
    installed_view.schema = annotated_schema

    # Update the schema after creating the view
    # installed_view.schema = schema['schema']['fields']
    client.update_table(installed_view,['schema'])

    progresslogger.info(f'Created table {table_id}')
    return


# We have a table but not a view. In this case
# we have get the SQL and schema from the files
# used to create the table.
def create_view(client, args, view_id, table_id):
    with open(f'../derived_table_creation/BQ_Table_Building/derived_data_views/schema/{table_id.split(".")[-1]}.json') as f:
        schema = json.load(f)
    with open(f'../derived_table_creation/BQ_Table_Building/derived_data_views/sql/{table_id.split(".")[-1]}.sql') as f:
        sql = f'{f.read()}'.format(project=args.project, dataset=args.trg_dataset)

    # For this step, if generating dicom_all_view, we need to remove aws_url from the schema and sql.
    # They will be added later
    if table_id.split('.')[-1] == 'dicom_all':
        sql = sql.replace('    aux.aws_url as aws_url,\n', '')
        schema['schema']['fields'].pop(32)

    new_view = bigquery.Table(view_id)
    new_view.view_query = sql
    new_view.friendly_name = schema['friendlyName']
    new_view.description = schema['description']
    new_view.labels = schema['labels']

    # Delete the existing view in case it exists
    client.delete_table(view_id, not_found_ok=True)
    installed_view = client.create_table(new_view, exists_ok=True)

    # Convert the dictionary into the tree of SchemaField objects:
    targ_schema = make_schema_list(schema["schema"]["fields"])
    # Convert the schema that was generated when we created the view into a tree of SchemaField objects
    generated_schema = installed_view.schema
    # Add annotations to the generated view
    annotated_schema = annotate_schema(generated_schema, targ_schema)
    # Update the schema after creation:
    installed_view.schema = annotated_schema

    # Update the schema after creating the view
    # installed_view.schema = schema['schema']['fields']
    client.update_table(installed_view,['schema'])

    progresslogger.info(f'Created view {view_id}')
    return


def clone_derived(args, table_id):
    client = bigquery.Client()
    table_id = f'{args.project}.{args.trg_dataset}.{table_id}'
    view_id = f'{table_id}_view'
    try:
        table = client.get_table(table_id)
    except NotFound:
        # The table doesn't exist in this dataset
        # This is an error
        progresslogger.info(f'Table {table_id} does not exist')
        return

    if table.table_type == 'VIEW':
        # Change view name; add _view
        recreate_view(client, args, view_id, table_id)
        # We'll regenerate dicom_all later when we add reload aws and gcs urls
        if table_id.split('.')[-1] != 'dicom_all':
            create_table_from_view(client, args, view_id, table_id)
    else:
        # A table by this name exists.
        # we only need to create the view
        create_view(client, args, view_id, table_id)
    return


def revise_derived_tables(args):
    # revise_dicom_all(args)
    clone_derived(args, 'measurement_groups')
    clone_derived(args, 'qualitative_measurements')
    clone_derived(args, 'quantitative_measurements')
    clone_derived(args, 'segmentations')
    clone_derived(args, 'dicom_metadata_curated')
    clone_derived(args, 'dicom_metadata_curated_series_level')
    clone_derived(args, 'dicom_all')

if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()

    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version number')
    parser.add_argument('--project', default="idc-dev-etl", help='Project in which tables live')
    parser.add_argument('--src_dataset', default=f"idc_v1", help="BQ source dataset")
    # parser.add_argument('--trg_dataset', default=f"whc_dev_idc_v13_pub", help="BQ target dataset")
    parser.add_argument('--dataset_prefix', default='whc_dev_')
    args = parser.parse_args()

    args.trg_dataset = f'{args.dataset_prefix}{args.src_prefix}'

    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    revise_derived_tables(args)
