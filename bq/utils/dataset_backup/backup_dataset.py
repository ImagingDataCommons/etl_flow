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

# Backup a dataset
# Tables in the dataset are snapshotted.
# Views in the dataset are copied.
import settings
import argparse
import json
import time
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import NotFound, BadRequest
from utilities.logging_config import successlogger, progresslogger, errlogger

'''
----------------------------------------------------------------------------------------------
Create the target dataset:
'''

def create_dataset(target_client, target_project_id, dataset_id, dataset_dict):

    full_dataset_id = "{}.{}".format(target_project_id, dataset_id)
    install_dataset = bigquery.Dataset(full_dataset_id)
    a=bigquery.CopyJobConfig

    install_dataset.location = "US"
    install_dataset.description = dataset_dict["description"]
    install_dataset.labels = dataset_dict["labels"]

    target_client.create_dataset(install_dataset)

    return True

'''
----------------------------------------------------------------------------------------------
Check if dataset exists:
'''

def bq_dataset_exists(client, project , target_dataset):

    dataset_ref = bigquery.DatasetReference(project, target_dataset)
    try:
        src_dataset = client.get_dataset(dataset_ref)
        return True
    except NotFound:
        return False


def snapshot_table(client, args,  table_id):
    try:
        table = client.get_table(f'{args.trg_project}.{args.trg_dataset}.{table_id}')
        progresslogger.info(f'Table {table} already exists.')
    except:
        src_table_id = f'{args.src_project}.{args.src_dataset}.{table_id}'
        trg_table_id = f'{args.trg_project}.{args.trg_dataset}.{table_id}'

        # Construct a BigQuery client object.
        client = bigquery.Client()
        job_config = bigquery.CopyJobConfig()
        job_config.operation_type = 'SNAPSHOT'
        job_config.write_disposition = 'WRITE_EMPTY'

        # Construct and run a copy job.
        job = client.copy_table(
            src_table_id,
            trg_table_id,
            # Must match the source and destination tables location.
            location="US",
            job_config=job_config,
        )  # Make an API request.
        job.result()  # Wait for the job to complete.
        progresslogger.info("Backed up table {} to {}".format(src_table_id, trg_table_id)
        )

def add_descriptions_to_schema(src_schema, trg_schema):
    for i, src_field in enumerate(src_schema):
        if src_field.description and  ((j:=(next((j for j, trg_field in enumerate(trg_schema) if src_field.name == trg_field.name), -1))) != -1):
            new_schemaField_dict = trg_schema[j].to_api_repr()
            new_schemaField_dict['description'] = src_schema[i].description
            new_schemaField = bigquery.SchemaField.from_api_repr(new_schemaField_dict)
            trg_schema[j] = new_schemaField
    return trg_schema


def copy_view(client, args, view_id):

    try:
        view = client.get_table(f'{args.trg_project}.{args.trg_dataset}.{view_id}')
        progresslogger.info(f'View {view} already exists.')
    except:
        view = client.get_table(f'{args.src_project}.{args.src_dataset}.{view_id}')

        new_view = bigquery.Table(f'{args.trg_project}.{args.trg_dataset}.{view_id}')
        new_view.view_query = view.view_query
        new_view.friendly_name = view.friendly_name
        new_view.description = view.description
        new_view.labels = view.labels
        installed_view = client.create_table(new_view)
        try:
            # # Update the schema after creating the view
            installed_view.schema = view.schema
            client.update_table(installed_view, ['schema'])
            progresslogger.info(f'Copied view {view_id}: DONE')
        except BadRequest as exc:
            errlogger.error(f'{exc}')
    return

def backup_dataset(args):
    client = bigquery.Client()
    src_dataset_ref = bigquery.DatasetReference(args.src_project, args.src_dataset)
    src_dataset = client.get_dataset(src_dataset_ref)

    # Create the target dataset if it doesn't exist
    if not bq_dataset_exists(client, args.trg_project, args.trg_dataset):
        dataset_dict = dict(
            description = src_dataset.description,
            labels = src_dataset.labels
        )
        create_dataset(client, args.trg_project, args.trg_dataset, dataset_dict)

    progresslogger.info(f'Backing up {args.src_dataset} to {args.trg_dataset}')
    table_ids = {table.table_id: table.table_type for table in client.list_tables(f'{args.src_project}.{args.src_dataset}')}
    # Create tables first
    for table_id in table_ids:
        if table_ids[table_id] == 'TABLE':
            snapshot_table(client, args, table_id)
    for table_id in table_ids:
        if table_ids[table_id] == 'VIEW':
            copy_view(client, args, table_id)
