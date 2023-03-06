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

# Duplicate a BQ dataset, including views.
import settings
import argparse
import json
import time
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import NotFound
from utilities.logging_config import successlogger, progresslogger, errlogger

'''
----------------------------------------------------------------------------------------------
Create the target dataset:
'''

def create_dataset(target_client, target_project_id, dataset_id, dataset_dict):

    full_dataset_id = "{}.{}".format(target_project_id, dataset_id)
    install_dataset = bigquery.Dataset(full_dataset_id)

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
    # dataset_ref = target_client.dataset(target_dataset)
    try:
        src_dataset = client.get_dataset(dataset_ref)
        # target_client.get_dataset(dataset_ref)
        return True
    except NotFound:
        return False

'''
----------------------------------------------------------------------------------------------
Delete all views in exisiting dataset:
'''

def delete_all_views(target_client, target_project, target_dataset):

    dataset_ref = bigquery.DatasetReference(target_project, target_dataset)
    # dataset_ref = target_client.dataset(target_dataset)
    dataset = target_client.get_dataset(dataset_ref)

    # table_list = list(target_client.list_tables(dataset.dataset_id))
    table_list = list(target_client.list_tables(f'{dataset.project}.{dataset.dataset_id}'))
    for tbl in table_list:
        table_id = '{}.{}.{}'.format(target_project, dataset.dataset_id, tbl.table_id)
        progresslogger.info("Deleting {}".format(table_id))
        target_client.delete_table(table_id)

    return True


def copy_table(client, args,  table_id):

    src_table_id = f'{args.src_project}.{args.src_dataset}.{table_id}'
    dst_table_id = f'{args.trg_project}.{args.trg_dataset}.{table_id}'

    job = client.copy_table(src_table_id, dst_table_id)
    while job.result().state != 'DONE':
        time.wait(1)
    progresslogger.info(f'Copy of table {table_id}: {job.result().state} ')

    dst_table = client.get_table(dst_table_id)
    pass


def add_missing_fields_to(trg_schema, src_schema):
    for i, src_field in enumerate(src_schema):
        if next((j for j, trg_field in enumerate(trg_schema) if src_field.name == trg_field.name), -1) == -1:
            trg_schema.insert(i, src_schema[i])

    for i, src_field in enumerate(src_schema):
        if next((j for j, trg_field in enumerate(trg_schema) if src_field.name == trg_field.name), -1) == -1:
            errlogger.error(f'{src_schema[i]} not found in dst_schema')


def copy_view(client, args, view_id):

    view = client.get_table(f'{args.src_project}.{args.src_dataset}.{view_id}')

    new_view = bigquery.Table(f'{args.trg_project}.{args.trg_dataset}.{view_id}')
    new_view.view_query = view.view_query.replace(args.src_dataset,args.trg_dataset) \
        .replace(args.src_project,args.trg_project)
    new_view.friendly_name = view.friendly_name
    new_view.description = view.description
    new_view.labels = view.labels
    installed_view = client.create_table(new_view)

    # For whatever reason, in at least one case, a field in the installed_view
    # schema is missing from the src_view schema. We add those missing fields.
    installed_view.schema = add_missing_fields_to(view.schema, installed_view.schema)

    # # Update the schema after creating the view
    # installed_view.schema = view.schema
    client.update_table(installed_view,['schema'])

    progresslogger.info(f'Copy of view {view_id}: DONE')

    pass


def clone_dataset(args):
    client = bigquery.Client()
    # client = bigquery.Client(project=args.trg_project)
    src_dataset_ref = bigquery.DatasetReference(args.src_project, args.src_dataset)
    src_dataset = client.get_dataset(src_dataset_ref)

    if bq_dataset_exists(client, args.trg_project, args.trg_dataset):
        delete_all_views(client, args.trg_project, args.trg_dataset)
    else:
        dataset_dict = dict(
            description = src_dataset.description,
            labels = src_dataset.labels
        )
        create_dataset(client, args.trg_project, args.trg_dataset, dataset_dict)

    table_ids = {table.table_id: table.table_type for table in client.list_tables(f'{args.src_project}.{args.src_dataset}')}
    for table_id in table_ids:
        if table_ids[table_id] == 'TABLE':
            copy_table(client, args, table_id)
        else:
            copy_view(client, args, table_id)


if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()

    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version number')
    parser.add_argument('--src_project', default="idc-dev-etl", help='Project from which tables are copied')
    parser.add_argument('--trg_project', default="idc-source-data", help='Project to which tables are copied')
    parser.add_argument('--trg_dataset_prefix', default=f"dev_", help="BQ target dataset")
    parser.add_argument('--dataset_prefix', default='idc-dev-etl_', help='Prefix added to source dataset name')
    parser.add_argument('--trg-version', default='', help='Dataset version to be cloned')
    args = parser.parse_args()

    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    for src_dataset in (
            'idc_v1',
            'idc_v2',
            'idc_v3',
            'idc_v4',
            'idc_v5',
            'idc_v6',
            'idc_v7',
            'idc_v8',
            'idc_v9_pub',
            'idc_v10_pub',
            'idc_v11_pub',
            'idc_v12_pub',
            'idc_v13_pub'
    ):
        args.src_dataset = src_dataset
        args.trg_dataset = args.dataset_prefix + src_dataset
        clone_dataset(args)
