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
from google.api_core.exceptions import NotFound, BadRequest
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

def delete_views_or_tables(target_client, target_project, target_dataset, table_type):

    dataset_ref = bigquery.DatasetReference(target_project, target_dataset)
    # dataset_ref = target_client.dataset(target_dataset)
    dataset = target_client.get_dataset(dataset_ref)

    # table_list = list(target_client.list_tables(dataset.dataset_id))
    table_list = list(target_client.list_tables(f'{dataset.project}.{dataset.dataset_id}'))
    for tbl in table_list:
        if tbl.table_type == table_type:
            table_id = '{}.{}.{}'.format(target_project, dataset.dataset_id, tbl.table_id)
            progresslogger.info("Deleting {}".format(table_id))
            target_client.delete_table(table_id)
    return


def copy_table(client, args,  table_id):

    src_table_id = f'{args.src_project}.{args.src_dataset}.{table_id}'
    recovered_table_id = f'{args.trg_project}.{args.trg_dataset}.{table_id}'

    # Construct a BigQuery client object.
    client = bigquery.Client()
    job_config = bigquery.CopyJobConfig()
    job_config.write_disposition = 'WRITE_TRUNCATE'

    # Construct the restore-from table ID using a snapshot decorator.
    snapshot_table_id = "{}@{}".format(src_table_id, snapshot_epoch)

    # Construct and run a copy job.
    job = client.copy_table(
        snapshot_table_id,
        recovered_table_id,
        # Must match the source and destination tables location.
        location="US",
        job_config=job_config,
    )  # Make an API request.

    job.result()  # Wait for the job to complete.

    progresslogger.info("Copied data from deleted table {} to {}".format(table_id, recovered_table_id)
    )




def add_descriptions_to_schema(src_schema, trg_schema):
    for i, src_field in enumerate(src_schema):
        if src_field.description and  ((j:=(next((j for j, trg_field in enumerate(trg_schema) if src_field.name == trg_field.name), -1))) != -1):
            new_schemaField_dict = trg_schema[j].to_api_repr()
            new_schemaField_dict['description'] = src_schema[i].description
            new_schemaField = bigquery.SchemaField.from_api_repr(new_schemaField_dict)
            trg_schema[j] = new_schemaField
    return trg_schema


def clone_dataset(args):
    client = bigquery.Client()
    # client = bigquery.Client(project=args.trg_project)
    src_dataset_ref = bigquery.DatasetReference(args.src_project, args.src_dataset)
    src_dataset = client.get_dataset(src_dataset_ref)

    # if bq_dataset_exists(client, args.trg_project, args.trg_dataset):
    #     delete_views_or_tables(client, args.trg_project, args.trg_dataset, 'TABLE')
    #     delete_views_or_tables(client, args.trg_project, args.trg_dataset, 'VIEW')
    # else:
    #     dataset_dict = dict(
    #         description = src_dataset.description,
    #         labels = src_dataset.labels
    #     )
    #     create_dataset(client, args.trg_project, args.trg_dataset, dataset_dict)

    if not bq_dataset_exists(client, args.trg_project, args.trg_dataset):
        dataset_dict = dict(
            description = src_dataset.description,
            labels = src_dataset.labels
        )
        create_dataset(client, args.trg_project, args.trg_dataset, dataset_dict)


    progresslogger.info(f'Cloning {args.src_dataset} to {args.trg_dataset}')
    # table_ids = {table.table_id: table.table_type for table in client.list_tables(f'{args.src_project}.{args.src_dataset}')}
    table_ids = {table.table_id: table.table_type for table in client.list_tables(f'{args.table_id_project}.{args.table_id_dataset_prefix+args.src_dataset}')}
    # Create tables first
    for table_id in table_ids:
        if table_ids[table_id] == 'TABLE':
            copy_table(client, args, table_id)


# if __name__ == '__main__':
#     # (sys.argv)
#     parser = argparse.ArgumentParser()
#     parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version number')
#     parser.add_argument('--src_project', default="idc-dev-etl", help='Project from which tables are copied')
#     parser.add_argument('--trg_project', default="idc-source-data", help='Project to which tables are copied')
#     parser.add_argument('--trg_dataset_prefix', default=f"idc_dev_etl_", help="BQ target dataset")
#     args = parser.parse_args()
#
#     progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')
#
#     for src_dataset in (
#             # 'idc_v1',
#             # 'idc_v2',
#             # 'idc_v3',
#             'idc_v4',
#             'idc_v5',
#             'idc_v6',
#             'idc_v7',
#             'idc_v8_pub',
#             'idc_v9_pub',
#             'idc_v10_pub',
#             'idc_v11_pub',
#             'idc_v12_pub',
#             'idc_v13_pub'
#     ):
#         args.src_dataset = src_dataset
#         args.trg_dataset = args.trg_dataset_prefix + src_dataset
#         clone_dataset(args)
