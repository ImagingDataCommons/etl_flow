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
Delete all views:
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


def copy_table(client, args,  src_dataset, table):

    src_table_id = f'idc-dev-etl.{args.src_dataset}.{table.table_id}'
    dst_table_id = f'idc-dev-etl.{args.dataset_prefix}{args.src_dataset}.{table.table_id}'

    job = client.copy_table(src_table_id, dst_table_id)
    while job.result().state != 'DONE':
        time.wait(1)
    progresslogger.info(f'Copy of table {table.table_id}: {job.result().state} ')

    dst_table = client.get_table(dst_table_id)
    pass


def add_missing_fields_to(trg_schema, src_schema):
    for i, src_field in enumerate(src_schema):
        if next((j for j, trg_field in enumerate(trg_schema) if src_field.name == trg_field.name), -1) == -1:
            trg_schema.insert(i, src_schema[i])

    for i, src_field in enumerate(src_schema):
        if next((j for j, trg_field in enumerate(trg_schema) if src_field.name == trg_field.name), -1) == -1:
            errlogger.error(f'{src_schema[i]} not found in dst_schema')


def copy_view(client, args, src_dataset, src_view):

    view = client.get_table(f'{src_view.project}.{src_view.dataset_id}.{src_view.table_id}')

    new_view = bigquery.Table(f'{view.project}.{args.dataset_prefix}{args.src_dataset}.{view.table_id}')
    new_view.view_query = view.view_query
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

    progresslogger.info(f'Copy of view {src_view.table_id}: DONE')

    pass


def clone_dataset(args):
    client = bigquery.Client()
    dataset_ref = bigquery.DatasetReference('idc-dev-etl', args.src_dataset)
    src_dataset = client.get_dataset(dataset_ref)

    if bq_dataset_exists(client, 'idc-dev-etl', f'{args.trg_dataset}'):
        delete_all_views(client, 'idc-dev-etl',f'{args.trg_dataset}')
    else:
        dataset_dict = dict(
            description = src_dataset.description,
            labels = src_dataset.labels
        )
        create_dataset(client, 'idc-dev-etl',f'{args.trg_dataset}', dataset_dict)

    tables = [ table for table in client.list_tables(f'idc-dev-etl.{args.src_dataset}')]
    for table in tables:
        if table.table_id in [
            'auxiliary_metadata',
            'dicom_metadata',
            'original_collections_metadata',
            'dicom_all', 'dicom_all_view',
            'dicom_metadata_curated', 'dicom_metadata_curated_view',
            'dicom_metadata_curated_series_level', 'dicom_metadata_curated_series_level_view',
            'measurement_groups', 'measurement_groups_view',
            'qualitative_measurements', 'qualitative_measurements_view',
            'quantitative_measurements', 'quantitative_measurements_view',
            'segmentations', 'segmentations_view',
            'dicom_derived_all', f'dicom_pivot_v{args.dataset_version}'
            ]:
            if table.table_type == 'TABLE':
                copy_table(client, args, src_dataset, table)
            else:
                copy_view(client, args, src_dataset, table)


if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()

    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version number')
    parser.add_argument('--project', default="idc-dev-etl", help='Project in which tables live')
    # parser.add_argument('--dataset', default=f"idc_v{settings.CURRENT_VERSION}_pub", help="BQ dataset")
    parser.add_argument('--src_dataset', default=f"idc_v5", help="BQ dataset")
    parser.add_argument('--dataset_prefix', default='whc_dev_')
    parser.add_argument('--trg-version', default='', help='Dataset version to be cloned')
    args = parser.parse_args()

    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')


    for version in (
            'idc_v1',
            'idc_v5',
            'idc_v12_pub',
            'idc_v13_pub'
    ):
        args.src_dataset = version
        clone_dataset(args)
