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
We have nested schema structure, so need to make recursive creation function
'''

# def make_schema_list(field_list):
#     full_list = []
#     for sf in field_list:
#         if sf["type"] == "RECORD":
#             # if sf["name"] == "ContextGroupIdentificationSequence":
#             #     print("Here")
#             field_list = make_schema_list(sf["fields"])
#             if not "description" in sf:
#                 sf["description"] = "TBD"
#             next_field = bigquery.SchemaField(sf["name"], sf["type"], sf["mode"], sf["description"], field_list)
#         else:
#             try:
#                 if not "description" in sf:
#                     sf["description"] = "TBD"
#                 next_field = bigquery.SchemaField(sf["name"], sf["type"], sf["mode"], sf["description"])
#             except KeyError as e:
#                 print("KeyError: {}".format(e))
#         full_list.append(next_field)
#     return full_list


'''
----------------------------------------------------------------------------------------------
Annotate the schema that was generated when a view was created with annotations from a 
supplied (probably partial) schema.
'''

# def annotate_schema(generated_schema, annotation_schema):
#     a=1
#     for src_field in annotation_schema:
#         # Only annotate in the case that the source description is not empty
#         if src_field.description and src_field.description != "TBD":
#             # Search for a field with matching name
#             gen_field = next((trg_field for trg_field in generated_schema if src_field.name == trg_field.name ), None)
#             if gen_field:
#                 new_gen_field = bigquery.SchemaField(
#                         gen_field.name,
#                         gen_field.field_type,
#                         description = src_field.description,
#                         fields = gen_field.fields if src_field.field_type != "RECORD" else \
#                             annotate_schema(list(gen_field.fields), list(src_field.fields)),
#                         mode = gen_field.mode,
#                         policy_tags = gen_field.policy_tags
#                     )
#                 # if src_field.field_type == "RECORD":
#                 #     annotate_schema(list(new_gen_field.fields), list(src_field.fields))
#                 generated_schema[generated_schema.index(gen_field)] = new_gen_field
#             else:
#                 print("Failed to find target analog of source field {}".format(src_field.name))
#     return generated_schema


# def create_table(target_client, target_project, target_dataset, table_name, view_schema, view_sql):
#
#     table_id = '{}.{}.{}'.format(target_project, target_dataset, table_name)
#
#     print("Deleting {}".format(table_id))
#     try:
#         target_client.delete_table(table_id)
#     except NotFound:
#         print("View not found")
#     print("Creating {}".format(table_id))
#
#     #
#     # Convert the dictionary into the tree of SchemaField objects:
#     #
#
#     targ_schema = make_schema_list(view_schema["schema"]["fields"])
#
#     #
#     # Not supposed to submit a schema for a view, so we don't. But we need to update it later to get the
#     # descriptions brought across:
#     #
#
#     targ_table = bigquery.Table(table_id)
#     targ_table.friendly_name = view_schema["friendlyName"]
#     targ_table.description = view_schema["description"]
#     targ_table.labels = view_schema["labels"]
#
#
#     # #
#     # # The way a table turns into a view is by setting the view_query property:
#     # #
#     # targ_table.view_query = view_sql
#
#     # Create the view
#     empty_targ_table = target_client.create_table(targ_table)
#
#     # Populate the table
#     table_id = "{}.{}.{}".format(target_project, target_dataset, table_name)
#     job_config = bigquery.QueryJobConfig(destination=table_id)
#     job = target_client.query(view_sql, job_config=job_config)
#     # Wait for the query to complete
#     result = job.result()
#
#     installed_targ_table = target_client.get_table(table_id)
#
#     # Convert the schema that was generated when we created the table into a tree of SchemaField objects
#     generated_schema = installed_targ_table.schema
#
#     # Add annotations to the generated view
#     annotated_schema = annotate_schema(generated_schema, targ_schema)
#
#     #
#     # If we created a view, update the schema after creation:
#     #
#
#     installed_targ_table.schema = annotated_schema
#
#     target_client.update_table(installed_targ_table, fields=["schema"])
#
#     return True


def revise_dicom_all(args):
    client = bigquery.Client()
    view_id = f'{args.project}.{args.dataset}.dicom_all'
    view = client.get_table(view_id)

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

    new_view = bigquery.Table(view_id + '_view')
    new_view.view_query = revised_sql
    new_view.friendly_name = view.friendly_name
    new_view.description = view.description
    new_view.labels = view.labels
    installed_view = client.create_table(new_view)

    # Update the schema after creating the view
    installed_view.schema = schema
    client.update_table(installed_view,['schema'])

    # Now create the corresponding table

    # First delete the original view. We will use its name for the table
    client.delete_table(view)
    new_table = bigquery.Table(view_id)
    # new_table.view_query = revised_sql
    new_table.friendly_name = view.friendly_name
    new_table.description = view.description
    new_table.labels = view.labels
    client.create_table(new_table)

    job_config = bigquery.QueryJobConfig(destination=view_id)
    job = client.query(revised_sql, job_config=job_config)
    # Wait for the query to complete
    result = job.result()

    installed_table = client.get_table(view_id)

    # Update the schema after creating the view
    installed_table.schema = schema
    client.update_table(installed_table,['schema'])

    return

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
        print("Deleting {}".format(table_id))
        target_client.delete_table(table_id)

    return True


def copy_table(client, args,  src_dataset, table):

    src_table_id = f'idc-dev-etl.{args.src_dataset}.{table.table_id}'
    dst_table_id = f'idc-dev-etl.{args.dataset_prefix}{args.src_dataset}.{table.table_id}'

    job = client.copy_table(src_table_id, dst_table_id)
    while job.result().state != 'DONE':
        wait(1)
    print(f'Copy of table {table.table_id}: {job.result().state} ')

    dst_table = client.get_table(dst_table_id)
    pass

def copy_view(client, args, src_dataset, table):

    view = client.get_table(f'{table.project}.{table.dataset_id}.{table.table_id}')

    new_view = bigquery.Table(f'{view.project}.{args.dataset_prefix}{args.src_dataset}.{view.table_id}')
    new_view.view_query = view.view_query
    new_view.friendly_name = view.friendly_name
    new_view.description = view.description
    new_view.labels = view.labels
    installed_view = client.create_table(new_view)

    # Update the schema after creating the view
    installed_view.schema = view.schema
    client.update_table(installed_view,['schema'])

    print(f'Copy of view {table.table_id}: DONE')

    pass


def clone_dataset(args):
    client = bigquery.Client()
    dataset_ref = bigquery.DatasetReference('idc-dev-etl', args.src_dataset)
    src_dataset = client.get_dataset(dataset_ref)

    if bq_dataset_exists(client, 'idc-dev-etl', f'{args.dataset_prefix}{args.src_dataset}'):
        delete_all_views(client, 'idc-dev-etl',f'{args.dataset_prefix}{args.src_dataset}')
    else:
        dataset_dict = dict(
            description = src_dataset.description,
            labels = src_dataset.labels
        )
        create_dataset(client, 'idc-dev-etl',f'{args.dataset_prefix}{args.src_dataset}', dataset_dict)

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
            'segmentations', 'segmentations_view'
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
    args = parser.parse_args()

    print(f'args: {json.dumps(args.__dict__, indent=2)}')


    for version in ('idc_v1', 'idc_v5', 'idc_v12_pub', 'idc_v13_pub'):
        args.src_dataset = version
        clone_dataset(args)
