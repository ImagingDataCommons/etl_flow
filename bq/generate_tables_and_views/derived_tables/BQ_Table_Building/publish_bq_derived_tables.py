"""

Copyright 2019-2020, Institute for Systems Biology

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""


"""
This script originally created views. To reduce execution time complexity,
it now creates tables. The create_views() routine is retained in case
it is needed for some reason.
"""


import sys
import yaml
import io
from json import loads as json_loads

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import NotFound

'''
----------------------------------------------------------------------------------------------
The configuration reader. Parses the YAML configuration into dictionaries
'''
def load_config(yaml_config):
    yaml_dict = None
    config_stream = io.StringIO(yaml_config)
    try:
        yaml_dict = yaml.load(config_stream, Loader=yaml.FullLoader)
    except yaml.YAMLError as ex:
        print(ex)

    if yaml_dict is None:
        return None, None

    return yaml_dict['files_and_buckets_and_tables'], yaml_dict['steps']

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

def bq_dataset_exists(target_client, target_dataset):

    dataset_ref = target_client.dataset(target_dataset)
    try:
        target_client.get_dataset(dataset_ref)
        return True
    except NotFound:
        return False

'''
----------------------------------------------------------------------------------------------
Delete all views:
'''

def delete_all_views(target_client, target_project, target_dataset):

    dataset_ref = target_client.dataset(target_dataset)
    dataset = target_client.get_dataset(dataset_ref)

    table_list = list(target_client.list_tables(dataset.dataset_id))
    for tbl in table_list:
        table_id = '{}.{}.{}'.format(target_project, dataset.dataset_id, tbl.table_id)
        print("Deleting {}".format(table_id))
        target_client.delete_table(table_id)

    return True

'''
----------------------------------------------------------------------------------------------
Delete empty BQ dataset
'''

def delete_dataset(target_client, target_dataset):

    dataset_ref = target_client.dataset(target_dataset)
    print(dataset_ref)
    dataset = target_client.get_dataset(dataset_ref)
    print(dataset.dataset_id)
    target_client.delete_dataset(dataset.dataset_id, delete_contents=True, not_found_ok=True)
    return True

'''
----------------------------------------------------------------------------------------------
Delete dataset:
'''

def delete_table_bq_job(target_dataset, delete_table):
    client = bigquery.Client()
    table_ref = client.dataset(target_dataset).table(delete_table)
    try:
        client.delete_table(table_ref)
        print('Table {}:{} deleted'.format(target_dataset, delete_table))
    except NotFound as ex:
        print('Table {}:{} was not present'.format(target_dataset, delete_table))
    except Exception as ex:
        print(ex)
        return False

    return True

'''
----------------------------------------------------------------------------------------------
We have nested schema structure, so need to make recursive creation function
'''

def make_schema_list(field_list):
    full_list = []
    for sf in field_list:
        if sf["type"] == "RECORD":
            # if sf["name"] == "ContextGroupIdentificationSequence":
            #     print("Here")
            field_list = make_schema_list(sf["fields"])
            if not "description" in sf:
                sf["description"] = "TBD"
            next_field = bigquery.SchemaField(sf["name"], sf["type"], sf["mode"], sf["description"], field_list)
        else:
            try:
                if not "description" in sf:
                    sf["description"] = "TBD"
                next_field = bigquery.SchemaField(sf["name"], sf["type"], sf["mode"], sf["description"])
            except KeyError as e:
                print("KeyError: {}".format(e))
        full_list.append(next_field)
    return full_list


'''
----------------------------------------------------------------------------------------------
Annotate the schema that was generated when a view was created with annotations from a 
supplied (probably partial) schema.
'''

def annotate_schema(generated_schema, annotation_schema):
    a=1
    for src_field in annotation_schema:
        # Only annotate in the case that the source description is not empty
        if src_field.description and src_field.description != "TBD":
            # Search for a field with matching name
            gen_field = next((trg_field for trg_field in generated_schema if src_field.name == trg_field.name ), None)
            if gen_field:
                new_gen_field = bigquery.SchemaField(
                        gen_field.name,
                        gen_field.field_type,
                        description = src_field.description,
                        fields = gen_field.fields if src_field.field_type != "RECORD" else \
                            annotate_schema(list(gen_field.fields), list(src_field.fields)),
                        mode = gen_field.mode,
                        policy_tags = gen_field.policy_tags
                    )
                # if src_field.field_type == "RECORD":
                #     annotate_schema(list(new_gen_field.fields), list(src_field.fields))
                generated_schema[generated_schema.index(gen_field)] = new_gen_field
            else:
                print("Failed to find target analog of source field {}".format(src_field.name))
    return generated_schema



'''
----------------------------------------------------------------------------------------------
Create a view in the target dataset
'''

def create_view(target_client, target_project, target_dataset, table_name, view_schema, view_sql):

    table_id = '{}.{}.{}'.format(target_project, target_dataset, table_name)

    print("Deleting {}".format(table_id))
    try:
        target_client.delete_table(table_id)
    except NotFound:
        print("View not found")
    print("Creating {}".format(table_id))

    #
    # Convert the dictionary into the tree of SchemaField objects:
    #

    targ_schema = make_schema_list(view_schema["schema"]["fields"])

    #
    # Not supposed to submit a schema for a view, so we don't. But we need to update it later to get the
    # descriptions brought across:
    #

    targ_view = bigquery.Table(table_id)
    targ_view.friendly_name = view_schema["friendlyName"]
    targ_view.description = view_schema["description"]
    targ_view.labels = view_schema["labels"]

    #
    # The way a table turns into a view is by setting the view_query property:
    #
    targ_view.view_query = view_sql

    # Create the view
    installed_targ_view = target_client.create_table(targ_view)

    # Convert the schema that was generated when we created the view into a tree of SchemaField objects
    generated_schema = installed_targ_view.schema

    # Add annotations to the generated view
    annotated_schema = annotate_schema(generated_schema, targ_schema)

    #
    # If we created a view, update the schema after creation:
    #

    installed_targ_view.schema = annotated_schema

    target_client.update_table(installed_targ_view, ["schema"])

    return True


'''
----------------------------------------------------------------------------------------------
Create a table in the target dataset
'''

def create_table(target_client, target_project, target_dataset, table_name, view_schema, view_sql):

    table_id = '{}.{}.{}'.format(target_project, target_dataset, table_name)

    print("Deleting {}".format(table_id))
    try:
        target_client.delete_table(table_id)
    except NotFound:
        print("View not found")
    print("Creating {}".format(table_id))

    #
    # Convert the dictionary into the tree of SchemaField objects:
    #

    targ_schema = make_schema_list(view_schema["schema"]["fields"])

    #
    # Not supposed to submit a schema for a view, so we don't. But we need to update it later to get the
    # descriptions brought across:
    #

    targ_table = bigquery.Table(table_id)
    targ_table.friendly_name = view_schema["friendlyName"]
    targ_table.description = view_schema["description"]
    targ_table.labels = view_schema["labels"]


    # #
    # # The way a table turns into a view is by setting the view_query property:
    # #
    # targ_table.view_query = view_sql

    # Create the view
    empty_targ_table = target_client.create_table(targ_table)

    # Populate the table
    table_id = "{}.{}.{}".format(target_project, target_dataset, table_name)
    job_config = bigquery.QueryJobConfig(destination=table_id)
    job = target_client.query(view_sql, job_config=job_config)
    # Wait for the query to complete
    result = job.result()

    installed_targ_table = target_client.get_table(table_id)
    # Convert the schema that was generated when we created the table into a tree of SchemaField objects
    generated_schema = installed_targ_table.schema
    # Add annotations to the generated schema
    annotated_schema = annotate_schema(generated_schema, targ_schema)
    installed_targ_table.schema = annotated_schema
    target_client.update_table(installed_targ_table, fields=["schema"])

    # Add any clustering fields
    if view_schema['clustering_fields']:
        installed_targ_table = target_client.get_table(table_id)
        installed_targ_table.clustering_fields = view_schema['clustering_fields']
        target_client.update_table(installed_targ_table, fields=['clustering_fields'])

    return True


'''
----------------------------------------------------------------------------------------------
Main Control Flow
Note that the actual steps run are configured in the YAML input!
This allows you to e.g. skip previously run steps.
'''

def main(args):

    # if len(args) != 5:
    #     print(" ")
    #     print(" Usage : {} <configuration_yaml>".format(args[0]))
    #     return
    #
    print('job started')

    #
    # Get the YAML config loaded:
    #

    with open(args.yaml_template, mode='r') as yaml_file:
        yaml_template = yaml_file.read()
        formatted_yaml = yaml_template.format(version=args.version, project=args.project, \
                    dataset=args.dataset)
        params, steps = load_config(formatted_yaml)
        # params, steps = load_config(yaml_file.read())

    if params is None:
        print("Bad YAML load")
        return

    target_project = params['TARGET_PROJECT']
    dataset_id = params['DATASET']
    data_file_path = params['DATA_FILE_PATH']
    dataset_metadata_file = params['DATASET_METADATA_FILE']
    install_list = params['INSTALL_LIST']

    if args.tables:
        new_install_list = []
        for row in install_list:
            for table in row:
                if table in args.tables:
                    new_install_list.append(row)
                break
        install_list = new_install_list

    target_client = bigquery.Client(project=target_project)

    #
    # Step 0: Delete dataset if it exists (for updates until versioning is in place)
    #

    if 'delete_existing_dataset' in steps:
        if bq_dataset_exists(target_client, dataset_id):
            success = delete_all_views(target_client, target_project, dataset_id)
            if not success:
                print("delete dataset step 1 failed")
                return
            success = delete_dataset(target_client, dataset_id)
            if not success:
                print("delete dataset step 2 failed")
                return

    #
    # Step 1: Do we need to create the dataset in the target project?
    #

    if 'create_dataset' in steps:
        # Where is the dataset description file:
        dataset_metadata_file_full_path = "{}/{}".format(data_file_path, dataset_metadata_file)
        with open(dataset_metadata_file_full_path, mode='r') as dataset_metadata:
            ds_meta_dict = json_loads(dataset_metadata.read())

        success = create_dataset(target_client, target_project, dataset_id, ds_meta_dict)
        if not success:
            print("create_dataset failed")
            return

    if 'delete_all_views' in steps:
        if bq_dataset_exists(target_client, dataset_id):
            success = delete_all_views(target_client, target_project, dataset_id)
            if not success:
                print("deleting all views failed")
                return

    if 'install_views' in steps:
        for mydict in install_list:
            for view_name, view_dict in mydict.items():
                if not args.view_name or args.view_name==view_name:
                    print("creating view: {}".format(view_name))
                    sql_format_file = view_dict["sql"]
                    metadata_file = view_dict["metadata"]
                    table_list = view_dict["table_list"]
                    metadata_file_full_path = "{}/{}".format(data_file_path, metadata_file)
                    sql_format_file_full_path = "{}/{}".format(data_file_path, sql_format_file)
                    with open(sql_format_file_full_path, mode='r') as sql_format_file:
                        sql_format = sql_format_file.read()
                    # use list as argument to format:
                    print(table_list)
                    # view_sql = sql_format.format(*table_list)
                    view_sql = sql_format.format(project=target_project, dataset=dataset_id)
                    with open(metadata_file_full_path, mode='r') as view_metadata_file:
                        view_schema = json_loads(view_metadata_file.read())
                    success = create_table(target_client, target_project, dataset_id, view_name, view_schema, view_sql)
                    success = create_view(target_client, target_project, dataset_id, f'{view_name}_view', view_schema, view_sql)
                    if not success:
                        print("shadow_datasets failed")
                        return

    print('job completed')

# if __name__ == "__main__":
#     main(sys.argv)