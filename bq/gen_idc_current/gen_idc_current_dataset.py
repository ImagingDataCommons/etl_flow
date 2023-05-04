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

# This script generates the idc_current dataset, which is comprised of a view of every table and view in
# the idc_vX dataset corresponding to the current IDC data version.

import argparse
import sys
from google.cloud import bigquery
# from bq.derived_tables_creation.publish_bq_derived_tables import annotate_schema

# from google.cloud.exceptions import A

from utilities.bq_helpers import load_BQ_from_json, query_BQ, create_BQ_dataset, delete_BQ_dataset

'''
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


def delete_all_views_in_target_dataset(target_client, args,):
    views = [views for views in target_client.list_tables (f'{args.trg_project}.{args.current_bqdataset}')]
    for view in views:
        # pass
        target_client.delete_table(view)


def gen_idc_current_dataset(args):
    trg_client = bigquery.Client(project=args.trg_project)

    try:
        create_BQ_dataset(trg_client, args.current_bqdataset)
    except Exception as exc:
        print("Target dataset already exists")

    # Delete any views in the target dataset
    # delete_all_views_in_target_dataset(trg_client, args)

    # Get a list of the tables in the source dataset
    src_tables = [table for table in trg_client.list_tables (f'{args.src_project}.{args.src_bqdataset}')]

    # For each table, get a view and create a view in the target dataset
    for src_table in src_tables:

        table_id = src_table.table_id
        print(f'View: {table_id}')


        trg_view = bigquery.Table(f'{args.trg_project}.{args.current_bqdataset}.{table_id}')
        # Delete the existing view
        trg_client.delete_table(trg_view, not_found_ok=True)
        # Form the view SQL
        view_sql = f"""
            select * from `{args.src_project}.{args.src_bqdataset}.{table_id}`"""
        # Add the SQL to the view object
        trg_view.view_query = view_sql
        # Create the view in the target dataset
        installed_targ_view = trg_client.create_table(trg_view)

        # Now add descriptions to the view
        src_schema = trg_client.get_table(trg_client.dataset(args.src_bqdataset).table(table_id)).schema
        generated_schema = installed_targ_view.schema
        annotated_schema = annotate_schema(generated_schema, src_schema)

        installed_targ_view.schema = annotated_schema
        installed_targ_view.description = 'Views in this dataset reference the tables in the dataset corresponding to the current IDC version.'
        trg_client.update_table(installed_targ_view, fields=["schema", "description"])


# if __name__ == '__main__':
#     parser = argparse.ArgumentParser()
#     parser.add_argument('--version', default=7, help='Current IDC version')
#     args = parser.parse_args()
#     parser.add_argument('--src_project', default='idc-dev-etl')
#     parser.add_argument('--trg_project', default='idc-dev-etl')
#     parser.add_argument('--src_bqdataset', default=f'idc_v{args.version}', help='BQ dataset name')
#     parser.add_argument('--current_bqdataset', default=f'idc_current_whc', help='current dataset name')
#
#     args = parser.parse_args()
#     print("{}".format(args), file=sys.stdout)
#     gen_idc_current_dataset(args)
