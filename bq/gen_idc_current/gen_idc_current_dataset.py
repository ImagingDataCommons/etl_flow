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
# from google.cloud.exceptions import A

from utilities.bq_helpers import load_BQ_from_json, query_BQ, create_BQ_dataset, delete_BQ_dataset


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
    delete_all_views_in_target_dataset(trg_client, args)

    # Get a list of the tables in the source dataset
    src_tables = [table for table in trg_client.list_tables (f'{args.src_project}.{args.src_bqdataset}')]

    # For each table, get a view and create a view in the target dataset
    for table in src_tables:

        table_id = table.table_id

        # Create the view object
        trg_view = bigquery.Table(f'{args.trg_project}.{args.current_bqdataset}.{table_id}')
        # Form the view SQL
        view_sql = f"""
            select * from `{args.src_project}.{args.src_bqdataset}.{table_id}`"""
        # Add the SQL to the view object
        trg_view.view_query = view_sql
        # Create the view in the target dataset
        installed_targ_view = trg_client.create_table(trg_view)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=7, help='Current IDC version')
    args = parser.parse_args()
    parser.add_argument('--src_project', default='idc-dev-etl')
    parser.add_argument('--trg_project', default='idc-dev-etl')
    parser.add_argument('--src_bqdataset', default=f'idc_v{args.version}', help='BQ dataset name')
    parser.add_argument('--current_bqdataset', default=f'idc_current_whc', help='current dataset name')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    gen_idc_current_dataset(args)
