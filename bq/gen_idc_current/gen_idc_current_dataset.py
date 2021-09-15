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


def gen_idc_current_dataset(args):
    target_client = bigquery.Client(project=args.project)

    try:
        delete_BQ_dataset(target_client, args.current_bqdataset)
    except Exception as exc:
        print(exc)
        exit

    try:
        dataset = create_BQ_dataset(target_client, args.current_bqdataset)
    except Exception as exc:
        print(exc)
        exit

    tables = [table for table in target_client.list_tables (f'{args.project}.{args.bqdataset}')]

    for table in tables:
        table_id = table.table_id
        targ_view = bigquery.Table(f'{args.project}.{args.current_bqdataset}.{table_id}')

        view_sql = f"""
            select * from `{args.project}.{args.bqdataset}.{table_id}`"""
        targ_view.view_query = view_sql
        installed_targ_view = target_client.create_table(targ_view)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=2, help='Current IDC version')
    args = parser.parse_args()
    parser.add_argument('--project', default='idc-dev-etl')
    parser.add_argument('--bqdataset', default=f'idc_v{args.version}', help='BQ dataset name')
    parser.add_argument('--current_bqdataset', default=f'idc_current_whc', help='current dataset name')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    gen_idc_current_dataset(args)
