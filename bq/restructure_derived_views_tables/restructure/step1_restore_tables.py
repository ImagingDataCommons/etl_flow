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
Add an aws_url column to auxiliary_metadata table
"""
import settings
import argparse
import json
import time
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import NotFound
from utilities.bq_helpers import copy_BQ_table
from utilities.logging_config import successlogger, progresslogger, errlogger


# We do a table update rather than regenerate the entire table.
# By doing it this way, we do not need the SQL for each IDC version
def restore_tables(args, dones):
    client = bigquery.Client()
    src_client = bigquery.Client(args.src_project)
    trg_client = bigquery.Client(args.trg_project)

    src_dataset = src_client.get_dataset(args.src_dataset)
    trg_dataset = trg_client.get_dataset(args.trg_dataset)

    for table_name in ['auxiliary_metadata', 'dicom_all', 'dicom_derived_all']:
        table_id = f'{args.trg_project}.{args.trg_dataset}.{table_name}'
        if not table_id in dones:
            try:
                table = client.get_table(table_id)
                if table.table_type == 'TABLE':
                    src_table = src_dataset.table(table_name)
                    trg_table = trg_dataset.table(table_name)
                    copy_BQ_table(client, src_table, trg_table)
                    successlogger.info(f"{table_id}")
            except NotFound:
                progresslogger.info(f"Table {table_id} doesn't exist")

    return


# if __name__ == '__main__':
#     # (sys.argv)
#     parser = argparse.ArgumentParser()
#
#     parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version number')
#     parser.add_argument('--project', default="idc-dev-etl", help='Project in which tables live')
#     # parser.add_argument('--dataset', default=f"idc_v{settings.CURRENT_VERSION}_pub", help="BQ dataset")
#     parser.add_argument('--trg_dataset', default=f"whc_dev_idc_v13_pub", help="BQ target dataset")
#     args = parser.parse_args()
#
#     progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')
#
#     remove_aws_column_from_aux(args)
