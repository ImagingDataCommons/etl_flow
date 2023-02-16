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
Populate the gcs_url and aws_url columns of auxiliary_metadata
"""

import settings
import argparse
import json
import time
from google.cloud import bigquery
from utilities.logging_config import successlogger, progresslogger, errlogger

def create_table_from_view(client, args, view_id, table_id):
    # # We first create a view named like <table_id)_view
    # create_view_from_table(client, args, table, )
    view = client.get_table(view_id)
    new_table = bigquery.Table(table_id)
    # new_table.view_query = revised_sql
    new_table.friendly_name = view.friendly_name
    new_table.description = view.description
    new_table.labels = view.labels
    view_query = view.view_query
    schema = view.schema
    # Ensure that the table does not already exist
    client.delete_table(table_id, not_found_ok=True)
    client.create_table(new_table)

    job_config = bigquery.QueryJobConfig(destination=table_id)
    job = client.query(view_query, job_config=job_config)
    # Wait for the query to complete
    result = job.result()

    installed_table = client.get_table(table_id)

    # Update the schema after creating the view
    installed_table.schema = schema
    client.update_table(installed_table,['schema'])

    progresslogger.info(f'Created table {table_id}')
    return


def populate_urls_in_dicom_all(args):
    client = bigquery.Client()
    table_id = f'{args.trg_project}.{args.trg_dataset}.dicom_all'
    view_id = f'{table_id}_view'
    create_table_from_view(client, args, view_id, table_id)
    return

if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()

    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version number')
    # parser.add_argument('--project', default="idc-dev-etl", help='Project in which tables live')
    # parser.add_argument('--dev_dataset', default=f"idc_v{settings.CURRENT_VERSION}_dev", help="BQ source dataset")
    # parser.add_argument('--trg_dataset', default=f"whc_dev_idc_v5", help="BQ targetdataset")
    # # parser.add_argument('--uuid_url_map', default="idc-dev-etl.idc_v14_dev.uuid_url_map",
    # #                     help="Table that maps instance uuids to URLS")
    parser.add_argument('--dev_or_pub', default='dev', help='Revising the dev or pub version of auxiliary_metadata')
    args = parser.parse_args()

    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    # populate_urls_in_dicom_all(args)
