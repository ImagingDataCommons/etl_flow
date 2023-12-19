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
import argparse
import json
import settings
from collections import OrderedDict
from utilities.logging_config import successlogger, progresslogger,errlogger
from google.cloud import bigquery,storage

def get_table_hash(table):

    client = bigquery.Client()
    query = f"""
    WITH selected AS (
        SELECT * 
        FROM `{table}`
    )
    SELECT BIT_XOR(DISTINCT FARM_FINGERPRINT(TO_JSON_STRING(t))) as table_hash
    FROM selected  AS t
    """

    table_hash =  [dict(row) for row in client.query(query)][0]['table_hash']
    return table_hash

def compare_views_in_dataset(dataset):
    client = bigquery.Client()
    # client = bigquery.Client(project=args.trg_project)


    table_ids = {table.table_id: table.table_type for table in client.list_tables(f'nci-idc-bigquery-data.{dataset}')}
    # Create tables first
    for table_id in table_ids:
        if table_ids[table_id] == 'VIEW':

            table = f'nci-idc-bigquery-data.{dataset}.{table_id}'
            src_hash = get_table_hash(table)
            table = f'bigquery-public-data.{dataset}.{table_id}'
            trg_hash = get_table_hash(table)
            if src_hash != trg_hash:
                errlogger.info(f'Hashes do not match for {table_id}')
            else:
                progresslogger.info(f'Hashes match for {table_id}')

    return


if __name__ == '__main__':

    for dataset in [f'idc_v{settings.CURRENT_VERSION}', 'idc_current', 'idc_current_clinical']:
        progresslogger.info(f'Validating {dataset}')
        compare_views_in_dataset(dataset)