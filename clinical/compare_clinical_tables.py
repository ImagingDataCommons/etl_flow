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

# Compare the sets of clinical table in the current and previous IDC versions

import argparse
from google.cloud import bigquery
from utilities.logging_config import successlogger, progresslogger, errlogger
import settings


def compare_tables(args):
    client = bigquery.Client()
    current_dataset = f'idc-dev-etl.idc_v{settings.CURRENT_VERSION}_clinical'
    previous_dataset = f'idc-dev-etl.idc_v{settings.PREVIOUS_VERSION}_clinical'

    current_tables = set([table.table_id for table in client.list_tables(current_dataset)])
    previous_tables = set([table.table_id for table in client.list_tables(previous_dataset)])

    new_tables = current_tables - previous_tables
    progresslogger.info("\n***New tables")
    for table_id in new_tables:
        progresslogger.info(f'\t{table_id}')

    dropped_tables =  previous_tables - current_tables
    progresslogger.info("\n***dropped tables")
    for table_id in dropped_tables:
        progresslogger.info(f'\t{table_id}')

    progresslogger.info("\n***Revised tables")
    retained_table = current_tables & previous_tables
    for table_id in retained_table:
        query = f"""
            SELECT BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) as table_hash
            FROM `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_clinical.{table_id}` AS t
        """
        # Make an API request to execute the query
        query_job = client.query(query)

        # Wait for the job to complete and get the results
        current_table_hash = list(query_job.result())[0].table_hash

        query = f"""
             SELECT BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) as table_hash
             FROM `idc-dev-etl.idc_v{settings.PREVIOUS_VERSION}_clinical.{table_id}` AS t
         """
        # Make an API request to execute the query
        query_job = client.query(query)

        # Wait for the job to complete and get the results
        previous_table_hash = list(query_job.result())[0].table_hash
        if current_table_hash != previous_table_hash:
            progresslogger.info(f'\t{table_id}')


    return

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    args = parser.parse_args()
    successlogger.info(f"{args}")

    compare_tables(args)