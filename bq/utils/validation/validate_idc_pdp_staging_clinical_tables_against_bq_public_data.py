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

from utilities.logging_config import successlogger, progresslogger,errlogger
from google.cloud import bigquery
from google.api_core.exceptions import NotFound


def skip(args):
    progresslogger.info("Skipped stage")
    return

def schema_has(schema, field_name):
    return next((row for row in schema if row.name == field_name),-1) != -1


def get_table_hash(table_id, excepts):

    client = bigquery.Client()
    try:
        table = client.get_table(table_id)
    except NotFound:
        progresslogger.info(f'{table_id} not found')
        return ""

    # See if the table has any of the 'standard' fields to exclude
    # for field in ['gcs_url', 'aws_url', 'gcs_bucket', 'instance_size']:
    # for field in ['aws_url']:
    #     if schema_has(table.schema, field):
    #         excepts.append(field)
    # if excepts:
    #     except_clause = f"EXCEPT({','.join(excepts)})"
    # else:
    #     except_clause = ""
    except_clause = ""
    query = f"""
    WITH no_urls AS (
        SELECT * {except_clause}
        FROM `{table}`
    )
    SELECT BIT_XOR(DISTINCT FARM_FINGERPRINT(TO_JSON_STRING(t))) as table_hash
    FROM no_urls  AS t
    """

    table_hash =  [dict(row) for row in client.query(query)][0]['table_hash']
    return table_hash
    # job = client.query(query)
    # # Wait for completion
    # result = job.result()
    # return result.json()


if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()

    parser.add_argument('--project1', default="bigquery-public-data", help='Project of reference datasets')
    parser.add_argument('--project2', default="idc-pdp-staging", help='Project of pub datasets')
    parser.add_argument('--version2_delta', default=0)
    args = parser.parse_args()

    client = bigquery.Client()
    dones = open(f'{successlogger.handlers[0].baseFilename}').read().splitlines()
    errors = [row.split(':')[-1] for row in open(f'{errlogger.handlers[0].baseFilename}').read().splitlines()]
    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    for dataset_version in [i for i in range(1,14)]:
        if str(dataset_version) in dones:
            continue

        project1_table_ids = [table.table_id for table in
                     client.list_tables(f'{args.project1}.idc_v{dataset_version}_clinical')]
        project2_table_ids = [table.table_id for table in
                     client.list_tables(f'{args.project2}.idc_v{dataset_version}_clinical')]

        if set(project1_table_ids) != set(project2_table_ids):
            errlogger.error(f'Datasets idc_v{dataset_version} are different')

        for table_name in project1_table_ids:
            # if (table_name == 'dicom_derived_all') & (int(dataset_version) < 4):
            #     continue
            table1 = f'{args.project1}.idc_v{dataset_version}_clinical.{table_name}'
            # See if we've already done this table/view

            excepts = []
            table2 = f'{args.project2}.idc_v{dataset_version}_clinical.{table_name}'
            if table2 not in dones and table2 not in errors:

                ref_hash = get_table_hash(
                    table1,
                    excepts
                )
                # Validate the view in prod
                test_hash = get_table_hash( \
                    table2,
                    excepts)
                progresslogger.info(f'{table1}:{ref_hash}, {table2}:{test_hash}')
                if str(ref_hash) == str(test_hash):
                    successlogger.info(table2)
                else:
                    errlogger.error(table2)
            else:
                progresslogger.info(f'Skipping {table2} previously verified')


        # successlogger.info(dataset_version)

