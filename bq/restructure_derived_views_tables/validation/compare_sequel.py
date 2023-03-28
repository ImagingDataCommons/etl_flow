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
import difflib
from utilities.logging_config import successlogger, progresslogger,errlogger
from google.cloud import bigquery,storage


def skip(args):
    progresslogger.info("Skipped stage")
    return

def get_table_hash(table, except_clause):

    client = bigquery.Client()
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

def compare_sql(pdp_table_name, idc_table_name, has_urls, has_view):
    progresslogger.info(f'Compare {pdp_table_name} == {idc_table_name}')
    client = bigquery.Client()
    # try:
    #     pdp_table = client.get_table(f'{pdp_table_name}_view')
    # except:
    #     pdp_table = client.get_table(pdp_table_name)
    pdp_table = client.get_table(pdp_table_name)
    if pdp_table.table_type == 'TABLE':
        progresslogger.info(f'No view form of {pdp_table_name}')
        return
    idc_table = client.get_table(idc_table_name)
    # pdp_schema = {row.name:row for row in pdp_table.schema}
    # idc_schema = {row.name:row for row in idc_table.schema}
    pdp_sql = pdp_table.view_query
    idc_sql = idc_table.view_query
    pdp_sql = pdp_sql.replace('bigquery-public-data', 'idc-pdp-staging')
    pdp_sql = pdp_sql.split('\n')
    idc_sql = idc_sql.split('\n')

    if has_urls:
        # Delete the aws_url schema element
        idc_sql.pop(
            next(index for index, row in enumerate(idc_sql) if 'aws_url' in row)
        )
    if pdp_sql == idc_sql:
        successlogger.info(f'{pdp_table_name} == {idc_table_name}')
    else:
        for line in difflib.unified_diff(pdp_sql, idc_sql, fromfile='pdp', tofile='idc', lineterm='\n'):
            print(line)

def compare_tables(args, ref_name, table_name, has_urls, min_version, has_view):
    if int(dataset_version) >= min_version:
        # index = next((index for index, row in enumerate(dones) if row.split(',')[0] == ref_name), -1)
        # if index == -1:
        #     if table_name == 'auxiliary_metadata' and int(dataset_version) <= 2:
        #         excepts = 'EXCEPT(gcs_url, gcs_bucket)'
        #     else:
        #         excepts = 'EXCEPT(gcs_url)' if has_urls else ''
        #     ref_hash = get_table_hash(
        #         ref_name,
        #         excepts
        #     )
        #     successlogger.info(f'{ref_name},{ref_hash}')
        # else:
        #     ref_hash = dones[index].split(',')[1]
        # # continue

        if table_name == 'original_collections_metadata' and int(dataset_version) <= 2:
            excepts = 'EXCEPT(gcs_url, aws_url, gcs_bucket)'
        else:
            excepts = 'EXCEPT(gcs_url, aws_url)' if has_urls else ''

       # Validate the view in prod
        project = args.pub_project
        # full_name = f'{project}.idc_v{dataset_version}.{table_name}_view'
        full_name = f'{project}.idc_v{dataset_version}.{table_name}{"_view" if has_view else ""}'
        if f'{ref_name} == {full_name}' not in dones and full_name not in errors:
            test_hash = compare_sql(
                ref_name,
                full_name,
                has_urls,
                has_view)
            # progresslogger.info(f'{ref_name}:{ref_hash}, {full_name}:{test_hash}')
            # if str(ref_hash) == str(test_hash):
            #     successlogger.info(full_name)
            # else:
            #     errlogger.error(full_name)
        else:
            progresslogger.info(f'Skipping {full_name}')


if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()

    parser.add_argument('--ref_project', default="bigquery-public-data", help='Project of reference datasets')
    parser.add_argument('--dev_project', default="idc-dev-etl", help='Project of dev datasets')
    parser.add_argument('--pub_project', default="idc-pdp-staging", help='Project of pub datasets')
    args = parser.parse_args()

    dones = open(f'{successlogger.handlers[0].baseFilename}').read().splitlines()
    errors = [row.split(':')[-1] for row in open(f'{errlogger.handlers[0].baseFilename}').read().splitlines()]

    for dataset_version in [str(i) for i in range(1,14)]:
        # if dataset_version in dones:
        #     continue
        progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

        steps = [
            ("dicom_all", True, 1, True),
            ("dicom_metadata_curated", False, 7, True),
            ("dicom_metadata_curated_series_level", False, 13, True),
            ("measurement_groups", False, 1, True),
            ("qualitative_measurements", False, 1, True),
            ("quantitative_measurements", False, 1, True),
            ("segmentations", False, 1, True),
            (f"dicom_pivot_v{dataset_version}", True, 1, False),
        ]
        
        for table_name, has_urls, min_version, has_view in steps:
            if (table_name == 'dicom_derived_all') & (int(dataset_version) < 4):
                continue
        #     if has_view:
        #         ref_name = f'{args.ref_project}.idc_v{dataset_version}.{table_name}'
        #         compare_tables(args, ref_name, table_name, has_urls, min_version, has_view)
        #
            ref_name = f'{args.ref_project}.idc_v{dataset_version}.{table_name}'
            compare_tables(args, ref_name, table_name, has_urls, min_version, has_view)

        # successlogger.info(dataset_version)

