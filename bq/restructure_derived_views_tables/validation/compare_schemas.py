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

def compare_schemas(pdp_table_name, idc_table_name, has_urls):
    progresslogger.info(f'Compare {pdp_table_name} == {idc_table_name}')
    client = bigquery.Client()
    pdp_table = client.get_table(pdp_table_name)
    idc_table = client.get_table(idc_table_name)
    # pdp_schema = {row.name:row for row in pdp_table.schema}
    # idc_schema = {row.name:row for row in idc_table.schema}
    pdp_schema = OrderedDict((row.name, (index, row)) for index, row in enumerate(pdp_table.schema))
    idc_schema = OrderedDict((row.name, (index, row)) for index, row in enumerate(idc_table.schema))
    if has_urls:
        # Delete the aws_url schema element
        idc_schema.pop('aws_url')
    if pdp_table.schema == idc_table.schema:
        successlogger.info(f'{pdp_table_name} == {idc_table_name}')
    else:
        errlogger.error(f'{pdp_table_name} != {idc_table_name}')
        if len(idc_schema) != len(pdp_schema):
            errlogger.error(f'\tLengths differ: idc_schema:{len(idc_schema)}, pdp_schema:{len(pdp_schema)}')
        for key in idc_schema:
            if key in pdp_schema:
                if idc_schema[key] != pdp_schema[key]:
                    if idc_schema[key][1].field_type != pdp_schema[key][1].field_type:
                        errlogger.error(f'\t{key} fieldType differs:')
                        errlogger.error(f'\t\tIDC: {idc_schema[key][1]}')
                        errlogger.error(f'\t\tPDP: {pdp_schema[key][1]}')
                    # if idc_schema[key][0] != pdp_schema[key][0]:
                    #     errlogger.error(f'\t{key} offset  differs:')
                    #     errlogger.error(f'\t\tIDC: {idc_schema[key][0]}')
                    #     errlogger.error(f'\t\tPDP: {pdp_schema[key][0]}')
            else:
                errlogger.error(f'\tField {key} not in pdp_schema')
        for key in pdp_schema:
            if key not in idc_schema:
                errlogger.error(f'\tField {key} not in idc_schema')


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
        if has_view:
            # Validate the view form
            # project = args.dev_project
            # full_name = f'{project}.idc_v{dataset_version}.{table_name}_view' \
            #     if int(dataset_version) < 8 \
            #     else f'{project}.idc_v{dataset_version}_pub.{table_name}_view'
            # if full_name not in dones and full_name not in errors:
            #     test_hash = get_table_hash( \
            #         full_name,
            #         excepts)
            #     progresslogger.info(f'{ref_name}:{ref_hash}, {full_name}:{test_hash}')
            #     if str(ref_hash) == str(test_hash):
            #         successlogger.info(full_name)
            #     else:
            #         errlogger.error(full_name)
            # else:
            #     progresslogger.info(f'Skipping {full_name}')

            # Validate the view in prod
            project = args.pub_project
            full_name = f'{project}.idc_v{dataset_version}.{table_name}_view'
            if full_name not in dones and full_name not in errors:
                test_hash = compare_schemas(
                    ref_name,
                    full_name,
                    has_urls)
                # progresslogger.info(f'{ref_name}:{ref_hash}, {full_name}:{test_hash}')
                # if str(ref_hash) == str(test_hash):
                #     successlogger.info(full_name)
                # else:
                #     errlogger.error(full_name)
            else:
                progresslogger.info(f'Skipping {full_name}')

        # Validate the table  form
        # project = args.dev_project
        # # Validate the view in dev
        # full_name = f'{project}.idc_v{dataset_version}.{table_name}' \
        #     if int(dataset_version) < 8 \
        #     else f'{project}.idc_v{dataset_version}_pub.{table_name}'
        # if full_name not in dones and full_name not in errors:
        #     test_hash = get_table_hash( \
        #         full_name,
        #         excepts)
        #     progresslogger.info(f'{ref_name}:{ref_hash}, {full_name}:{test_hash}')
        #     if str(ref_hash) == str(test_hash):
        #         successlogger.info(full_name)
        #     else:
        #         errlogger.error(full_name)
        # else:
        #     progresslogger.info(f'Skipping {full_name}')

        # Validate the view in prod
        project = args.pub_project
        full_name = f'{project}.idc_v{dataset_version}.{table_name}'
        if full_name not in dones and full_name not in errors:
            test_hash = compare_schemas(
                ref_name,
                full_name,
                has_urls)
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
            ("original_collections_metadata", False, 1, False),
            ("analysis_results_metadata", False, 1, False),
            ("dicom_all", True, 1, True),
            ("dicom_metadata_curated", False, 7, True),
            ("dicom_metadata_curated_series_level", False, 13, True),
            ("measurement_groups", False, 1, True),
            ("qualitative_measurements", False, 1, True),
            ("quantitative_measurements", False, 1, True),
            ("segmentations", False, 1, True),
            ("dicom_derived_all", True, 1, False),
            (f"dicom_pivot_v{dataset_version}", True, 1, False),
            ("auxiliary_metadata", True, 1, False),
        ]
        
        for table_name, has_urls, min_version, has_view in steps:
            if (table_name == 'dicom_derived_all') & (int(dataset_version) < 4):
                continue
            if has_view:
                ref_name = f'{args.ref_project}.idc_v{dataset_version}.{table_name.removesuffix("_view")}'
                compare_tables(args, ref_name, table_name, has_urls, min_version, has_view)
            ref_name = f'{args.ref_project}.idc_v{dataset_version}.{table_name.removesuffix("_view")}'
            compare_tables(args, ref_name, table_name, has_urls, min_version, has_view)
        successlogger.info(dataset_version)

