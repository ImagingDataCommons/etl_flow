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


def compare_sql(table1_name, table2_name):
    progresslogger.info(f'Compare {table1_name} == {table2_name}')
    client = bigquery.Client()
    # try:
    #     table1 = client.get_table(f'{pdp_table_name}_view')
    # except:
    #     table1 = client.get_table(pdp_table_name)
    table1 = client.get_table(table1_name)
    if table1.table_type == 'TABLE':
        progresslogger.info(f'No view form of {table1_name}')
        return
    table2 = client.get_table(table2_name)
    # pdp_schema = {row.name:row for row in table1.schema}
    # idc_schema = {row.name:row for row in table2.schema}
    sql1 = table1.view_query
    sql2 = table2.view_query
    sql1 = sql1.replace('bigquery-public-data', 'idc-pdp-staging')
    sql2 = sql2.replace('bigquery-public-data', 'idc-pdp-staging')
    sql1 = sql1.split('\n')
    sql2 = sql2.split('\n')

    if sql1 == sql2:
        successlogger.info(f'{table1_name} == {table2_name}')
    else:
        for line in difflib.unified_diff(sql1, sql2, fromfile='pdp', tofile='idc', lineterm='\n'):
            print(line)

def compare_views(dones, table1_name, table2_name):
    if f'{table1_name} == {table2_name}' not in dones and f'{table1_name} != {table2_name}' not in errors:
        compare_sql(table1_name, table2_name)
    else:
        progresslogger.info(f'Skipping {table1_name} == {table2_name}')


if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()

    parser.add_argument('--project_1', default="idc-dev-etl")
    parser.add_argument('--project_2', default="idc-dev-etl")
    parser.add_argument('--version_delta', default=1)
    args = parser.parse_args()

    dones = open(f'{successlogger.handlers[0].baseFilename}').read().splitlines()
    errors = [row.split(':')[-1] for row in open(f'{errlogger.handlers[0].baseFilename}').read().splitlines()]

    for dataset_version in range(13,14):
        # if dataset_version in dones:
        #     continue
        progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

        steps = [
            ("dicom_all", 1, 9),
            ("dicom_all_view", 13, 13),
            ("dicom_metadata_curated", 5, 12),
            ("dicom_metadata_curated_view", 13, 13),
            ("dicom_metadata_curated_series_level_view", 13, 13),
            ("measurement_groups", 1, 12),
            ("measurement_groups_view", 13, 13),
            ("qualitative_measurements", 1, 12),
            ("qualitative_measurements_view", 13, 13),
            ("quantitative_measurements", 1, 12),
            ("quantitative_measurements_view", 13, 13),
            ("segmentations", 1, 12),
            ("segmentations_view", 13, 13),
            (f"dicom_pivot_v{dataset_version}", 1, 13),
        ]
        
        for table_name, min_version, max_version in steps:
            if dataset_version >= min_version and dataset_version <= max_version:
                table1_name = f'{args.project_1}.idc_v{dataset_version}_pub.{table_name}'
                table2_name = f'{args.project_2}.idc_v{dataset_version+args.version_delta}_pub.{table_name}'
                compare_views(dones, table1_name, table2_name)

        # successlogger.info(dataset_version)

