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

# Generate the analysis_results_descriptions table in BQ and PSQL from a
# spreadsheet in Google Drive
import settings
import argparse
from utils.json_to_bq_table import json_to_bq
from utils.bq_table_to_cloudsql import export_table

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--spreadsheet_id', default = '1HqKVSjvfaX1IOrZYgkVtVmwuSj8iIjTt',
                        help='"id" portion of spreadsheet URL')
    parser.add_argument('--project', default='idc-dev-etl', help='BQ project')
    parser.add_argument('--bq_dataset_id', default=f'idc_v{settings.CURRENT_VERSION}_dev', help='BQ datasey')
    parser.add_argument('--table_id', default='doi_to_access', help='Table name to which to copy data')
    parser.add_argument('--columns', default=[], help='Columns in df to keep. Keep all if list is empty')

    args = parser.parse_args()
    print('args: {}'.format(args))

    with open('table_generation_jsons/doi_to_access.json') as f:
        json_string = f.read()
    json_to_bq(args, json_string)
