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

# Generate the idc_dois table in BQ and PSQL from a
# spreadsheet in Google Drive
import settings
import argparse
from utils.google_sheet_to_bq_table import load_spreadsheet
from utils.bq_table_to_cloudsql import export_table

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # parser.add_argument('--spreadsheet_id', default = '1vRJCPYfyyfsbhU8wTWGplBRMtODP56AR',
    parser.add_argument('--spreadsheet_id', default='1VKthFbTIExjOwl0lgHzTbjAtgv22hHbovAcjJXHWEhc',
                                            help='"id" portion of spreadsheet URL')
    parser.add_argument('--sheet_name', default = 'idc_collection_dois', help='Sheet within spreadsheet to load')
    parser.add_argument('--project', default='idc-dev-etl', help='BQ project')
    parser.add_argument('--bq_dataset_id', default=f'idc_v{settings.CURRENT_VERSION}_dev', help='BQ datasey')
    parser.add_argument('--table_id', default='idc_collection_dois', help='Table name to which to copy data')
    parser.add_argument('--columns', default=[], help='Columns in df to keep. Keep all if list is empty')

    args = parser.parse_args()
    print('args: {}'.format(args))

    load_spreadsheet(args)
