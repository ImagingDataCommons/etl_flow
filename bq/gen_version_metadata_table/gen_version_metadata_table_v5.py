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

# This script generates the BQ version_metadata table.
import argparse
import sys
from bq.gen_version_metadata_table.gen_version_metadata_table import gen_version_metadata_table

if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--version', default=5, help='Max IDC version for which to build the table')
    args = parser.parse_args()
    parser.add_argument('--src_project', default='idc-dev-etl')
    parser.add_argument('--dst_project', default='idc-dev-etl')
    parser.add_argument('--bqdataset_name', default=f'idc_v{args.version}', help='BQ dataset name')
    parser.add_argument('--bqtable_name', default='version_metadata', help='BQ table name')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    gen_version_metadata_table(args)