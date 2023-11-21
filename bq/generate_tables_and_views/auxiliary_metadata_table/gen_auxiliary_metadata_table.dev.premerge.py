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

# This script generates the BQ auxiliary_metadata table. It is parameterized
# to build with 'pre-merge GCS URLS of new instances.
# It is also paramaterized to build in the idc-dev-etl project.
import argparse
import sys
import json

import settings
from bq.generate_tables_and_views.auxiliary_metadata_table.gen_auxiliary_metadata_table import gen_aux_table

if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version for which to build the table')
    parser.add_argument('--target', default='dev', help="dev or prod")
    parser.add_argument('--merged', default=False, help='True if premerge buckets have been merged in dev buckets')
    parser.add_argument('--dst_project', default=f'{settings.DEV_PROJECT}')
    parser.add_argument('--trg_bqdataset_name', default=f'idc_v{settings.CURRENT_VERSION}_pub', help='BQ dataset of resulting table')
    parser.add_argument('--bqtable_name', default='auxiliary_metadata', help='BQ table name')
    args = parser.parse_args()

    args.access ='Public' # Fixed value
    print("{}".format(args), file=sys.stdout)

    gen_aux_table(args)