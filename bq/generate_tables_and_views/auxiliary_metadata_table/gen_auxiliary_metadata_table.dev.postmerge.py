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

# This script generates the BQ auxiliary_metadata table. It basically joins the BQ version, collection,
# patient, study, series, and instance tables. Typically these are uploaded from PostgreSQL to BQ using
# the upload_psql_to_bq.py script
import argparse
import sys
import json

import settings
from bq.auxiliary_metadata_table.gen_auxiliary_metadata_table import gen_aux_table

if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version for which to build the table')
    parser.add_argument('--target', default='dev', help="dev or prod")
    parser.add_argument('--merged', default=True, help='True if premerge buckets have been merged in dev buckets')
    # parser.add_argument('--src_project', default='idc-dev-etl')
    parser.add_argument('--dst_project', default=f'{settings.DEV_PROJECT}')
    # parser.add_argument('--dev_bqdataset_name', default=f'idc_v{args.version}_dev', help='BQ dataset containing development tables')
    # parser.add_argument('--pub_bqdataset_name', default=f'idc_v{args.version}_pub', help='BQ dataset containing public tables')
    parser.add_argument('--trg_bqdataset_name', default=f'idc_v{settings.CURRENT_VERSION}_pub', help='BQ dataset of resulting table')
    parser.add_argument('--bqtable_name', default='auxiliary_metadata', help='BQ table name')
    parser.add_argument('--temp_license_table_name', default='temp_licenses', help='BQ table name')
    args = parser.parse_args()

    args.access ='Public' # Fixed value
    print("{}".format(args), file=sys.stdout)

    gen_aux_table(args)