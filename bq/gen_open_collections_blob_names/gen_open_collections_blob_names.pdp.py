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

# This script generates a BQ table that is the names, <uuid>.dcm,
# of all blobs of instances in the open collections...those collections
# hosted by Googls PDP.
import argparse
import sys
from bq.gen_open_collections_blob_names.gen_open_collections_blob_names import gen_blob_table

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=5, help='IDC version for which to build the table')
    args = parser.parse_args()
    parser.add_argument('--src_project', default='idc-dev-etl')
    parser.add_argument('--dst_project', default='idc-pdp-staging')
    parser.add_argument('--bqdataset_name', default=f'idc_metadata', help='BQ dataset name')
    parser.add_argument('--bqtable_name', default='open_collections_blob_names_v5', help='BQ table name')
    parser.add_argument('--sql', default=f'./gen_open_collections_blob_names.sql')
    args = parser.parse_args()

    print("{}".format(args), file=sys.stdout)

    args.sql = open(args.sql).read()

    gen_blob_table(args)

