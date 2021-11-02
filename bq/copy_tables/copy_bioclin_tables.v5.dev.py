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

# Copy "related" tables (bioclin) from one dataset to another. Normally
# we just copy since these do not change very often.
import argparse
import sys
from bq.copy_tables.copy_tables import copy_tables
from google.cloud import bigquery
from utilities.bq_helpers import create_BQ_dataset, copy_BQ_table
from bq.gen_aux_metadata_table.auxiliary_metadata_sql_v3 import auxiliary_metadata_sql
from google.api_core.exceptions import NotFound


if __name__ == '__main__':

    parser =argparse.ArgumentParser()
    parser.add_argument('--version', default=5, help='IDC version for which to build the table')
    args = parser.parse_args()
    parser.add_argument('--src_project', default='idc-dev-etl')
    parser.add_argument('--dst_project', default='idc-dev-etl')
    parser.add_argument('--src_bqdataset', default=f'idc_v{args.version-1}', help='Source BQ dataset')
    parser.add_argument('--dst_bqdataset', default=f'idc_v{args.version}', help='Destination BQ dataset')
    parser.add_argument('--dataset_description', default = f'IDC V{args.version} BQ tables and views')
    parser.add_argument('--bqtables', \
        default=['nlst_canc', 'nlst_ctab', 'nlst_ctabc', 'nlst_prsn', 'nlst_screen', \
                'tcga_biospecimen_rel9', 'tcga_clinical_rel9'], help='BQ tables to be copied')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    copy_tables(args)