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
import sys
import os
import json
import time
from google.cloud import bigquery
from utilities.bq_helpers import load_BQ_from_json, query_BQ
from bq.gen_original_data_collections_table.schema import data_collections_metadata_schema
from utilities.tcia_helpers import get_collection_descriptions
from utilities.tcia_scrapers import scrape_tcia_data_collections_page
from bq.gen_aux_metadata_table.schema import auxiliary_metadata_schema

def gen_aux_table(args):
    client = bigquery.Client(project=args.project)
    query = auxiliary_metadata_schema.format(project=args.project, dataset=args.bqdataset_name)
    result=query_BQ(client, args.bqdataset_name, args.bqtable_name, query, write_disposition='WRITE_TRUNCATE')

if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--project', default='idc-dev-etl')
    parser.add_argument('--version', default=2, help='IDC version for which to build the table')
    args = parser.parse_args()
    parser.add_argument('--bqdataset_name', default=f'idc_v{args.version}', help='BQ dataset name')
    # parser.add_argument('--bqdataset_name', default=f'whc_dev', help='BQ dataset name')
    parser.add_argument('--bqtable_name', default='auxiliary_metadata_dev', help='BQ table name')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    gen_aux_table(args)