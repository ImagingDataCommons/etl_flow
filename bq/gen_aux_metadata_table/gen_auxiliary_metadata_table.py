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
from google.cloud import bigquery
from utilities.bq_helpers import load_BQ_from_json, query_BQ
from bq.gen_aux_metadata_table.schema import auxiliary_metadata_schema

def gen_aux_table(args):
    client = bigquery.Client(project=args.dst_project)
    query = auxiliary_metadata_schema.format(target=args.target, version=args.version, project=args.src_project, dataset=args.bqdataset_name, gcs_bucket=args.gcs_bucket)
    result=query_BQ(client, args.bqdataset_name, args.bqtable_name, query, write_disposition='WRITE_TRUNCATE')

if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--version', default=2, help='IDC version for which to build the table')
    args = parser.parse_args()
    parser.add_argument('--src_project', default='idc-dev-etl')
    parser.add_argument('--dst_project', default='idc-dev-etl')
    parser.add_argument('--bqdataset_name', default=f'idc_v{args.version}', help='BQ dataset name')
    parser.add_argument('--bqtable_name', default='auxiliary_metadata', help='BQ table name')
    parser.add_argument('--gcs_bucket', default='idc_dev', help="Bucket where blobs are")
    parser.add_argument('--target', default='dev', help="dev or pub")

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    gen_aux_table(args)