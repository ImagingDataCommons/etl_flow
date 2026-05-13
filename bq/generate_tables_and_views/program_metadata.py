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


# This script generates the BQ program_metadata table.
import argparse
import sys
import json
from google.cloud import bigquery
import hashlib

import settings
from utilities.bq_helpers import load_BQ_from_json
from utilities.logging_config import successlogger, errlogger
from bq.bq_utilities import get_data_from_comet

version_metadata_schema = [
    bigquery.SchemaField('program_name', 'STRING', mode='REQUIRED', description='Short program name'),
    bigquery.SchemaField('program_id', 'STRING', mode='REQUIRED', description="Lower cased short program name"),
    bigquery.SchemaField('program_title', 'STRING', mode='REQUIRED', description='Descriptive program title'),
    bigquery.SchemaField('program_url', 'STRING', mode='REQUIRED', description='URL of program information page'),
    bigquery.SchemaField('program_description', 'STRING', mode='REQUIRED', description='Brief program description'),
    ]



def gen_program_metadata_table(args):
    client = bigquery.Client(project=args.src_project)
    programs = get_data_from_comet(args.path, branch=args.comet_branch)['programs']

    for row in programs:
        row["program_url"] = "None" if row["program_url"] is None else row["program_url"]
    metadata_json = '\n'.join([json.dumps(row) for row in
                               sorted(programs, key=lambda d: d['program_name'])])
    try:
        job = load_BQ_from_json(client, args.dst_project, args.bqdataset_name, args.bqtable_name, metadata_json,
                            version_metadata_schema, write_disposition='WRITE_TRUNCATE')
        successlogger.info('program_metadata table generation completed')
        return
    except Exception as exc:
        errlogger.info(f'Error creating BQ table; {exc}')
    exit(1)
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='Max IDC version for which to build the table')
    parser.add_argument('--src_project', default='idc-dev-etl')
    parser.add_argument('--dst_project', default='idc-dev-etl')
    parser.add_argument('--bqdataset_name', default=f'idc_v{settings.CURRENT_VERSION}_pub', help='BQ dataset name')
    parser.add_argument('--bqtable_name', default=f'program_metadata', help='BQ table name')
    parser.add_argument('--comet_branch', default='release/v24', help="idc_comet github branch")
    parser.add_argument("--path", default="vocabularies/programs.yaml", help="Path from branch to file")

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    gen_program_metadata_table(args)