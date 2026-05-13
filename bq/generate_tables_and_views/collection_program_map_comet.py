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


# Generate a BQ table that maps collection_name to the program of that collection
# The tables includes all TCIA collections, not just TCIA collections that IDC also
# has...it's a superset of IDC collections.

import pandas as pd

import settings
import argparse
from utilities.tcia_helpers import get_tcia_collection_manager_data
from google.cloud import bigquery
from bq.bq_utilities import get_github_directory_contents_from_comet, dataframe_to_bq,\
    get_data_from_comet


SCHEMA = [
    bigquery.SchemaField('collection_id', 'STRING', mode='NULLABLE', description='Collection id'),
    bigquery.SchemaField('program', 'STRING', mode='NULLABLE', description='Program name')]

def get_all_programs(path, branch):
    collection_files = get_github_directory_contents_from_comet(path, branch=branch)
    programs = []
    for collection_file in collection_files:
        # print(collection_file)
        collection_data = get_data_from_comet(f"{path}/{collection_file}", branch=branch)
        programs.append(
            {
                "collection_id": collection_data['collection_id'],
                "program": collection_data['program']
            }
        )
    return pd.DataFrame(programs)

def gen_table(args):
    all_programs = get_all_programs("collections/original", args.comet_branch)
    dataframe_to_bq(args, all_programs)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', default='idc-dev-etl', help='BQ project')
    parser.add_argument('--bq_dataset_id', default=f'idc_v{settings.CURRENT_VERSION}_dev', help='BQ datasey')
    parser.add_argument('--table_id', default='collection_program_map', help='Table name to which to copy data')
    parser.add_argument("--comet_branch", default = 'release/v24')

    args = parser.parse_args()
    print('args: {}'.format(args))

    gen_table(args)
    # export_table(args)
