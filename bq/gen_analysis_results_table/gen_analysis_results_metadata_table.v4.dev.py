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
from bq.gen_analysis_results_table.gen_analysis_results_metadata_table import gen_collections_table
import os
import json
import time
from google.cloud import bigquery
from utilities.bq_helpers import load_BQ_from_json
from bq.gen_analysis_results_table.schema import analysis_results_metadata_schema
from utilities.tcia_scrapers import scrape_tcia_analysis_collections_page

# Build the analysis_results_metadata BQ table

# Return a list of the source_dois in IDC collections
# This includes original collection and analysis results DOIs


if __name__ == '__main__':
    parser =argparse.ArgumentParser()

    parser.add_argument('--version', default=4, help='IDC version for which to build the table')
    args = parser.parse_args()
    parser.add_argument('--src_project', default='idc-dev-etl')
    parser.add_argument('--dst_project', default='idc-dev-etl')
    parser.add_argument('--bqdataset_name', default=f'idc_v{args.version}', help='BQ dataset name')
    parser.add_argument('--bqtable_name', default='analysis_results_metadata', help='BQ table name')
    parser.add_argument('--bq_collection_table', default='collection', help='BQ table from which to get collections in version')
    parser.add_argument('--bq_patient_table', default='patient', help='BQ table from which to get patients in version')
    parser.add_argument('--bq_study_table', default='study', help='BQ table from which to get study in version')
    parser.add_argument('--bq_series_table', default='series', help='BQ table from which to get series in version')
    parser.add_argument('--bq_excluded_collections', default='excluded_collections', help='BQ table from which to get collections to exclude')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    gen_collections_table(args)