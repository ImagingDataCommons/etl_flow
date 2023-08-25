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

# Generate a list of distinct IDC-sourced subcollections.

import argparse
import json
import sys
from google.cloud import bigquery

from python_settings import settings
from time import sleep
from utilities.bq_helpers import load_BQ_from_json, query_BQ, create_BQ_table, delete_BQ_Table
from utilities.logging_config import successlogger,progresslogger
from bq.utils.gen_license_table import get_original_collection_licenses
from bq.generate_tables_and_views.auxiliary_metadata_table.schema import auxiliary_metadata_schema

def distinct_subcollections(args):
    query = f"""
SELECT
    DISTINCT collection_id,
    min(c_rev_idc_version) version,
    c_hashes.idc_hash idc_hash
  FROM
    `idc-dev-etl.idc_v{args.version}_dev.all_joined`
GROUP BY
  collection_id,
  idc_hash
HAVING
    c_hashes.idc_hash IS NOT NULL
    AND c_hashes.idc_hash != ""
ORDER BY
  collection_id,
  version
"""

    client = bigquery.Client()
    result = client.query(query)
    collections = [{'collection_id': row.collection_id, 'version': row.version, 'idc_hash': row.idc_hash} for row in result]
    return collections

if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    # parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version for which to build the table')
    parser.add_argument('--version', default=15, help='IDC version for which to build the table')
    args = parser.parse_args()

    args.access ='Public' # Fixed value
    print("{}".format(args), file=sys.stdout)

    distinct_subcollections(args)
