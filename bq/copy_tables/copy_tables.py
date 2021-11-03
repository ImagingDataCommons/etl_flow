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

# Copy some set of BQ tables from one dataset to another. Used to populate public dataset
# Also used to copy table of "related" data (generally bioclin) from one version to the next.
from google.cloud import bigquery
from utilities.bq_helpers import create_BQ_dataset, copy_BQ_table
from google.api_core.exceptions import NotFound


def copy_tables(args):
    client = bigquery.Client(project=args.dst_project)
    try:
        dst_dataset = client.get_dataset(args.dst_bqdataset)
    except NotFound:
        dst_dataset = create_BQ_dataset(client, args.dst_bqdataset, args.dataset_description)
    src_dataset = client.dataset(args.src_bqdataset, args.src_project)
    # dst_dataset = client.dataset(args.dst_bqdataset, args.dst_project)
    for table in args.bqtables:
        src_table = src_dataset.table(table)
        dst_table = dst_dataset.table(table)
        result = copy_BQ_table(client, src_table, dst_table)
        print(f"Copied table {table}")

