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
from google.cloud import bigquery
from utilities.bq_helpers import query_BQ


def gen_blob_table(args):
    client = bigquery.Client(project=args.dst_project)
    query = args.sql.format(version=args.version)
    result=query_BQ(client, args.bqdataset_name, args.bqtable_name, query, write_disposition='WRITE_TRUNCATE')