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

# Generate a BQ table of metadata about each file in the idc-source-data project
import argparse
import sys
import json
import settings
from google.cloud import bigquery, storage
from base64 import b64decode
import pandas as pd
import pandas_gbq as pd_gbq

from utilities.bq_helpers import load_BQ_from_json, delete_BQ_Table
from utilities.tcia_helpers import get_all_tcia_metadata
from utilities.logging_config import progresslogger, errlogger

def gen_all_sources_table(args):

    storage_client = storage.Client(project=args.storage_project)
    buckets = storage_client.list_buckets()

    sources = []
    for bucket in buckets:
        blobs = bucket.list_blobs()
        for blob in blobs:
            try:
                md5_hash = b64decode(blob.md5_hash).hex()
            except:
                # A blob might not have an md5 hash
                md5_hash = ""
            sources.append({"name": f"{bucket.name}/{blob.name}",
                           "md5_hash": md5_hash,
                           "created": blob.time_created,
                           "updated": blob.updated})

    df = pd.DataFrame(sources)

    pd_gbq.to_gbq(df, f"{args.project}.idc_v{args.version}_dev.{args.bqtable_name}", if_exists='replace')
    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version for which to build the table')
    parser.add_argument('--project', default=f'{settings.DEV_PROJECT}')
    parser.add_argument('--storage_project', default='idc-source-data')
    parser.add_argument('--bqdataset_name', default=f'idc_v{settings.CURRENT_VERSION}_dev', help='BQ dataset name')
    parser.add_argument('--bqtable_name', default=f'idc_source_data_files', help='BQ table name')
    args = parser.parse_args()

    print("{}".format(args), file=sys.stdout)

    # args.sql = open(args.sql).read()

    gen_all_sources_table(args)
