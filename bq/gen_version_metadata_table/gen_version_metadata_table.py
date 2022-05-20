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

# This script generates the BQ version_metadata table.
import argparse
import sys
import json
from google.cloud import bigquery
import hashlib

import settings
from utilities.bq_helpers import load_BQ_from_json
from utilities.logging_config import successlogger
from bq.gen_version_metadata_table.version_metadata_schema import version_metadata_schema


# Hash a sorted list of hashes
def get_merkle_hash(hashes):
    md5 = hashlib.md5()
    hashes.sort()
    for hash in hashes:
        md5.update(hash.encode())
    return md5.hexdigest()

def version_hash(client, args, version):
    if version < 3:
        query = f"""
            SELECT distinct version_hash
            FROM `{args.src_project}.idc_v{version}.version`
            """
        version_hash = [row['version_hash'] for row in client.query(query).result()][0]
    elif version <7:
        query = f"""
            SELECT distinct collection_hash
            FROM `{args.src_project}.idc_v{version}.auxiliary_metadata`
            """
        hashes = [row['collection_hash'] for row in client.query(query).result()]
        version_hash = get_merkle_hash(hashes)
    else:
        query = f"""
            SELECT hashes.all_sources as version_hash
            FROM `{args.src_project}.idc_v{args.version}_dev.version` v
            WHERE v.version = {version}
             """
        version_hash = list(client.query(query).result())[0]['version_hash']

    return version_hash

def version_timestamp(client, args, version):
    if version < 3:
        query = f"""
            SELECT max(collection_timestamp) as max_timestamp
            FROM `{args.src_project}.idc_v{version}.collection`
            """
    elif version < 7:
        query = f"""
            SELECT max_timestamp
            FROM `{args.src_project}.idc_v{version}.version`
            """
    else:
        query = f"""
            SELECT max_timestamp
            FROM `{args.src_project}.idc_v{args.version}_dev.version`
            WHERE version = {version}
            """
    version_timestamp = [row['max_timestamp'] for row in client.query(query).result()][0].date()
    return version_timestamp


def gen_version_metadata_table(args):
    client = bigquery.Client(project=args.src_project)
    rows = []
    for version in range(1,args.version+1):
        version_data = {'idc_version': version}
        version_data['version_hash'] = version_hash(client, args, version)
        version_data['version_timestamp'] = version_timestamp(client, args, version)
        rows.append(json.dumps(version_data, default=str))
    metadata = '\n'.join(rows)
    job = load_BQ_from_json(client, args.dst_project, args.bqdataset_name, args.bqtable_name, metadata,
                            version_metadata_schema, write_disposition='WRITE_TRUNCATE')
    successlogger('version_metadata table generation completed')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='Max IDC version for which to build the table')
    parser.add_argument('--src_project', default='idc-dev-etl')
    parser.add_argument('--dst_project', default='idc-dev-etl')
    parser.add_argument('--bqdataset_name', default=f'idc_v{settings.CURRENT_VERSION}_pub', help='BQ dataset name')
    parser.add_argument('--bqtable_name', default='version_metadata', help='BQ table name')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    gen_version_metadata_table(args)
