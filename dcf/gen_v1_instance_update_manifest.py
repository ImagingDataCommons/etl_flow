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

# Regenerate the manifest of V1 instances. This is needed because V1 blob
# names have been changed to the flat address space names.
import argparse
from google.cloud import bigquery
from utilities.bq_helpers import query_BQ, export_BQ_to_GCS, delete_BQ_Table

def gen_v1_instance_manifest(args):
    BQ_client = bigquery.Client(project=args.project)
    query= f"""
        SELECT concat('dg.4DFC/',instance_uuid) as GUID, 
            instance_hash as md5, 
            instance_size as size, 
            '*' as acl, 
            concat('gs://idc-open/', instance_uuid, '.dcm') as url
        FROM {args.project}.{args.bqdataset}.{args.table}
        WHERE idc_version_number = {args.version}"""

    # Run a query that generates the manifest data
    results = query_BQ(BQ_client, args.bqdataset, args.temp_table, query, write_disposition='WRITE_TRUNCATE')

    results = export_BQ_to_GCS(BQ_client, args.bqdataset, args.temp_table, args.manifest_uri)

    while results.state == 'RUNNING':
        pass

    delete_BQ_Table(BQ_client, args.project, args.bqdataset, args.temp_table)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=1, help='Next version to generate')
    parser.add_argument('--project', default='idc-dev-etl')
    parser.add_argument('--bqdataset', default='idc_v1')
    parser.add_argument('--table', default='instance')
    parser.add_argument('--manifest_uri', default='gs://indexd_manifests/dcf_input/idc_v1_instance_update_manifest.tsv',
                        help="GCS file in which to save results")
    parser.add_argument('--temp_table', default='tmp_manifest', \
                        help='Table in which to write query results')

    args = parser.parse_args()


    gen_v1_instance_manifest(args)
