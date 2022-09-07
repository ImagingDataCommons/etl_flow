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


# Single use script for v11, where the PROSTATE-DIAGNOSIS collection
# moved from exclused to open,
# Generate a manifest of uuids that are new to a version.
# The resulting manifest is intended to be submitted to
# DCF to detect if there are collisions with other uuids.

import argparse
import settings
from google.cloud import bigquery
from utilities.bq_helpers import query_BQ, export_BQ_to_GCS, delete_BQ_Table

def gen_revision_manifest(args):
    BQ_client = bigquery.Client(project=args.project)
    query= f"""
        SELECT distinct i_uuid
        FROM `{args.project}.idc_v{args.version}_dev.all_joined_included` 
        WHERE i_rev_idc_version = {args.version}
        OR collection_id = 'PROSTATE-DIAGNOSIS'
        ORDER BY i_uuid
    """

    # Run a query that generates the manifest data
    results = query_BQ(BQ_client, args.dst_bqdataset, args.temp_table, query, write_disposition='WRITE_TRUNCATE')

    # Export the resulting table to GCS
    results = export_BQ_to_GCS(BQ_client, args.dst_bqdataset, args.temp_table, args.manifest_uri)

    while results.state == 'RUNNING':
        pass

    delete_BQ_Table(BQ_client, args.project, args.dst_bqdataset, args.temp_table)

if __name__ == '__main__':
    version = settings.CURRENT_VERSION
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', default=settings.DEV_PROJECT)
    parser.add_argument('--dst_bqdataset', default=settings.BQ_DEV_INT_DATASET)
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help= 'The version of the new uuids')
    parser.add_argument('--manifest_uri', default=f'gs://indexd_manifests/dcf_input/pdp_hosting/idc_v{settings.CURRENT_VERSION}_new_uuids_*.tsv',
                        help="GCS file in which to save results")
    parser.add_argument('--temp_table', default=f'idc_v{settings.CURRENT_VERSION}_new_uuids_manifest', \
                        help='Temporary table in which to write query results')
    args = parser.parse_args()

    gen_revision_manifest(args)