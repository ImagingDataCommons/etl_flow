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

# Generate a manifest of "large" bundles. These are bundles with >= 3000 instances.
# We did not previously register these.
import argparse
from google.cloud import bigquery
from utilities.bq_helpers import query_BQ, export_BQ_to_GCS, delete_BQ_Table

def gen_v1_large_bundle_manifest(args):
    BQ_client = bigquery.Client(project=args.project)
    query= f"""
        SELECT
          bundle_names,
          ids,
          guid
        FROM (
          SELECT
            2 AS sortby,
            CONCAT('dg.4DFC/',aux.study_uuid) AS bundle_names,
            CONCAT('[',
              STRING_AGG(DISTINCT CONCAT('dg.4DFC/',aux.series_uuid ), ',' ),
              ']') AS ids,
            concat('dg.4DCF/',study_uuid) as guid
          FROM
            `idc-dev-etl.idc_v1.auxiliary_metadata` AS aux
          WHERE
            aux.study_instances >= 3000
          GROUP BY
            aux.StudyInstanceUID,
            aux.tcia_api_collection_id,
            aux.study_uuid
          UNION ALL
          SELECT
            1 AS sortby,
            CONCAT( 'dg.4DFC/',aux.series_uuid) AS bundle_names,
            CONCAT('[',
              STRING_AGG(DISTINCT CONCAT('dg.4DFC/',aux.instance_uuid  ), ',' ),
              ']') AS ids,
            concat('dg.4DCF/',series_uuid) as guid
          FROM
            `idc-dev-etl.idc_v1.auxiliary_metadata` AS aux
          WHERE
            aux.series_instances >= 3000
          GROUP BY
            aux.SeriesInstanceUID,
            aux.tcia_api_collection_id,
            aux.series_uuid)
        ORDER BY
          sortby,
          bundle_names"""

    # Run a query that generates the manifest data
    results = query_BQ(BQ_client, args.bqdataset, args.temp_table, query, write_disposition='WRITE_TRUNCATE')

    results = export_BQ_to_GCS(BQ_client, args.bqdataset, args.temp_table, args.manifest_uri)

    delete_BQ_Table(BQ_client, args.project, args.bqdataset, args.temp_table)

    while results.state == 'RUNNING':
        pass

    delete_BQ_Table(BQ_client, args.project, args.bqdataset, args.temp_table)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=1, help='Next version to generate')
    parser.add_argument('--project', default='idc-dev-etl')
    parser.add_argument('--bqdataset', default='idc_v1')
    parser.add_argument('--table', default='instance')
    parser.add_argument('--manifest_uri', default='gs://indexd_manifests/dcf_input/idc_v1_large_bundle_manifest.tsv',
                        help="GCS file in which to save results")
    parser.add_argument('--temp_table', default='tmp_manifest', \
                        help='Table in which to write query results')

    args = parser.parse_args()


    gen_v1_large_bundle_manifest(args)
