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

def gen_bundle_manifest(args):
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
            `idc-dev-etl.idc_v{args.version}.auxiliary_metadata` AS aux
          WHERE study_revised_idc_version = {args.version}
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
            `idc-dev-etl.idc_v{args.version}.auxiliary_metadata` AS aux
          WHERE series_revised_idc_version = {args.version}
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


