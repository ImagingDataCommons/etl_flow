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


# Generate a manifest of revised manifest URLs for some set of versions of instances.
# That is we include instances having a rev_idc_version in some set of IDC versions

import argparse
from google.cloud import bigquery
from utilities.bq_helpers import query_BQ, export_BQ_to_GCS, delete_BQ_Table

def gen_revision_manifest(args):
    BQ_client = bigquery.Client(project=args.project)
    query= f"""
        with collection_urls as (
            SELECT tcia_api_collection_id, pub_url as gcs_url
            FROM `idc-dev-etl.{args.bqdataset}.open_collections`
            UNION ALL
            SELECT tcia_api_collection_id, pub_url as gcs_url
            FROM `idc-dev-etl.{args.bqdataset}.cr_collections`
            UNION ALL
            SELECT tcia_api_collection_id, pub_url as gcs_url
            FROM `idc-dev-etl.{args.bqdataset}.redacted_collections`
            UNION ALL
            SELECT tcia_api_collection_id, pub_url as gcs_url
            FROM `idc-dev-etl.{args.bqdataset}.defaced_collections`)
        SELECT concat('dg.4DFC/', a.instance_uuid) as GUID, 
            a.instance_hash as md5, 
            a.instance_size as size, 
            '*' as acl, 
            CONCAT('gs://', u.gcs_url, '/', a.instance_uuid, '.dcm') as url
        FROM `idc-dev-etl.{args.bqdataset}.auxiliary_metadata` as a
        JOIN collection_urls AS u
        ON a.tcia_api_collection_id = u.tcia_api_collection_id
        WHERE a.instance_revised_idc_version in {args.versions}

        UNION ALL 
        SELECT concat('dg.4DFC/', r.instance_uuid) as GUID, 
            r.hash as md5, 
            r.size as size, 
            '*' as acl, 
            CONCAT('gs://', u.gcs_url, '/', r.instance_uuid, '.dcm') as url
        FROM `idc-dev-etl.{args.bqdataset}.retired` AS r
        JOIN collection_urls AS u
        ON r.collection_id = u.tcia_api_collection_id
        WHERE r.rev_idc_version in {args.versions}
    """

    # Run a query that generates the manifest data
    results = query_BQ(BQ_client, args.bqdataset, args.temp_table, query, write_disposition='WRITE_TRUNCATE')

    results = export_BQ_to_GCS(BQ_client, args.bqdataset, args.temp_table, args.manifest_uri)

    while results.state == 'RUNNING':
        pass

    delete_BQ_Table(BQ_client, args.project, args.bqdataset, args.temp_table)


