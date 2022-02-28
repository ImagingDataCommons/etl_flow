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
# hosted by Google PDP. The script creates it directly in the
# idc_metadata dataset in idc-pdp-staging
import argparse
import sys
from google.cloud import bigquery
from utilities.bq_helpers import query_BQ


def gen_blob_table(args):
    query = f"""
    WITH
      pubs AS (
      SELECT
        DISTINCT tcia_api_collection_id
      FROM
        `{args.src_project}.{args.src_bqdataset_name}.open_collections`
      UNION ALL
      SELECT
        tcia_api_collection_id
      FROM
        `{args.src_project}.{args.src_bqdataset_name}.cr_collections`
      UNION ALL
      SELECT
        tcia_api_collection_id
      FROM
        `{args.src_project}.{args.src_bqdataset_name}.defaced_collections` )
    SELECT
      DISTINCT CONCAT(i.uuid, '.dcm') as blob_name
      FROM
        `{args.src_project}.{args.src_bqdataset_name}.version` AS v
      JOIN
        `{args.src_project}.{args.src_bqdataset_name}.version_collection` AS vc
      ON
        v.version = vc.version
      JOIN
        `{args.src_project}.{args.src_bqdataset_name}.collection` AS c
      ON
        vc.collection_uuid = c.uuid
      JOIN
        `{args.src_project}.{args.src_bqdataset_name}.collection_patient` AS cp
      ON
        c.uuid = cp.collection_uuid
      JOIN
        `{args.src_project}.{args.src_bqdataset_name}.patient` AS p
      ON
        cp.patient_uuid = p.uuid
      JOIN
        `{args.src_project}.{args.src_bqdataset_name}.patient_study` AS ps
      ON
        p.uuid = ps.patient_uuid
      JOIN
        `{args.src_project}.{args.src_bqdataset_name}.study` AS st
      ON
        ps.study_uuid = st.uuid
      JOIN
        `{args.src_project}.{args.src_bqdataset_name}.study_series` AS ss
      ON
        st.uuid = ss.study_uuid
      JOIN
        `{args.src_project}.{args.src_bqdataset_name}.series` AS se
      ON
        ss.series_uuid = se.uuid
      JOIN 
        `{args.src_project}.{args.src_bqdataset_name}.series_instance` s_i
      ON 
        se.uuid = s_i.series_uuid 
      JOIN 
        `{args.src_project}.{args.src_bqdataset_name}.instance` i
      ON 
        s_i.instance_uuid = i.uuid 
      JOIN
        pubs
      ON
        pubs.tcia_api_collection_id = c.collection_id
      WHERE
        v.version = {args.version} and i.excluded = False
      ORDER BY
        blob_name
    """

    client = bigquery.Client(project=args.dst_project)
    # query = args.sql.format(version=args.version)
    result=query_BQ(client, args.trg_bqdataset_name, args.bqtable_name, query, write_disposition='WRITE_TRUNCATE')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=8, help='IDC version for which to build the table')
    args = parser.parse_args()
    parser.add_argument('--src_project', default='idc-dev-etl')
    parser.add_argument('--dst_project', default='idc-pdp-staging')
    parser.add_argument('--src_bqdataset_name', default=f'idc_v{args.version}_dev', help='BQ dataset name')
    parser.add_argument('--trg_bqdataset_name', default=f'idc_metadata', help='BQ dataset name')
    parser.add_argument('--bqtable_name', default=f'open_collections_blob_names_v{args.version}', help='BQ table name')
    # parser.add_argument('--sql', default=f'./gen_open_collections_blob_names.sql')
    args = parser.parse_args()

    print("{}".format(args), file=sys.stdout)

    # args.sql = open(args.sql).read()

    gen_blob_table(args)


