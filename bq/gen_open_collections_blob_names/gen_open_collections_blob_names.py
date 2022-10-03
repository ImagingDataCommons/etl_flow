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
import settings
from google.cloud import bigquery
from utilities.bq_helpers import query_BQ


def gen_blob_table(args):
    # query = f"""
    # SELECT distinct CONCAT(a.i_uuid, '.dcm') as blob_name
    # FROM `{args.src_project}.{args.src_bqdataset_name}.all_joined_included` a
    # JOIN `{args.src_project}.{args.src_bqdataset_name}.all_included_collections` i
    # ON a.collection_id = i.tcia_api_collection_id
    # WHERE (a.i_source='tcia' and i.pub_tcia_url='public-datasets-idc')
    # OR (a.i_source='path' and i.pub_path_url='public-datasets-idc')
    # AND a.i_excluded=FALSE
    # """

    # This query is a hack to deal with V10 pathology in CPTAC-CM, -LSCC is in public-datasets-pdp
    # but previous is in idc-open-idc1
    query = f"""
        SELECT
          DISTINCT CONCAT(a.i_uuid, '.dcm') AS blob_name
        FROM
          `{args.src_project}.{args.src_bqdataset_name}.all_joined` a
        JOIN
          `{args.src_project}.{args.src_bqdataset_name}.all_collections` i
        ON
          a.collection_id = i.tcia_api_collection_id
        WHERE
          ( (a.i_source='tcia'
              AND i.pub_tcia_url='public-datasets-idc')
            OR (a.i_source='path'
              AND i.pub_path_url='public-datasets-idc') )
          AND a.i_excluded=FALSE
     """

    client = bigquery.Client(project=args.dst_project)
    result=query_BQ(client, args.trg_bqdataset_name, args.bqtable_name, query, write_disposition='WRITE_TRUNCATE')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version for which to build the table')
    parser.add_argument('--src_project', default=f'{settings.DEV_PROJECT}')
    parser.add_argument('--dst_project', default=f'{settings.PDP_PROJECT}')
    parser.add_argument('--src_bqdataset_name', default=f'idc_v{settings.CURRENT_VERSION}_dev', help='BQ dataset name')
    parser.add_argument('--trg_bqdataset_name', default=f'idc_metadata', help='BQ dataset name')
    parser.add_argument('--bqtable_name', default=f'open_collections_blob_names_v{settings.CURRENT_VERSION}', help='BQ table name')
    args = parser.parse_args()

    print("{}".format(args), file=sys.stdout)

    # args.sql = open(args.sql).read()

    gen_blob_table(args)


