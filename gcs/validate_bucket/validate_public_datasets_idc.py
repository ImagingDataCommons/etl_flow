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

"""
Multiprocess script to validate that the public-datasets-idc bucket
contains the expected set of blobs.
"""

import argparse
import os
import settings

from gcs.validate_bucket.validate_bucket_mp import check_all_instances


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=f'{settings.CURRENT_VERSION}')
    parser.add_argument('--bucket', default='public-datasets-idc')
    parser.add_argument('--src_project', default=settings.PDP_PROJECT)
    parser.add_argument('--src_bqdataset_name', default=settings.BQ_PDP_DATASET)
    parser.add_argument('--collection_group_table', default='all_included_collections', help='BQ table containing list of collections')
    parser.add_argument('--expected_blobs', default=f'{settings.LOG_DIR}/expected_blobs.txt', help='List of blobs names expected to be in above collections')
    parser.add_argument('--found_blobs', default=f'{settings.LOG_DIR}/found_blobs.txt', help='List of blobs names found in bucket')
    parser.add_argument('--dev_or_pub', default='pub', help='Validating a dev or pub bucket')
    parser.add_argument('--batch', default=10000, help='Size of batch assigned to each process')
    parser.add_argument('--log_dir', default=f'/mnt/disks/idc-etl/logs/validate_open_buckets')
    args = parser.parse_args()

    query = f"""
    SELECT *
    FROM `{args.src_project}.idc_metadata.open_collections_blob_names_v{args.version}`
    """

    # query = f"""
    # SELECT distinct split(gcs_url,'/')[offset(3)] as blob_name
    # FROM `{args.src_project}.{args.src_bqdataset_name}.auxiliary_metadata`
    # WHERE instance_revised_idc_version = {args.version} and split(gcs_url,'/')[offset(2)] = 'public-datasets-idc'
    # UNION all
    # SELECT distinct split(gcs_url,'/')[offset(3)]  as blob_name
    # FROM `{args.src_project}.{args.src_bqdataset_name}.auxiliary_metadata`
    # WHERE idc_webapp_collection_id = 'vestibular_schwannoma_seg'
    # """
    # query = f"""
    #     SELECT distinct CONCAT(a.i_uuid, '.dcm') as blob_name
    #     FROM `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.all_joined_included` a
    #     JOIN `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.all_included_collections` i
    #     ON a.collection_id = i.tcia_api_collection_id
    #     WHERE
    #     (a.i_source='tcia' and i.pub_tcia_url='public-datasets-idc')
    #     OR (a.i_source='path' and
    #     a.collection_id = 'CPTAC-CM' and a.i_rev_idc_version=10)
    #     OR (a.i_source='path' and
    #     a.collection_id = 'CPTAC-LSCC' and a.i_rev_idc_version=10)
    #     OR (a.i_source='path' and a.collection_id != 'CPTAC-CM' and a.collection_id != 'CPTAC-LSCC')
    #     AND a.i_excluded=FALSE
    #     UNION ALL
    #     SELECT distinct CONCAT(a.i_uuid, '.dcm') as blob_name
    #     FROM `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.all_joined_included` a
    #     JOIN `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.all_included_collections` i
    #     ON a.collection_id = i.tcia_api_collection_id
    #     WHERE
    #     a.collection_id = 'Vestibular-Schwannoma-SEG'
    #
    #     """


    check_all_instances(args, query)
