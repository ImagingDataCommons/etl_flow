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


    check_all_instances(args, query)
