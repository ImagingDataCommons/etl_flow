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
Multiprocess script to validate that the idc-dev-open bucket
contains the expected set of blobs.
"""

import argparse
import json
import settings
import builtins
builtins.APPEND_PROGRESSLOGGER = True
from utilities.logging_config import progresslogger

from gcs.validate_buckets.validate_bucket_mp import check_all_instances_mp


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION)
    parser.add_argument('--processes', default=64)
    parser.add_argument('--bucket', default='idc-dev-open')
    # parser.add_argument('--src_project', default=settings.DEV_PROJECT)
    parser.add_argument('--dev_or_pub', default = 'dev', help='Validating a dev or pub bucket')
    parser.add_argument('--access', default='Public', help='Public, Limited or Excluded')
    parser.add_argument('--premerge', default=False, help='True when performing prior to merging premerge  buckets')
    parser.add_argument('--expected_blobs', default=f'{settings.LOG_DIR}/expected_blobs.txt', help='List of blobs names expected to be in above collections')
    # parser.add_argument('--found_blobs', default=f'{settings.LOG_DIR}/found_blobs.txt', help='List of blobs names found in bucket')
    parser.add_argument('--found_blobs', default=f'{settings.LOG_DIR}/success.log', help='List of blobs names found in bucket')
    parser.add_argument('--batch', default=10000, help='Size of batch assigned to each process')
    parser.add_argument("--find_blobs", default=False, help='If true find blobs in bucket even if already have some')
    parser.add_argument('--log_dir', default=f'/mnt/disks/idc-etl/logs/validate_open_buckets')

    args = parser.parse_args()
    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    check_all_instances_mp(args, premerge=args.premerge)