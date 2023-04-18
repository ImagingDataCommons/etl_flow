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

# One time script to copy all APOLLOxx blobs from idc-dev-redacted to idc-dev-open
import json
import os
import argparse

from copy_blobs_mp import copy_all_blobs
from utilities.logging_config import successlogger, progresslogger, errlogger

# Copy the blobs that are new to a version from dev pre-staging buckets
# to dev staging buckets.
import settings


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='Version to work on')
    parser.add_argument('--src_bucket', default="idc-dev-redacted")
    parser.add_argument('--dst_bucket', default="idc-dev-open")
    parser.add_argument('--batch', default=1000)
    parser.add_argument('--processes', default=1)
    args = parser.parse_args()
    args.id = 0 # Default process ID

    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    query=f"""
    SELECT DISTINCT concat(i_uuid,'.dcm') blob
    FROM `idc-dev-etl.idc_v{args.version}_dev.all_joined` aj
    WHERE collection_id LIKE 'APOLLO%'
    """

    copy_all_blobs(args, query)