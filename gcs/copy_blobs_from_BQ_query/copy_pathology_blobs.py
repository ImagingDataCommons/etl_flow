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
import json
import os
import argparse

from copy_blobs_mp import copy_all_blobs
from pathology_collections import collection_list
from utilities.logging_config import successlogger, progresslogger, errlogger

# Copy the blobs that are new to a version from dev pre-staging buckets
# to dev staging buckets.
import settings




if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=12, help='Version to work on')
    parser.add_argument('--src_bucket', default="idc-dev-open")
    parser.add_argument('--dst_bucket', default="pathology_blobs_whc")
    parser.add_argument('--batch', default=1000)
    parser.add_argument('--processes', default=1)
    args = parser.parse_args()
    args.id = 0 # Default process ID

    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    query=f"""
    select concat(i_uuid,'.dcm') blob
    from `idc-dev-etl.idc_v{args.version}_dev.all_joined` aj
    where idc_version={args.version} and i_source='path'
    and collection_id in {tuple(collection_list)}    """

    copy_all_blobs(args, query)