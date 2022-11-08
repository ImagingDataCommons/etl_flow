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


import os
import argparse

import settings
from collection_list_crdcobj_nlst import collection_list
from copy_and_rename_instances_crdcobj_nlst import copy_all_blobs

# Copy the blobs that are new to a version from dev pre-staging buckets
# to dev staging buckets.
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=12, help='Version to work on')
    parser.add_argument('--collections', default=tuple(c for c in collection_list))
    parser.add_argument('--src_bucket', default='idc-dev-open', help='Bucket from which to copy blobs')
    parser.add_argument('--dst_bucket', default='crdcobj', help='Bucket into which to copy blobs')
    parser.add_argument('--batch', default=100)
    parser.add_argument('--processes', default=16)
    args = parser.parse_args()
    args.id = 0 # Default process ID

    # if not os.path.exists('{}'.format(args.log_dir)):
    #     os.mkdir('{}'.format(args.log_dir))

    copy_all_blobs(args)
