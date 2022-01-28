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
Copy all blobs in idc-dev-open to a dicomstore staging bucket.
This was used, among other things, for the initial population of the idc-dev-open
bucket.
"""

import argparse
import os
import logging
from logging import INFO

from gcs.obsolete.populate_buckets_with_collections.populate_bucket_with_collections import precopy


if __name__ == '__main__':
    group = 'open'
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=5, help='Next version to generate')
    args = parser.parse_args()
    parser.add_argument('--db', default=f'idc_v{args.version}')
    parser.add_argument('--bqdataset_name', default=f'idc_v{args.version}')
    parser.add_argument('--bq_collections_table', default=f'{group}_collections', help='Table listing collections in group')
    parser.add_argument('--retired', default=False, help="Copy retired instances in collection if True")
    parser.add_argument('--src_bucket', default='idc-dev-open')
    parser.add_argument('--dst_bucket', default=f'idc-dev-v{args.version}-dicomstore-staging')
    parser.add_argument('--excluded_tables', default=[
        'excluded_collections',
        'cr_collections',
        'redacted_collections'
        ], help="Tables of lists of collections in other buckets to be excluded"
    )
    parser.add_argument('--processes', default=128, help="Number of concurrent processes")
    parser.add_argument('--batch', default=1000, help='Size of batch assigned to each process')
    parser.add_argument('--src_project', default='idc-dev-etl')
    parser.add_argument('--dst_project', default='idc-dev-etl')
    parser.add_argument('--log_dir', default=f'/mnt/disks/idc-etl/logs/populate_v{args.version}_dicomstore_staging'
                                             f'')
    # parser.add_argument('--collection_list', default='./collection_list.txt')
    parser.add_argument('--dones', default=f'./logs/populate_v{args.version}_dicomstore_staging_dones.txt')

    args = parser.parse_args()

    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler(f'{os.environ["PWD"]}/logs/{group}_buckets.log')
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    successlogger = logging.getLogger('success')
    successlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')

    precopy(args)

