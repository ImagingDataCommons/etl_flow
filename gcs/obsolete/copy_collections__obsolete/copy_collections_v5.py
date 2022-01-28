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
Copy all blobs named in some collections from the dev bucket to some other bucket.
This is/was used, among other things, for the initial population of the idc_gch_staging
bucket from which Google Healthcare ingests our data.
"""

import argparse
import os
from subprocess import run, PIPE
import logging
from logging import INFO

from gcs.copy_collections__obsolete.copy_collections_bq import precopy


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=5, help='Next version to generate')
    args = parser.parse_args()
    parser.add_argument('--db', default=f'idc_v{args.version}')
    parser.add_argument('--bqdataset_name', default=f'idc_v{args.version}')
    parser.add_argument('--bq_collection_table', default='collection')
    parser.add_argument('--bq_excluded_collections', default='excluded_collections')
    parser.add_argument('--src_bucket', default='idc_dev')
    parser.add_argument('--dst_bucket', default=f'idc_dev_v{args.version}_dicomstore_staging')
    parser.add_argument('--processes', default=96, help="Number of concurrent processes")
    parser.add_argument('--batch', default=1000, help='Size of batch assigned to each process')
    parser.add_argument('--src_project', default='idc-dev-etl')
    parser.add_argument('--dst_project', default='idc-dev-etl')
    parser.add_argument('--log_dir', default='/mnt/disks/idc-etl/logs/copy_collections_v5_dicomstore_staging')
    # parser.add_argument('--collection_list', default='./collection_list.txt')
    parser.add_argument('--dones', default=f'./logs/copy_collections_v{args.version}_dicomstore_staging_dones.txt')

    args = parser.parse_args()

    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/copy_collections__obsolete.log'.format(os.environ['PWD']))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    successlogger = logging.getLogger('success')
    successlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')

    precopy(args)

