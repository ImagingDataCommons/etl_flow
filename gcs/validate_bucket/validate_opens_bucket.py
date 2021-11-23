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
Multiprocess script to validate that the instances in a bucket are only
those in some set of collections
"""

import argparse
import os
import logging
from logging import INFO
rootlogger = logging.getLogger('root')
successlogger = logging.getLogger('success')
errlogger = logging.getLogger('root.err')

from gcs.validate_bucket.validate_bucket_mp import pre_validate


if __name__ == '__main__':
    version = 5

    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default = version)
    parser.add_argument('--project', default = 'idc-dev-etl')
    parser.add_argument('--bqdataset', default=f'idc_v{version}')
    parser.add_argument('--bucket', default='idc-open-pdp-staging')
    parser.add_argument('--collection_table', default='open_collections', help='BQ table containing list of collections')
    parser.add_argument('--blob_names', default='./logs/blobs.txt', help='List of blobs names in above collections')
    parser.add_argument('--processes', default=16, help="Number of concurrent processes")
    parser.add_argument('--batch', default=100, help='Size of batch assigned to each process')
    parser.add_argument('--log_dir', default=f'/mnt/disks/idc-etl/logs/validate_open_buckets')

    args = parser.parse_args()

    if not os.path.exists('{}'.format(args.log_dir)):
        os.mkdir('{}'.format(args.log_dir))

    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler(f'{os.environ["PWD"]}/logs/bucket.log')
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    successlogger = logging.getLogger('success')
    successlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')



    pre_validate(args)
