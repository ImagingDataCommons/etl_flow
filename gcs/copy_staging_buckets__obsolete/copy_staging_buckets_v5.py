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

# Copy some set of BQ tables from one dataset to another. Used to populate public dataset
import argparse
import sys
from gcs.copy_staging_buckets__obsolete.copy_staging_buckets import copy_buckets
import logging

if __name__ == '__main__':

    parser =argparse.ArgumentParser()
    parser.add_argument('--version', default=5, help='IDC version for which to build the table')
    args = parser.parse_args()
    parser.add_argument('--db', default=f'idc_v{args.version}', help='Database to access')
    parser.add_argument('--src_bucket_prefix', default=f'idc_v{args.version}_')
    parser.add_argument('--dst_bucket', default=f'idc_dev', help='Destination BQ dataset')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)

    rootlogger = logging.getLogger('root')
    errlogger = logging.getLogger('root.err')

    copy_buckets(args)