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


import argparse
from gcs.copy_bucket_mp import copy_all_instances
from utilities.logging_config import successlogger, progresslogger, errlogger

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--processes', default=32, help="Number of concurrent processes")
    parser.add_argument('--batch', default=100, help='Size of batch assigned to each process')
    parser.add_argument('--log_dir', default=f'/mnt/disks/idc-etl/logs/copy_bucket_mp')

    args = parser.parse_args()

    try:
        # Create a set of previously copied blobs
        dones = set(open(successlogger.handlers[0].baseFilename).read().splitlines())
    except:
        dones = set([])


    # args.src_bucket = 'idc-open-idc1-staging'
    # args.dst_bucket = 'idc-open-idc1'
    # copy_all_instances(args, dones)
    #
    # args.src_bucket = 'idc-open-cr-staging'
    # args.dst_bucket = 'idc-open-cr'
    # copy_all_instances(args, dones)

    args.src_bucket = 'public-datasets-idc-staging'
    args.dst_bucket = 'public-datasets-idc'
    copy_all_instances(args, dones)


