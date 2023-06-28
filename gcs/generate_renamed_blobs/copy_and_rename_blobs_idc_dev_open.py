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

# Copy blobs, in idc-dev-open, having flat names to blobs having hierarchical names

import settings
import json
import argparse
from utilities.logging_config import successlogger, progresslogger, errlogger
from copy_and_rename_blobs_pub import copy_all_blobs


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='Version to work on')
    parser.add_argument('--dones_table_id', default='idc-dev-etl.whc_dev.idc_dev_open_dones', help='BQ table into which to import dones')
    parser.add_argument('--project', default=settings.DEV_PROJECT)
    parser.add_argument('--dataset', default=f'idc_v{settings.CURRENT_VERSION}_dev')
    parser.add_argument('--bucket', default='idc-dev-open', help='Bucket whose blobs are to be copied')
    parser.add_argument('--batch', default=1000)
    parser.add_argument('--processes', default=128)
    args = parser.parse_args()
    args.id = 0 # Default process ID

    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    copy_all_blobs(args)