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
Multiprocess script to validate that the idc-dev-cr bucket
contains the expected set of blobs.
"""

import argparse
import json
import settings

import builtins
builtins.APPEND_PROGRESSLOGGER = True
from validate_archive import check_all_zips

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # parser.add_argument('--version', default=settings.CURRENT_VERSION)
    parser.add_argument('--version', default=20)
    parser.add_argument('--bucket', default='idc-arch-excluded')
    parser.add_argument('--dev_or_pub', default = 'dev', help='Validating a dev or pub bucket')
    parser.add_argument('--batch', default=10000, help='Size of batch assigned to each process')

    args = parser.parse_args()
    print(f'args: {json.dumps(args.__dict__, indent=2)}')
    check_all_zips(args)
