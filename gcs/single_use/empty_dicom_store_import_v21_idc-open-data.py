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
Script to empty idc-open-idc bucket.
"""

import argparse
from gcs.empty_bucket_mp.empty_bucket_mp import del_all_instances
from utilities.logging_config import successlogger, progresslogger, errlogger


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--processes', default=64   , help="Number of concurrent processes")
    parser.add_argument('--batch', default=1000, help='Size of batch assigned to each process')

    args = parser.parse_args()

    for bucket in ['dicom_store_import_v21_idc-open-data']:
        args.bucket = bucket
        del_all_instances  (args)
