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
General purpose multiprocessing routine to copy the entire contents
of a bucket to another bucket.
Used to duplicate dev buckets such as idc-dev-open, idc-dev-cr, etc.
that hold all IDC data across all versions (not just the current version)
to open/public buckets.
"""

import argparse
import os

from utilities.logging_config import successlogger, progresslogger, errlogger
import time
from multiprocessing import Process, Queue
from google.cloud import storage, bigquery
from google.api_core.exceptions import ServiceUnavailable, GoogleAPICallError
from copy_bucket_mp import copy_all_instances
from utilities.logging_config import successlogger, progresslogger, errlogger

from python_settings import settings
import settings as etl_settings

if not settings.configured:
    settings.configure(etl_settings)
assert settings.configured

TRIES = 3


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--src_project', default='idc-dev-etl')
    parser.add_argument('--src_bucket', default='idc_v19_idc_ccdi_mci')
    parser.add_argument('--dst_project', default='idc-pdp-staging')
    parser.add_argument('--dst_bucket', default=f'public-datasets-idc')
    parser.add_argument('--processes', default=16, help="Number of concurrent processes")
    parser.add_argument('--batch', default=100, help='Size of batch assigned to each process')

    args = parser.parse_args()

    dones = set(open(successlogger.handlers[0].baseFilename).read().splitlines())

    copy_all_instances(args, dones)
