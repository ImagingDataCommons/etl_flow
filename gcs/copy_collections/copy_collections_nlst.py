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
Copy all blobs named in some collection from the dev bucket to some other bucket.
This is/was used, among other things, for the initial population of the idc_gch_staging
bucket from which Google Healthcare ingests our data.
"""

import argparse
import os
from subprocess import run, PIPE
import logging
from logging import INFO
import time
from datetime import timedelta
from multiprocessing import Process, Queue
from queue import Empty
from google.cloud import storage


from python_settings import settings
import settings as etl_settings

# settings.configure(etl_settings)
# assert settings.configured
# import psycopg2
# from psycopg2.extras import DictCursor
from gcs.copy_collections.copy_collections import precopy


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=4, help='Next version to generate')
    args = parser.parse_args()
    parser.add_argument('--db', default=f'idc_v{args.version}')
    parser.add_argument('--src_bucket', default='idc_v5_nlst')
    parser.add_argument('--dst_bucket', default='idc_dev')
    parser.add_argument('--processes', default=96, help="Number of concurrent processes")
    parser.add_argument('--src_project', default='idc-dev-etl')
    parser.add_argument('--dst_project', default='idc-dev-etl')
    parser.add_argument('--log_dir', default='/mnt/disks/idc-etl/logs/copy_collections')
    parser.add_argument('--collection_list', default='./collection_list.txt')
    parser.add_argument('--dones', default='./logs/dones.txt')

    args = parser.parse_args()

    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/copy_collections.log'.format(os.environ['PWD']))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    successlogger = logging.getLogger('success')
    successlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')

    precopy(args)

