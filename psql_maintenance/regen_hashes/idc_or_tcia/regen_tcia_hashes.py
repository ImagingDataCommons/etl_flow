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

# Update how hashes.all_hash is generate:
# For series, it is the hash of child instance hashes.
# For higher level objects it is the hash of child
# hashes.all_hashes.

import os
import argparse
from idc.models import instance_source
import logging
import time
from logging import INFO

from idc.models import Base, Version, Collection, Patient, Study, Series
import settings as etl_settings
from python_settings import settings
from google.cloud import storage
import settings
from regen_tcia_or_idc_hashes import update_all_hashes

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='Version to work on')
    parser.add_argument('--client', default=storage.Client())
    args = parser.parse_args()
    parser.add_argument('--db', default=f'idc_vx', help='Database on which to operate')
    parser.add_argument('--processes', default=16, help="Number of concurrent processes")
    parser.add_argument('--source', default=instance_source.tcia, help='Channel in hashes struct; 0==tcia, 1==idc')
    args = parser.parse_args()
    args.id = 0 # Default process ID

    update_all_hashes(args)