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

# Copy an idc_dev_etl.idc_vX_dev to another version
import argparse
import json
import settings
from copy_dataset import copy_dataset
from utilities.logging_config import successlogger, progresslogger, errlogger

# (sys.argv)
parser = argparse.ArgumentParser()
parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version number')
parser.add_argument('--src_project', default="idc-dev-etl", help='Project from which tables are copied')
parser.add_argument('--trg_project', default="idc-dev-etl", help='Project to which tables are copied')
parser.add_argument('--pub_project', default="idc-dev-etl", help='Project where public datasets live')
parser.add_argument('--table_ids', default={},
                    help="Copy all tables/views unless this is non-empty.")
parser.add_argument('--clinical_table_ids', default={}, help="Copy all tables/views unless this is non-empty")
args = parser.parse_args()

progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')


args.src_dataset = f'idc_v{settings.CURRENT_VERSION-1}_dev'
args.trg_dataset = f'idc_v{settings.CURRENT_VERSION}_dev'
copy_dataset(args, args.table_ids)
