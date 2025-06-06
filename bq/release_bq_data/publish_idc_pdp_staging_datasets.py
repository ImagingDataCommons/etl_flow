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

# Duplicate idc_vxx datasets in idc_pdp_staging to the nci-idc-bigquery-data project
import settings
import argparse
import json
from utilities.logging_config import successlogger, progresslogger, errlogger
from bq.copy_tables.copy_dataset import copy_dataset


if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version number')
    parser.add_argument('--src_project', default="idc-pdp-staging", help='Project from which tables are copied')
    parser.add_argument('--trg_project', default="nci-idc-bigquery-data", help='Project to which tables are copied')
    parser.add_argument('--pub_project', default="nci-idc-bigquery-data", help='Project where public datasets live')
    parser.add_argument('--table_ids', default={}, help="Copy all tables/views unless this is non-empty.")
    parser.add_argument('--clinical_table_ids', default={}, help="Copy all tables/views unless this is non-empty")
    args = parser.parse_args()

    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    for src_dataset, table_ids in [
                (f'idc_v{settings.CURRENT_VERSION}', args.table_ids),
                (f'idc_v{settings.CURRENT_VERSION}_clinical', args.clinical_table_ids)
            ]:
        args.src_dataset = src_dataset
        args.trg_dataset = src_dataset
        copy_dataset(args, table_ids)
