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

# Copy all tables in datasets  idc_vX/idc_vX_pub to mitigation projects
# for some range of versions.
import settings
import argparse
import json
from utilities.logging_config import successlogger, progresslogger, errlogger
from bq.release_bq_data.publish_dataset import publish_dataset


if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version number')
    # parser.add_argument('--pub_project', default=settings.DEV_MITIGATION_PROJECT,
    #                     help='Project where public datasets live')
    parser.add_argument('--clinical_table_ids', default={}, help="Copy all tables/views unless this is non-empty")
    parser.add_argument('--range', default = [18,18], help='Range of versions over which to clone')
    args = parser.parse_args()

    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')
    args.skipped_table_ids = []
    args.table_ids = []

    for version in range(args.range[0], args.range[1]+1):

        args.src_project = settings.DEV_PROJECT
        args.trg_project = settings.DEV_MITIGATION_PROJECT # Project to which tables are copied
        if version <= 7:
            args.src_dataset = f'idc_v{version}'
            args.trg_dataset = f'idc_v{version}'
        else:
            args.src_dataset = f'idc_v{version}_pub'
            args.trg_dataset = f'idc_v{version}_pub'
        progresslogger.info(f'\nCopying {args.src_project}.{args.src_dataset} to {args.trg_project}.{args.trg_dataset}')
        publish_dataset(args, args.table_ids, copy_views=False)

    for version in range(args.range[0], args.range[1] + 1):
        args.src_project = settings.PDP_PROJECT
        args.trg_project = settings.STAGING_MITIGATION_PROJECT # Project to which tables are copied
        args.src_dataset = f'idc_v{version}'
        args.trg_dataset = f'idc_v{version}'
        progresslogger.info(f'\nCopying {args.src_project}.{args.src_dataset} to {args.trg_project}.{args.trg_dataset}')
        publish_dataset(args, args.table_ids, copy_views=False)
        pass
