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
import json
import settings

from utilities.logging_config import successlogger, progresslogger
from step1_restore_tables import restore_tables
from step2_remove_aws_column_from_dicom_all_view import remove_aws_url_column_from_dicom_all_view
from step3_remove_aws_column_from_dicom_pivot import remove_aws_url_column_from_dicom_pivot


def skip(args):
    progresslogger.info("Skipped stage")
    return


if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()

    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version number')
    parser.add_argument('--dev_project', default="idc-dev-etl", help='Project  from which to get some -dev tables/views')
    parser.add_argument('--dev_dataset', default=f"idc_v{settings.CURRENT_VERSION}_dev", help="Dataset from which to get some -dev tables/views")
    parser.add_argument('--src_project', default="idc-source-data", help='Project from which tables are copied')
    parser.add_argument('--src_prefix', default='tt_ips_')
    parser.add_argument('--trg_project', default="idc-pdp-staging", help='Project to which tables are copied')
    # parser.add_argument('--trg_project', default="idc-source-data", help='Project to which tables are copied')
    parser.add_argument('--trg_prefix', default='')
    # parser.add_argument('--dataset_prefix', default='idc_dev_etl_')
    parser.add_argument('--dev_or_pub', default='dev', help='Revising the dev or pub version of auxiliary_metadata')
    args = parser.parse_args()

    dones = set(open(f'{successlogger.handlers[0].baseFilename}').read().splitlines())
    versions = [ '1', '2', '3', '4', '5', '6', '7', '8', '9', '10',
        '11', '12', '13']
    for dataset_version in [version for version in versions if not version in dones]:
        args.dataset_version = dataset_version
        args.src_dataset = f'{args.src_prefix}idc_v{dataset_version}'
        args.trg_dataset = f'{args.trg_prefix}idc_v{dataset_version}'

        progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

        steps = [
            restore_tables,
            remove_aws_url_column_from_dicom_all_view,
            remove_aws_url_column_from_dicom_pivot
        ]
        
        for index, func in enumerate(steps):
            step = index+1
            if f'v{dataset_version}_step{step}' not in dones:
                progresslogger.info(f'Begin v{dataset_version}_step{step}')
                func(args, dones)
                successlogger.info(f'v{dataset_version}_step{step}')
            else:
                progresslogger.info(f'Skipping v{dataset_version}_step{step}')
            step += 1





        successlogger.info(dataset_version)

