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
from step1_clone_dataset import clone_dataset
from step2_revise_derived_views_tables import revise_derived_tables
from step3_add_aws_column_to_aux import add_aws_url_column_to_auxiliary_metadata_table
from step4_add_aws_column_to_dicom_all import add_aws_url_column_to_dicom_all
from step5_populate_urls_in_auxiliary_metadata import revise_auxiliary_metadata_gcs_urls
from step6_populate_urls_in_dicom_all import revise_dicom_all_gcs_urls
from utilities.logging_config import successlogger, progresslogger




if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()

    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version number')
    parser.add_argument('--project', default="idc-dev-etl", help='Project in which tables live')
    # parser.add_argument('--dataset', default=f"idc_v{settings.CURRENT_VERSION}_pub", help="BQ dataset")
    parser.add_argument('--dataset_prefix', default='whc_dev_')
    parser.add_argument('--dev_dataset', default=f"idc_v{settings.CURRENT_VERSION}_dev", help="BQ source dataset")
    parser.add_argument('--dev_or_pub', default='dev', help='Revising the dev or pub version of auxiliary_metadata')
    args = parser.parse_args()

    dones = set(open(f'{successlogger.handlers[0].baseFilename}').read().splitlines())
    versions = [ '1', '2', '3', '4', '5', '6', '7', '8_pub', '9_pub', '10_pub',
        '11_pub', '12_pub', '13_pub']
    for trg_version in [version for version in versions if not version in dones]:
        args.src_dataset = f'idc_v{trg_version}'
        args.trg_dataset = f'{args.dataset_prefix}{args.src_dataset}'

        progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

        # Step 1
        if f'step1_{trg_version}' not in dones:
            progresslogger.info(f'Begin step1_{trg_version}')
            clone_dataset(args)
            successlogger.info(f'step1_{trg_version}')
        else:
            progresslogger.info(f'Skipping step1_{trg_version}')

        # Step 2
        if f'step2_{trg_version}' not in dones:
            progresslogger.info(f'Begin step2_{trg_version}')
            revise_derived_tables(args)
            successlogger.info(f'step2_{trg_version}')
        else:
            progresslogger.info(f'Skipping step2_{trg_version}')

        # Step3
        if f'step3_{trg_version}' not in dones:
            progresslogger.info(f'Begin step3_{trg_version}')
            add_aws_url_column_to_auxiliary_metadata_table(args)
            successlogger.info(f'step3_{trg_version}')
        else:
            progresslogger.info(f'Skipping step3_{trg_version}')

        # Step 4
        if f'step4_{trg_version}' not in dones:
            progresslogger.info(f'Begin step4_{trg_version}')
            add_aws_url_column_to_dicom_all(args)
            successlogger.info(f'step4_{trg_version}')
        else:
            progresslogger.info(f'Skipping step4_{trg_version}')

        # Step 5
        if f'step5_{trg_version}' not in dones:
            progresslogger.info(f'Begin step5_{trg_version}')
            revise_auxiliary_metadata_gcs_urls(args)
            successlogger.info(f'step5_{trg_version}')
        else:
            progresslogger.info(f'Skipping step5_{trg_version}')

        # Step 6
        if f'step6_{trg_version}' not in dones:
            progresslogger.info(f'Begin step6_{trg_version}')
            revise_dicom_all_gcs_urls(args)
            successlogger.info(f'step6_{trg_version}')
        else:
            progresslogger.info(f'Skipping step6_{trg_version}')

        successlogger.info(trg_version)

