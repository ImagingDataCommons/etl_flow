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
from step2_revise_derived_views_tables import revise_derived_views_tables
from step3_add_aws_column_to_aux import add_aws_column_to_aux
from step7_add_aws_column_to_dicom_derived_all import add_aws_url_column_to_dicom_derived_all
from step4_add_aws_column_to_dicom_all import add_aws_url_column_to_dicom_all
from step8_add_aws_column_to_dicom_pivot import add_aws_url_column_to_dicom_pivot
from step5_populate_urls_in_auxiliary_metadata import populate_urls_in_auxiliary_metadata
from step6_populate_urls_in_dicom_all import populate_urls_in_dicom_all
from step9_populate_urls_in_dicom_derived_all import revise_dicom_derived_all_urls
from utilities.logging_config import successlogger, progresslogger




if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()

    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version number')
    parser.add_argument('--dev_project', default="idc-dev-etl", help='Project from which tables are copied')
    parser.add_argument('--src_project', default="idc-pdp-staging", help='Project from which tables are copied')
    parser.add_argument('--trg_project', default="idc-source-data", help='Project to which tables are copied')
    # parser.add_argument('--dataset', default=f"idc_v{settings.CURRENT_VERSION}_pub", help="BQ dataset")
    parser.add_argument('--dataset_prefix', default='whc_pdp_')
    parser.add_argument('--dev_dataset', default=f"idc_v{settings.CURRENT_VERSION}_dev", help="Were to get some -dev tables/vies")
    parser.add_argument('--dev_or_pub', default='pub', help='Revising the dev or pub version of auxiliary_metadata')
    args = parser.parse_args()

    dones = set(open(f'{successlogger.handlers[0].baseFilename}').read().splitlines())
    # versions = [ '1', '2', '3', '4', '5', '6', '7', '8_pub', '9_pub', '10_pub',
    #     '11_pub', '12_pub', '13_pub']
    versions = [ '1', '2', '3', '4', '5', '6', '7', '8', '9', '10',
        '11', '12', '13']
    for dataset_version in [version for version in versions if not version in dones]:
        args.dataset_version = dataset_version
        if args.dev_or_pub == 'dev':
            args.src_dataset = f'idc_v{dataset_version}' if int(dataset_version) <=7 else f'idc_v{dataset_version}_pub'
        else:
            args.src_dataset = f'idc_v{dataset_version}'

        args.trg_dataset = f'{args.dataset_prefix}{args.src_dataset}'

        progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

        steps = [
            clone_dataset, # 1
            revise_derived_views_tables, # 2
            add_aws_column_to_aux, # 3
            add_aws_url_column_to_dicom_all, # 4
            populate_urls_in_auxiliary_metadata, # 5
            populate_urls_in_dicom_all, # 6
            add_aws_url_column_to_dicom_derived_all, # 7
            add_aws_url_column_to_dicom_pivot,
            revise_dicom_derived_all_urls
        ]
        
        for index, func in enumerate(steps):
            step = index+1
            if f'v{dataset_version}_step{step}' not in dones:
                progresslogger.info(f'Begin v{dataset_version}_step{step}')
                func(args)
                successlogger.info(f'v{dataset_version}_step{step}')
            else:
                progresslogger.info(f'Skipping v{dataset_version}_step{step}')
            step += 1





        successlogger.info(dataset_version)

