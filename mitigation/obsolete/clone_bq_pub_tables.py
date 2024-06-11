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

# This script copies selected tables in idc-dev-etl datasets to idc-dev-mitigation.
import settings
import argparse
import json
from utilities.logging_config import successlogger, progresslogger, errlogger
from bq.release_bq_data.publish_dataset import publish_dataset


if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version number')
    parser.add_argument('--src_project', default=settings.DEV_PROJECT, help='Project from which tables are copied')
    parser.add_argument('--trg_project', default=settings.DEV_MITIGATION_PROJECT, help='Project to which tables are copied')
    parser.add_argument('--pub_project', default=settings.DEV_MITIGATION_PROJECT, help='Project where public datasets live')
    parser.add_argument('--clinical_table_ids', default={}, help="Copy all tables/views unless this is non-empty")
    parser.add_argument('--range', default = [1,18], help='Range of versions over which to clone')
    args = parser.parse_args()

    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    for version in range(args.range[0], args.range[1]+1):
        args.skipped_table_ids = []
        args.table_ids = []
        if version in (1,2,3,4,5,6,7):
            pub_table_ids = {
                "auxiliary_metadata": "TABLE",
                "dicom_derived_all": "TABLE",
                "dicom_metadata": "TABLE",
            }

            args.src_dataset = f'idc_v{version}'
            args.trg_dataset = f'idc_v{version}'
            publish_dataset(args, table_ids=pub_table_ids, copy_views=False)

        elif version in (8,9):
            pub_table_ids = {
                "auxiliary_metadata": "TABLE",
                "dicom_derived_all": "TABLE",
                "dicom_metadata": "TABLE",
            }
            args.src_dataset = f'idc_v{version}_pub'
            args.trg_dataset = f'idc_v{version}_pub'
            publish_dataset(args, table_ids=pub_table_ids, copy_views=False)

        elif version == 10:
            pub_table_ids = {
                "auxiliary_metadata": "TABLE",
                "dicom_all": "TABLE",
                "dicom_derived_all": "TABLE",
                "dicom_derived_all_premerge": "TABLE",
                "dicom_metadata": "TABLE",
                "dicom_metadata_curated": "TABLE",
                "measurement_groups": "TABLE",
                "qualitative_measurements": "TABLE",
                "quantitative_measurements": "TABLE",
                "segmentations": "TABLE",
            }
            args.src_dataset = f'idc_v{version}_pub'
            args.trg_dataset = f'idc_v{version}_pub'
            publish_dataset(args, table_ids=pub_table_ids, copy_views=False)

        elif version in (11,12):
            pub_table_ids = {
                "auxiliary_metadata": "TABLE",
                "dicom_all": "TABLE",
                "dicom_derived_all": "TABLE",
                "dicom_metadata": "TABLE",
                "dicom_metadata_curated": "TABLE",
                "measurement_groups": "TABLE",
                "qualitative_measurements": "TABLE",
                "quantitative_measurements": "TABLE",
                "segmentations": "TABLE",
            }
            args.src_dataset = f'idc_v{version}_pub'
            args.trg_dataset = f'idc_v{version}_pub'
            publish_dataset(args, table_ids=pub_table_ids, copy_views=False)

        elif version == 13:
            pub_table_ids = {
                "auxiliary_metadata": "TABLE",
                "dicom_all": "TABLE",
                "dicom_derived_all": "TABLE",
                "dicom_metadata": "TABLE",
                "dicom_metadata_curated": "TABLE",
                "dicom_metadata_curated_series_level": "TABLE",
                "measurement_groups": "TABLE",
                "qualitative_measurements": "TABLE",
                "quantitative_measurements": "TABLE",
                "segmentations": "TABLE",
            }
            args.src_dataset = f'idc_v{version}_pub'
            args.trg_dataset = f'idc_v{version}_pub'
            publish_dataset(args, table_ids=pub_table_ids, copy_views=False)

        elif version == 14:
            pub_table_ids = {
                "auxiliary_metadata": "TABLE",
                "dicom_all": "TABLE",
                "dicom_derived_all": "TABLE",
                "dicom_metadata": "TABLE",
                "dicom_metadata_curated": "TABLE",
                "dicom_metadata_curated_series_level": "TABLE",
                "measurement_groups": "TABLE",
                "qualitative_measurements": "TABLE",
                "quantitative_measurements": "TABLE",
                "quantitative_pivot": "TABLE",
                "segmentations": "TABLE",
            }
            args.src_dataset = f'idc_v{version}_pub'
            args.trg_dataset = f'idc_v{version}_pub'
            publish_dataset(args, table_ids=pub_table_ids, copy_views=False)

        elif version == 15:
            pub_table_ids = {
                "auxiliary_metadata": "TABLE",
                "dicom_all": "TABLE",
                "dicom_derived_all": "TABLE",
                "dicom_metadata": "TABLE",
                "dicom_metadata_curated": "TABLE",
                "dicom_metadata_curated_series_level": "TABLE",
                "dicom_pivot": "TABLE",
                "measurement_groups": "TABLE",
                "mutable_metadata": "TABLE",
                "qualitative_measurements": "TABLE",
                "quantitative_measurements": "TABLE",
                "segmentations": "TABLE",
            }
            args.src_dataset = f'idc_v{version}_pub'
            args.trg_dataset = f'idc_v{version}_pub'
            publish_dataset(args, table_ids=pub_table_ids, copy_views=False)

        elif version in (16,17,18):
            pub_table_ids = {
                "auxiliary_metadata": "TABLE",
                "dicom_all": "TABLE",
                "dicom_metadata": "TABLE",
                "dicom_metadata_curated": "TABLE",
                "dicom_metadata_curated_series_level": "TABLE",
                "dicom_pivot": "TABLE",
                "measurement_groups": "TABLE",
                "mutable_metadata": "TABLE",
                "qualitative_measurements": "TABLE",
                "quantitative_measurements": "TABLE",
                "segmentations": "TABLE",
            }
            args.src_dataset = f'idc_v{version}_pub'
            args.trg_dataset = f'idc_v{version}_pub'
            publish_dataset(args, table_ids=pub_table_ids, copy_views=False)
        else:
            errlogger.error(f'This script needs to be extended for version {version}')





