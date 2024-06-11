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

# This is a single use script that opies all tables in datasets idc_vX, idc_vX_dev, idc_vX_pub to mitigation project
# for some range of versions. In the process, it splits dataset idc_v<1..7> into idc_v<1..7>_dev and idc_v<1..7>_pub
# for consistency with later versions.
# It also copies dicom_metadata to both idc_vX_dev and idc_vX_pub.
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
    parser.add_argument('--range', default = [12,12], help='Range of versions over which to clone')
    args = parser.parse_args()

    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    for version in range(args.range[0], args.range[1]+1):
        args.skipped_table_ids = []
        args.table_ids = []
        if version == 1:
            pub_table_ids = {
                            "analysis_results_metadata": "TABLE",
                            "auxiliary_metadata": "TABLE",
                            "dicom_all": "VIEW",
                            "dicom_derived_all": "TABLE",
                            "dicom_pivot_v1": "VIEW",
                            "measurement_groups": "VIEW",
                            "original_collections_metadata": "TABLE",
                            "qualitative_measurements": "VIEW",
                            "quantitative_measurements": "VIEW",
                            "segmentations": "VIEW"
            }

            args.src_dataset = f'idc_v{version}'
            args.trg_dataset = f'idc_v{version}_dev'
            progresslogger.info(f'/n/nCopying {args.src_dataset} to {args.trg_dataset}')
            publish_dataset(args, {}, pub_table_ids )

            pub_table_ids["dicom_metadata"] = "TABLE"
            args.src_dataset = f'idc_v{version}'
            args.trg_dataset = f'idc_v{version}_pub'
            progresslogger.info(f'\nCopying {args.src_dataset} to {args.trg_dataset}')
            publish_dataset(args, pub_table_ids, {})

        elif version == 2:
            pub_table_ids = {
                "analysis_results_metadata": "TABLE",
                "auxiliary_metadata": "TABLE",
                "dicom_all": "VIEW",
                "dicom_derived_all": "TABLE",
                "dicom_pivot_v2": "VIEW",
                "measurement_groups": "VIEW",
                "original_collections_metadata": "TABLE",
                "qualitative_measurements": "VIEW",
                "quantitative_measurements": "VIEW",
                "segmentations": "VIEW"
            }
            args.src_dataset = f'idc_v{version}'
            args.trg_dataset = f'idc_v{version}_dev'
            progresslogger.info(f'\n\nCopying {args.src_dataset} to {args.trg_dataset}')
            publish_dataset(args, {}, pub_table_ids )

            pub_table_ids["dicom_metadata"] = "TABLE"
            args.src_dataset = f'idc_v{version}'
            args.trg_dataset = f'idc_v{version}_pub'
            progresslogger.info(f'\nCopying {args.src_dataset} to {args.trg_dataset}')
            publish_dataset(args, pub_table_ids, {})

        elif version == 3:
            dev_table_ids = {
                "wsi_metadata": "TABLE"
            }
            pub_table_ids = {
                "analysis_results_metadata": "TABLE",
                "auxiliary_metadata": "TABLE",
                "dicom_all": "VIEW",
                "dicom_derived_all": "TABLE",
                "dicom_pivot_v3": "VIEW",
                "measurement_groups": "VIEW",
                "original_collections_metadata": "TABLE",
                "qualitative_measurements": "VIEW",
                "quantitative_measurements": "VIEW",
                "segmentations": "VIEW",
                "version_metadata": "TABLE",
            }
            args.src_dataset = f'idc_v{version}'
            args.trg_dataset = f'idc_v{version}_dev'
            progresslogger.info(f'\n\nCopying {args.src_dataset} to {args.trg_dataset}')
            # publish_dataset(args, {}, pub_table_ids )
            publish_dataset(args, dev_table_ids )

            # pub_table_ids["dicom_metadata"] = "TABLE"
            # args.src_dataset = f'idc_v{version}'
            # args.trg_dataset = f'idc_v{version}_pub'
            # progresslogger.info(f'\nCopying {args.src_dataset} to {args.trg_dataset}')
            # publish_dataset(args, pub_table_ids, {})

        elif version == 4:
            dev_table_ids = {
                "wsi_metadata": "TABLE"
            }
            pub_table_ids = {
                "analysis_results_metadata": "TABLE",
                "auxiliary_metadata": "TABLE",
                "dicom_all": "VIEW",
                "dicom_derived_all": "TABLE",
                "dicom_pivot_v4": "VIEW",
                "measurement_groups": "VIEW",
                "nlst_canc": "TABLE",
                "nlst_ctab": "TABLE",
                "nlst_ctabc": "TABLE",
                "nlst_prsn": "TABLE",
                "nlst_screen": "TABLE",
                "original_collections_metadata": "TABLE",
                "qualitative_measurements": "VIEW",
                "quantitative_measurements": "VIEW",
                "segmentations": "VIEW",
                "tcga_biospecimen_rel9": "TABLE",
                "tcga_clinical_rel9": "TABLE",
                "version_metadata": "TABLE",
            }
            args.src_dataset = f'idc_v{version}'
            args.trg_dataset = f'idc_v{version}_dev'
            progresslogger.info(f'\n\nCopying {args.src_dataset} to {args.trg_dataset}')
            # publish_dataset(args, {}, pub_table_ids )
            publish_dataset(args, dev_table_ids )

            # pub_table_ids["dicom_metadata"] = "TABLE"
            # args.src_dataset = f'idc_v{version}'
            # args.trg_dataset = f'idc_v{version}_pub'
            # progresslogger.info(f'\nCopying {args.src_dataset} to {args.trg_dataset}')
            # publish_dataset(args, pub_table_ids, {})

        elif version == 5:
            dev_table_ids = {
                "wsi_metadata": "TABLE"
            }
            pub_table_ids = {
                "analysis_results_metadata": "TABLE",
                "auxiliary_metadata": "TABLE",
                "dicom_all": "VIEW",
                "dicom_derived_all": "TABLE",
                "dicom_metadata_curated": "VIEW",
                "dicom_pivot_v5": "VIEW",
                "measurement_groups": "VIEW",
                "nlst_canc": "TABLE",
                "nlst_ctab": "TABLE",
                "nlst_ctabc": "TABLE",
                "nlst_prsn": "TABLE",
                "nlst_screen": "TABLE",
                "original_collections_metadata": "TABLE",
                "qualitative_measurements": "VIEW",
                "quantitative_measurements": "VIEW",
                "segmentations": "VIEW",
                "tcga_biospecimen_rel9": "TABLE",
                "tcga_clinical_rel9": "TABLE",
                "version_metadata": "TABLE",
            }
            args.src_dataset = f'idc_v{version}'
            args.trg_dataset = f'idc_v{version}_dev'
            progresslogger.info(f'\n\nCopying {args.src_dataset} to {args.trg_dataset}')
            # publish_dataset(args, {}, pub_table_ids )
            publish_dataset(args, dev_table_ids )

            # pub_table_ids["dicom_metadata"] = "TABLE"
            # args.src_dataset = f'idc_v{version}'
            # args.trg_dataset = f'idc_v{version}_pub'
            # progresslogger.info(f'\nCopying {args.src_dataset} to {args.trg_dataset}')
            # publish_dataset(args, pub_table_ids, {})

        elif version == 6:
            dev_table_ids = {
                "wsi_metadata": "TABLE"
            }
            pub_table_ids = {
                "analysis_results_metadata": "TABLE",
                "auxiliary_metadata": "TABLE",
                "dicom_all": "VIEW",
                "dicom_derived_all": "TABLE",
                "dicom_metadata_curated": "VIEW",
                "dicom_pivot_v6": "VIEW",
                "measurement_groups": "VIEW",
                "nlst_canc": "TABLE",
                "nlst_ctab": "TABLE",
                "nlst_ctabc": "TABLE",
                "nlst_prsn": "TABLE",
                "nlst_screen": "TABLE",
                "original_collections_metadata": "TABLE",
                "qualitative_measurements": "VIEW",
                "quantitative_measurements": "VIEW",
                "segmentations": "VIEW",
                "tcga_biospecimen_rel9": "TABLE",
                "tcga_clinical_rel9": "TABLE",
                "version_metadata": "TABLE",
            }
            args.src_dataset = f'idc_v{version}'
            args.trg_dataset = f'idc_v{version}_dev'
            progresslogger.info(f'\n\nCopying {args.src_dataset} to {args.trg_dataset}')
            # publish_dataset(args, {}, pub_table_ids )
            publish_dataset(args, dev_table_ids )

            # pub_table_ids["dicom_metadata"] = "TABLE"
            # args.src_dataset = f'idc_v{version}'
            # args.trg_dataset = f'idc_v{version}_pub'
            # progresslogger.info(f'\nCopying {args.src_dataset} to {args.trg_dataset}')
            # publish_dataset(args, pub_table_ids, {})

        elif version == 7:
            dev_table_ids = {
                "wsi_collection": "TABLE",
                "wsi_patient": "TABLE",
                "wsi_study": "TABLE",
                "wsi_series": "TABLE",
                "wsi_instance": "TABLE",
            }
            pub_table_ids = {
                "analysis_results_metadata": "TABLE",
                "auxiliary_metadata": "TABLE",
                "dicom_all": "VIEW",
                "dicom_derived_all": "TABLE",
                "dicom_metadata_curated": "VIEW",
                "dicom_pivot_v7": "VIEW",
                "measurement_groups": "VIEW",
                "nlst_canc": "TABLE",
                "nlst_ctab": "TABLE",
                "nlst_ctabc": "TABLE",
                "nlst_prsn": "TABLE",
                "nlst_screen": "TABLE",
                "original_collections_metadata": "TABLE",
                "qualitative_measurements": "VIEW",
                "quantitative_measurements": "VIEW",
                "segmentations": "VIEW",
                "tcga_biospecimen_rel9": "TABLE",
                "tcga_clinical_rel9": "TABLE",
                "version_metadata": "TABLE",
            }
            args.src_dataset = f'idc_v{version}'
            args.trg_dataset = f'idc_v{version}_dev'
            progresslogger.info(f'\n\nCopying {args.src_dataset} to {args.trg_dataset}')
            # publish_dataset(args, {}, pub_table_ids )
            publish_dataset(args, dev_table_ids )

            # pub_table_ids["dicom_metadata"] = "TABLE"
            # args.src_dataset = f'idc_v{version}'
            # args.trg_dataset = f'idc_v{version}_pub'
            # progresslogger.info(f'\nCopying {args.src_dataset} to {args.trg_dataset}')
            # publish_dataset(args, pub_table_ids, {})

        elif version == 8:
            dev_table_ids = {
                "wsi_collection": "TABLE",
                "wsi_patient": "TABLE",
                "wsi_study": "TABLE",
                "wsi_series": "TABLE",
                "wsi_instance": "TABLE",
            }
            args.src_dataset = f'idc_v{version}_dev'
            args.trg_dataset = f'idc_v{version}_dev'
            progresslogger.info(f'\n\nCopying {args.src_dataset} to {args.trg_dataset}')
            publish_dataset(args, dev_table_ids )

        elif version == 9:
            dev_table_ids = {
                "wsi_collection": "TABLE",
                "wsi_patient": "TABLE",
                "wsi_study": "TABLE",
                "wsi_series": "TABLE",
                "wsi_instance": "TABLE",
            }
            args.src_dataset = f'idc_v{version}_dev'
            args.trg_dataset = f'idc_v{version}_dev'
            progresslogger.info(f'\n\nCopying {args.src_dataset} to {args.trg_dataset}')
            publish_dataset(args, dev_table_ids )

        elif version == 10:
            dev_table_ids = {
                "wsi_collection": "TABLE",
                "wsi_patient": "TABLE",
                "wsi_study": "TABLE",
                "wsi_series": "TABLE",
                "wsi_instance": "TABLE",
            }
            args.src_dataset = f'idc_v{version}_dev'
            args.trg_dataset = f'idc_v{version}_dev'
            progresslogger.info(f'\n\nCopying {args.src_dataset} to {args.trg_dataset}')
            publish_dataset(args, dev_table_ids )

        elif version == 11:
            dev_table_ids = {
                "wsi_collection": "TABLE",
                "wsi_patient": "TABLE",
                "wsi_study": "TABLE",
                "wsi_series": "TABLE",
                "wsi_instance": "TABLE",
            }
            args.src_dataset = f'idc_v{version}_dev'
            args.trg_dataset = f'idc_v{version}_dev'
            progresslogger.info(f'\n\nCopying {args.src_dataset} to {args.trg_dataset}')
            publish_dataset(args, dev_table_ids )

        elif version == 12:
            dev_table_ids = {
                "wsi_collection": "TABLE",
                "wsi_patient": "TABLE",
                "wsi_study": "TABLE",
                "wsi_series": "TABLE",
                "wsi_instance": "TABLE",
            }
            args.src_dataset = f'idc_v{version}_dev'
            args.trg_dataset = f'idc_v{version}_dev'
            progresslogger.info(f'\n\nCopying {args.src_dataset} to {args.trg_dataset}')
            publish_dataset(args, dev_table_ids )

        else:

            args.src_dataset = f'idc_v{version}_dev'
            args.trg_dataset = f'idc_v{version}_dev'
            progresslogger.info(f'\n\nCopying {args.src_dataset} to {args.trg_dataset}')
            publish_dataset(args, args.table_ids)

            args.src_dataset = f'idc_v{version}_pub'
            args.trg_dataset = f'idc_v{version}_pub'
            progresslogger.info(f'\nCopying {args.src_dataset} to {args.trg_dataset}')
            publish_dataset(args, args.table_ids)

            table_ids =  {"dicom_metadata": "TABLE"}
            args.table_ids = table_ids
            args.src_dataset = f'idc_v{version}_pub'
            args.trg_dataset = f'idc_v{version}_dev'
            progresslogger.info(f'\nCopying {args.src_dataset}.dicom_metadata to {args.trg_dataset}.dicom_metadata')
            publish_dataset(args, args.table_ids)