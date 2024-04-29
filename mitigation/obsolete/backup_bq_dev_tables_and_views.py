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

# This script copies selected tables and views in a specified dataset, idc-dev-etl.idc_vX_dev, to idc-dev-mitigation
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
    args = parser.parse_args()

    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    args.skipped_table_ids = []
    args.table_ids = []
    dev_table_ids = {
        # "all_collections": "TABLE",
        # "version": "TABLE",
        # "version_collection": "TABLE",
        # "collection": "TABLE",
        # "collection_patient": "TABLE",
        # "patient": "TABLE",
        # "patient_study": "TABLE",
        # "study": "TABLE",
        # "study_series": "TABLE",
        # "series": "TABLE",
        # "series_instance": "TABLE",
        # "instance": "TABLE",
        "idc_collection": "TABLE",
        "idc_patient": "TABLE",
        "idc_study": "TABLE",
        "idc_series": "TABLE",
        "idc_instance": "TABLE",
    }
    args.src_dataset = f'idc_v{args.version}_dev'
    args.trg_dataset = f'idc_v{args.version}_dev'
    publish_dataset(args, table_ids=dev_table_ids, copy_views=False)

    dev_view_ids = {
        "idc_all_joined": "VIEW",
        # "all_joined": "VIEW",
        # "all_joined_excluded": "VIEW",
        # "all_joined_limited": "VIEW",
        # "all_joined_public": "VIEW",
        # "all_joined_public_and_current": "VIEW",
    }
    args.src_dataset = f'idc_v{args.version}_dev'
    args.trg_dataset = f'idc_v{args.version}_dev'
    publish_dataset(args, table_ids=dev_view_ids, copy_views=True)







