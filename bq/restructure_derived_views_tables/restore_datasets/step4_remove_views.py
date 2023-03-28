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

# Duplicate a BQ dataset, including views.
import settings
import argparse
import json
import time
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import NotFound
from utilities.logging_config import successlogger, progresslogger, errlogger

def delete_view(target_client, target_project, target_dataset, target_table):

    dataset_ref = bigquery.DatasetReference(target_project, target_dataset)
    # dataset_ref = target_client.dataset(target_dataset)
    dataset = target_client.get_dataset(dataset_ref)

    table_id = '{}.{}.{}'.format(target_project, dataset.dataset_id, target_table)
    progresslogger.info("Deleting {}".format(table_id))
    target_client.delete_table(table_id)

    return True


# args.trg_project: idc_pdp_staging
# args.trg_dataset: idc_vX
def remove_views(args, dones):
    if f'remove_views_{args.trg_dataset}' not in dones:
        client = bigquery.Client()
        # idc_v13 has both tables and views so we don't delete the xxx_view
        if args.dataset_version >= 12:
            progresslogger.info(f'step3_remove_views skipping version {args.dataset_version}')

        table_ids = {table.table_id: table.table_type for table in client.list_tables(f'{args.trg_project}.{args.trg_dataset}')}
        for table_id in [
            'dicom_all_view',
            'dicom_metadata_curated_view'
            'measurement_groups_view',
            'qualitative_measurements_view',
            'quantitative_measurements_view',
            'segmentations_view']:
            if table_id in table_ids:
                if table_ids[table_id] == 'VIEW':
                    delete_view(client, args.trg_project, args.trg_dataset,  table_id)
        successlogger.info(f'remove_views_{args.trg_dataset}')
    else:
        progresslogger.info(f'Skipping remove_views_{args.trg_dataset}')
    return

# if __name__ == '__main__':
#     # (sys.argv)
#     parser = argparse.ArgumentParser()
#
#     parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version number')
#     parser.add_argument('--src_project', default="idc-dev-etl", help='Project from which tables are copied')
#     parser.add_argument('--trg_project', default="idc-source-data", help='Project to which tables are copied')
#     # parser.add_argument('--dataset', default=f"idc_v{settings.CURRENT_VERSION}_pub", help="BQ dataset")
#     parser.add_argument('--src_dataset', default=f"idc_v5", help="BQ source dataset")
#     parser.add_argument('--trg_dataset', default=f"whc_dev_idc_v5", help="BQ target dataset")
#     parser.add_argument('--dataset_prefix', default='whc_dev_', help='Prefix added to target datasets')
#     parser.add_argument('--trg-version', default='', help='Dataset version to be cloned')
#     args = parser.parse_args()
#
#     progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')
#
#
#     for version in (
#             'idc_v1',
#             'idc_v5',
#             'idc_v12_pub',
#             'idc_v13_pub'
#     ):
#         args.src_dataset = version
#         clone_dataset(args)
