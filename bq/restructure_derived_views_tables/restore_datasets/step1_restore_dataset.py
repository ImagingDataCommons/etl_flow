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
from google.api_core.exceptions import NotFound, BadRequest
from utilities.logging_config import successlogger, progresslogger, errlogger

def restore_table(client, args,  table_id):

    src_table_id = f'{args.src_project}.{args.src_dataset}.{table_id}'
    recovered_table_id = f'{args.trg_project}.{args.trg_dataset}.{table_id}'

    # Construct a BigQuery client object.
    client = bigquery.Client()
    job_config = bigquery.CopyJobConfig()
    job_config.write_disposition = 'WRITE_TRUNCATE'

    snapshot_epoch = int(time.time() * 1000) - (167*60*60*1000)

    # Construct the restore-from table ID using a snapshot decorator.
    snapshot_table_id = "{}@{}".format(src_table_id, snapshot_epoch)

    # Construct and run a copy job.
    job = client.copy_table(
        snapshot_table_id,
        recovered_table_id,
        # Must match the source and destination tables location.
        location="US",
        job_config=job_config,
    )  # Make an API request.

    job.result()  # Wait for the job to complete.

    progresslogger.info("Copied data from deleted table {} to {}".format(table_id, recovered_table_id)
    )

# args.src_project
# args.src_dataset
# args.trg_project
# args.trg_dataset
def restore_dataset(args, dones):
    client = bigquery.Client()

    if f'restore_dataset_{args.trg_dataset}' not in dones:

        # client = bigquery.Client(project=args.trg_project)
        # src_dataset_ref = bigquery.DatasetReference(args.src_project, args.src_dataset)
        # src_dataset = client.get_dataset(src_dataset_ref)
        progresslogger.info(f'Restoring {args.src_dataset} to {args.trg_dataset}')
        for table_id in (
                # 'auxiliary_metadata',
                # 'dicom_derived_all',
                # 'dicom_all',
                'measurement_groups',
                'qualitative_measurements',
                'quantitative_measurements',
                'segmentations'):
            # dicom_all is a view in versions 1-9
            if table_id == 'dicom_all' and int(args.dataset_version) < 10:
                progresslogger.info(f'Skipping restore_dataset_{args.trg_dataset}')
                return
            else:
                restore_table(client, args, table_id)
                successlogger.info(f'restore_dataset_{args.trg_dataset}')
    else:
        progresslogger.info(f'Skipping restore_dataset_{args.trg_dataset}')

# if __name__ == '__main__':
#     # (sys.argv)
#     parser = argparse.ArgumentParser()
#     parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version number')
#     parser.add_argument('--src_project', default="idc-dev-etl", help='Project from which tables are copied')
#     parser.add_argument('--trg_project', default="idc-source-data", help='Project to which tables are copied')
#     parser.add_argument('--trg_dataset_prefix', default=f"idc_dev_etl_", help="BQ target dataset")
#     args = parser.parse_args()
#
#     progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')
#
#     for src_dataset in (
#             # 'idc_v1',
#             # 'idc_v2',
#             # 'idc_v3',
#             'idc_v4',
#             'idc_v5',
#             'idc_v6',
#             'idc_v7',
#             'idc_v8_pub',
#             'idc_v9_pub',
#             'idc_v10_pub',
#             'idc_v11_pub',
#             'idc_v12_pub',
#             'idc_v13_pub'
#     ):
#         args.src_dataset = src_dataset
#         args.trg_dataset = args.trg_dataset_prefix + src_dataset
#         clone_dataset(args)
