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

# Copy a table in some range of per-version dataset to another project

import settings
import argparse
import json
import time
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import NotFound, BadRequest
from utilities.logging_config import successlogger, progresslogger, errlogger

def copy_table(client, args,  table_id, version):

    src_table_id = f'{args.src_project}.idc_v{version}.{table_id}'
    trg_table_id = f'{args.trg_project}.idc_v{version}.{table_id}'

    # Construct a BigQuery client object.
    client = bigquery.Client()
    job_config = bigquery.CopyJobConfig()
    job_config.operation_type = 'COPY'
    job_config.write_disposition = 'WRITE_TRUNCATE'

    # Construct and run a copy job.
    job = client.copy_table(
        src_table_id,
        trg_table_id,
        # Must match the source and destination tables location.
        location="US",
        job_config=job_config,
    )  # Make an API request.

    job.result()  # Wait for the job to complete.

    successlogger.info(f'{trg_table_id}')

    progresslogger.info("Copied table {} to {}".format(src_table_id, trg_table_id)
    )


def publish_tables(args, table_name, min_version, max_version, dones):
    client = bigquery.Client()
    for version in range(min_version, max_version+1):
        if f'{args.trg_project}.idc_v{version}.{table_name}' not in dones:
            copy_table(client, args, table_name, version)
        else:
            progresslogger.info(f'{args.trg_project}.idc_v{version}.{table_name} previously copied')
