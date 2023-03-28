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

"""
Add an aws_url column to auxiliary_metadata table
"""
import settings
import argparse
import json
import time
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import NotFound
from utilities.logging_config import successlogger, progresslogger, errlogger


# args.trg_project: idc_pdp_staging
# args.trg_dataset: idc_vX
# We do a table update rather than regenerate the entire table.
# By doing it this way, we do not need the SQL for each IDC version
def add_aws_column_to_aux(args, dones):
    if f'add_aws_column_to_aux_{args.trg_dataset}' not in dones:
        client = bigquery.Client()
        table_id = f'{args.trg_project}.{args.trg_dataset}.auxiliary_metadata'
        try:
            table = client.get_table(table_id)
        except:
            exit(-1)
        # Add the aws_url column if we have not already done so
        if next((index for index, field in enumerate(table.schema) if field.name == 'aws_url'), -1) == -1:
            client = bigquery.Client()
            query = f"""
            ALTER TABLE `{args.trg_project}.{args.trg_dataset}.auxiliary_metadata`
            ADD COLUMN aws_url STRING;
            """
            job = client.query(query)
            # Wait for completion
            result = job.result()

            query = f"""
            ALTER TABLE `{args.trg_project}.{args.trg_dataset}.auxiliary_metadata`
            ALTER COLUMN aws_url 
            SET OPTIONS (
                description='URL to this object containing the current version of this instance in Amazon Web Services (AWS)'
            )
            """

            job = client.query(query)
            # Wait for completion
            result = job.result()

        successlogger.info(f'add_aws_column_to_aux_{args.trg_dataset}' )
    else:
        progresslogger.info(f'Skipping add_aws_column_to_aux_{args.trg_dataset}' )
    return
