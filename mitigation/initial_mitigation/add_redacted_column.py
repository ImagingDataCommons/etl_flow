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

from google.cloud import bigquery

import settings
import argparse

def add_a_column(version, table, column, type, default):
    client = bigquery.Client()

    if not next((row for row in client.get_table(f'{settings.DEV_MITIGATION_PROJECT}.idc_v{version}_dev.{table}').schema if row.name == column), 0):
        query = f"""
            ALTER TABLE `{settings.DEV_MITIGATION_PROJECT}.idc_v{version}_dev.{table}`
            ADD COLUMN {column} {type}   
            """
        result = client.query(query)
        try:
            while result.state != "DONE":
                result = client.get_job(result.job_id)
            if result.error_result != None:
                breakpoint()
        except Exception as exc:
            breakpoint()

    query = f"""
        ALTER TABLE `{settings.DEV_MITIGATION_PROJECT}.idc_v{version}_dev.{table}`
        ALTER COLUMN {column} SET DEFAULT {default}
        """
    result = client.query(query)
    try:
        while result.state != "DONE":
            result = client.get_job(result.job_id)
        if result.error_result != None:
            breakpoint()
    except Exception as exc:
        breakpoint()

    query = f"""
        UPDATE `{settings.DEV_MITIGATION_PROJECT}.idc_v{version}_dev.{table}` t
        SET {column} = {default}
        WHERE true
        """
    result = client.query(query)
    try:
        while result.state != "DONE":
            result = client.get_job(result.job_id)
        if result.error_result != None:
            breakpoint()
    except Exception as exc:
        breakpoint()

    return

def add_columns(version):
    # if version >= 13:
    #     add_a_column(version, 'idc_instance', 'redacted', "BOOLEAN", "false")
    #     add_a_column(version, 'idc_instance', 'mitigation', "STRING", "''")
    # add_a_column(version, 'instance', 'redacted', "BOOLEAN", "false")
    # add_a_column(version, 'instance', 'mitigation', "STRING", "''")
    add_a_column(version, 'instance', 'ingestion_url', "STRING", "''")

    return

if __name__ == "__main__":
    for version in range(3,19):
        add_columns(version)
        print(f'Updated {version}')

