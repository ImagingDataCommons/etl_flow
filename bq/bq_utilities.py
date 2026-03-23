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

import settings
from io import StringIO
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta
import pytz
import argparse
import json5
from datetime import datetime, timedelta, timezone
import requests
import yaml


def get_data_from_comet(path, branch="current"):
    file_url = f"https://raw.githubusercontent.com/ImagingDataCommons/idc-comet/{branch}/{path}"
    headers = {
        "Authorization": f"token {settings.GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }
    response = requests.get(file_url, headers=headers)
    if response.status_code == 200:
        # Specify the local path where you want to save the file
        metadata = yaml.load(StringIO(response.text), Loader=yaml.Loader)
        return metadata["programs"]
    else:
        print(f"Failed to retrieve file. Status code: {response.status_code}")
        exit(1)

# Create a table from a data frame. The table will be deleted after the time limit expires
def create_temp_table_from_df(client, table_id, schema, df, expire_in_minutes=10):
    table = bigquery.Table(table_id)

    # Set expiration to 2 minutes from now
    expiration_duration = timedelta(minutes=expire_in_minutes)
    table.expires = datetime.now(timezone.utc) + expiration_duration
    try:
        client.create_table(table, exists_ok=True)
        # print(f"Table {table_id} created/updated with expiration at {table.expires}")
    except Exception as e:
        print(f"Error setting table metadata: {e}")
        exit(1)

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE"
    )
    # 5. Load data
    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )
    job.result()  # Wait for job to complete


# Read the file at the file path into a dataframe. The file is assumed to be JSON formatted
def read_json_to_dataframe(file_path):
    with open(file_path) as f:
        definitions = json5.load(f)
    idc_sourced_original_collections_metadata = pd.DataFrame(definitions)

    return idc_sourced_original_collections_metadata


# Create and populate a BQ table from a pandas dataframe
# If lifetime is specified, configure the table to be deleted after lifetime minutes
def dataframe_to_bq(args, df, lifetime=None):
    # Initialize the BigQuery client
    client = bigquery.Client()

    # Define the BigQuery table reference
    table_ref = f'{args.project}.{args.bq_dataset_id}.{args.table_id}'

    # Create the BigQuery table if it doesn't exist
    try:
        client.get_table(table_ref)
    except:
        table = bigquery.Table(table_ref)
        client.create_table(table)

    # Write the DataFrame data to BigQuery
    job_config = bigquery.LoadJobConfig(write_disposition='WRITE_TRUNCATE')
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    if lifetime:
        table = client.get_table(table_ref)  # Get the table object
        expiration_time = datetime.now(pytz.utc) + timedelta(minutes=lifetime)
        table.expires = expiration_time
        client.update_table(table, ["expires"])

    print('Data imported successfully!')

    return


# Create and populate a BQ table from a JSON file
# If lifetime is specified, configure the table to be deleted after lifetime minutes
def json_file_to_bq(args, file_path, lifetime=None):
    # Get the json from the specified file as a dataframe
    df = read_json_to_dataframe(file_path)

    dataframe_to_bq(args, df, lifetime=None)

    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', default='idc-dev-etl', help='BQ project')
    parser.add_argument('--bq_dataset_id', default=f'idc_v{settings.CURRENT_VERSION}_dev', help='BQ datasey')
    parser.add_argument('--table_id', default='zen', help='Table name to which to copy data')
    args = parser.parse_args()

    file_name = f'{settings.PROJECT_PATH}/bq/obsolete/{args.table_id}.json'
    r = json_file_to_bq(args, file_name, lifetime=5)