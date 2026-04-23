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
        return metadata
    else:
        print(f"Failed to retrieve file. Status code: {response.status_code}")
        return ""


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


# This script compares two versions of a table, the current version and the previous version
# It assumes that all columns in the previous version are also in the current version. This will be a problem in
# the case that a column is dropped.
# The results are return as a dataframe.
def compare_versioned_bq_tables(join_on, table_name):
    client = bigquery.Client()

    query = f"""
DECLARE schema_comparison_query STRING;

-- Step 1: Generate the list of columns to compare
SET schema_comparison_query = (
  SELECT
    format(\"""
      SELECT 
        a.{join_on},
        %s
      FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_PREV_EXT_DATASET}.{table_name}` AS a
      FULL OUTER JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_EXT_DATASET}.{table_name}` AS b
        ON LOWER(a.{join_on}) = LOWER(b.{join_on})
      # WHERE %s
      \""",
      -- Generate: a.col1, b.col1, a.col2, b.col2...
      STRING_AGG(format("IF(a.%s=b.%s, Null, a.%s) AS a_%s, IF(a.%s=b.%s, Null, b.%s) AS b_%s", column_name, column_name, column_name, column_name, column_name, column_name, column_name, column_name), ", "),

      -- Generate the WHERE clause: a.col1 != b.col1 OR a.col2 != b.col2...
      STRING_AGG(format("a.%s != b.%s", column_name, column_name), " OR ")
    )
  FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_PREV_EXT_DATASET}.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = '{table_name}' 
);

-- Step 2: Run the generated query
EXECUTE IMMEDIATE schema_comparison_query;
"""
    # We just run the query.
    result = client.query(query).to_dataframe()
    return result


def get_github_directory_contents_from_comet(path, branch="current"):
    file_url = f"https://api.github.com/repos/ImagingDataCommons/idc-comet/contents/{path}?ref={branch}"
    headers = {
        "Authorization": f"token {settings.GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }
    response = requests.get(file_url, headers=headers)
    if response.status_code == 200:
        contents = response.json()
        # List names of all files in the directory
        file_names = [item['name'] for item in contents if item['type'] == 'file']
    else:
        print(f"Error: {response.status_code}", response.json())

    return file_names


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', default='idc-dev-etl', help='BQ project')
    parser.add_argument('--bq_dataset_id', default=f'idc_v{settings.CURRENT_VERSION}_dev', help='BQ datasey')
    parser.add_argument('--table_id', default='zen', help='Table name to which to copy data')
    args = parser.parse_args()

    r = get_github_directory_contents_from_comet("collections/original", branch="release/v24")
    table_name = 'analysis_results_metadata'
    df = compare_versioned_bq_tables(table_name).dropna(axis=1, how='all')
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    # Set width to ensure it fits the console, adjust the number as needed
    pd.set_option('display.width', 2000)
    print(df)

    exit(0)
