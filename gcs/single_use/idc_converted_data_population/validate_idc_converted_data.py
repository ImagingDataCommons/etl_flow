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

# Validate that buckets in idc-converted-data have expected instances
import settings
import argparse
import pandas as pd
from utilities.logging_config import successlogger, progresslogger, errlogger
from google.cloud import bigquery, storage


def load_spreadsheet(args):
    # Load the Google Sheets data into a Pandas DataFrame

    url = f'https://docs.google.com/spreadsheets/d/{args.spreadsheet_id}/gviz/tq?tqx=out:csv&sheet={args.sheet_name}'

    df = pd.read_csv(url)
    df = df.applymap(str)
    df = df.replace({'nan': None})

    return df

def get_original_instances(original_bucket_path):

    client = bigquery.Client()
    path = original_bucket_path.replace('*', '%')
    pattern = f"gs://{path}%"
    query = f"""
    SELECT DISTINCT i_uuid, ingestion_url
    FROM `idc-dev-etl.idc_v19_dev.all_joined_public` 
    WHERE ingestion_url != ""
    AND ingestion_url LIKE '{pattern}'
    """

    instances = client.query(query).to_dataframe()
    return instances


def validate_bucket(args,dones, original_bucket_path, new_bucket, original_instances):
    client = storage.Client()
    bucket = client.bucket(new_bucket)
    error = False
    for i,j in original_instances.iterrows():
        blob_name = j.ingestion_url.replace('gs://', '')
        if not blob_name in dones:
            if bucket.blob(blob_name).exists():
                successlogger.info(blob_name)
            else:
                errlogger.error(blob_name)
                error = True
                break

    if not error:
        successlogger.info(f'{new_bucket}/{original_bucket_path}')
        progresslogger.info(f'{new_bucket}/{original_bucket_path} validated')
    else:
        progresslogger.info(f'{new_bucket}/{original_bucket_path} failed validation')

    return

def validate_buckets(args):
    df = load_spreadsheet(args)
    dones = open(successlogger.handlers[0].baseFilename).read().splitlines()

    for i in range(len(df)):
        row = df.loc[i]
        original_bucket_path = row.OriginalBucketPath
        if not f'{row.idc_converted_data_bucket}/{original_bucket_path}' in dones:
            if row.Copied == 'Yes':
                original_instances = get_original_instances(original_bucket_path)
                if len(original_instances) != int(float(row.InstanceCount)):
                    errlogger.error(f'Found instance count {len(original_instances)} != expected count {int(float(row.Count))}')
                    breakpoint()
                validate_bucket(args,dones, original_bucket_path, row.idc_converted_data_bucket, original_instances)
            else:
                progresslogger.info(f'Skipping {row.idc_converted_data_bucket}/{original_bucket_path}')
        else:
            progresslogger.info(f'Bucket {row.idc_converted_data_bucket}/{original_bucket_path} previously validated')

    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--spreadsheet_id', default = '1I31W1O0C3aJU_9ftYUXMTXOhJU1YDUot90CmAy0LzBA',
                        help='"id" portion of spreadsheet URL')
    parser.add_argument('--sheet_name', default = 'current', help='Sheet within spreadsheet to load')
    parser.add_argument('--project', default='idc-dev-etl', help='BQ project')
    parser.add_argument('--bq_dataset_id', default=f'idc_v{settings.CURRENT_VERSION}_dev', help='BQ datasey')
    parser.add_argument('--table_id', default='original_collections_descriptions', help='Table name to which to copy data')
    parser.add_argument('--columns', default=[], help='Columns in df to keep. Keep all if list is empty')

    args = parser.parse_args()
    print('args: {}'.format(args))
    validate_buckets(args)
