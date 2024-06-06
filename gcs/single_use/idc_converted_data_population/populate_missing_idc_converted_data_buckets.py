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

# Populate buckets in original_collections_descriptions that could not be populated by copying
# from some existing bucket
import argparse
import pandas as pd
from utilities.logging_config import successlogger, progresslogger, errlogger
from google.cloud import bigquery, storage
import time


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
    SELECT DISTINCT se_uuid, i_uuid, ingestion_url
    FROM `idc-dev-etl.idc_v19_dev.all_joined_public` 
    WHERE ingestion_url != ""
    AND ingestion_url LIKE '{pattern}'
    """

    instances = client.query(query).to_dataframe()
    return instances

TRIES = 3

def populate_bucket(args,dones, original_bucket_path, new_bucket, original_instances):
    client = storage.Client()
    # source_bucket = client.bucket(original_bucket_path)
    src_bucket = client.bucket('idc-dev-open')
    dst_bucket = client.bucket(new_bucket)
    error = False
    for i,j in original_instances.iterrows():
        src_blob_name = f'{j.se_uuid}/{j.i_uuid}.dcm'
        dst_blob_name = j.ingestion_url.split('/',2)[-1]
        if not src_blob_name in dones:
            src_blob = src_bucket.blob(src_blob_name)
            dst_blob = dst_bucket.blob(dst_blob_name)
            retries = 0
            while True:
                try:
                    rewrite_token = False
                    while True:
                        rewrite_token, bytes_rewritten, bytes_to_rewrite = dst_blob.rewrite(
                            src_blob, token=rewrite_token
                        )
                        if not rewrite_token:
                            break
                    successlogger.info(f'{src_blob_name}')
                    break
                except Exception as exc:
                    if retries == TRIES:
                        errlogger.error('p%s: %s/%s copy failed\n   %s', args.id, args.src_bucket, blob_name, exc)
                        break
                time.sleep(retries)
                retries += 1
        else:
            print(f'{src_blob_name} previously copied')

            # try:
            #     blob_copy = source_bucket.copy_blob(source_blob, destination_bucket, destination_blob_name)
            #     successlogger.info(source_blob_name)
            # except Exception as exc:
            #     errlogger.error(f"{j.ingestion_url}: {exc}")
            #     error = True
            #     break

    if not error:
        successlogger.info(f'{new_bucket}/{original_bucket_path}')
        progresslogger.info(f'{new_bucket}/{original_bucket_path} populated')
    else:
        progresslogger.info(f'{new_bucket}/{original_bucket_path} failed populated')

    return

def validate_buckets(args):
    df = load_spreadsheet(args)
    dones = open(successlogger.handlers[0].baseFilename).read().splitlines()

    for i in range(len(df)):
        row = df.loc[i]
        original_bucket_path = row.OriginalBucketPath
        if not f'{row.idc_converted_data_bucket}/{original_bucket_path}' in dones:
            if row.Copied == 'No':
                original_instances = get_original_instances(original_bucket_path)
                if len(original_instances) != int(float(row.InstanceCount)):
                    errlogger.error(f'Found instance count {len(original_instances)} != expected count {int(float(row.Count))}')
                    breakpoint()
                populate_bucket(args,dones, original_bucket_path, row.idc_converted_data_bucket, original_instances)
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

    args = parser.parse_args()
    print('args: {}'.format(args))
    validate_buckets(args)
