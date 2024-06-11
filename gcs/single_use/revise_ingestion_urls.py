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

# Revise ingestion_url values in instance table and gcs_url values in idc_instance table
# to ref buckets in idc-converted-data
import argparse
import pandas as pd
from utilities.logging_config import successlogger, progresslogger, errlogger
from google.cloud import bigquery
from idc.models import Instance, IDC_Instance_Dev
from utilities.tcia_helpers import get_access_token
from utilities.sqlalchemy_helpers import sa_session
from sqlalchemy import update



def load_spreadsheet(args):
    # Load the Google Sheets data into a Pandas DataFrame

    url = f'https://docs.google.com/spreadsheets/d/{args.spreadsheet_id}/gviz/tq?tqx=out:csv&sheet={args.sheet_name}'

    df = pd.read_csv(url)
    df = df.map(str)
    df = df.replace({'nan': None})

    return df



def revise_urls(args):
    df = load_spreadsheet(args)
    dones = open(successlogger.handlers[0].baseFilename).read().splitlines()
    with sa_session(echo=True) as sess:
        # Get a sharable NBIA access token

        for i, row in df.iterrows():
            if row.Copied == "Yes":
                instances = sess.query(IDC_Instance_Dev). \
                    filter(IDC_Instance_Dev.gcs_url.startswith(f"gs://{row.OriginalBucketPath.replace('*', '')}")).all()

                stmt = update(IDC_Instance_Dev). \
                    values(
                    gcs_url="gs://" + row.idc_converted_data_bucket + "/" + str(IDC_Instance_Dev.gcs_url).split('/', 2)[-1]). \
                    where(IDC_Instance_Dev.gcs_url.startswith(f"gs://{row.OriginalBucketPath.replace('*', '')}"))

                stmt = update(IDC_Instance_Dev). \
                    values(gcs_url = f"gs://{row.idc_converted_data_bucket}/{IDC_Instance_Dev.gcs_url.split('/',2)[-1]}").\
                    where(IDC_Instance_Dev.gcs_url.startswith(f"gs://{row.OriginalBucketPath.replace('*', '')}"))
                sess.execute(stmt)

                # instances = sess.query(IDC_Instance_Dev).\
                #     filter(IDC_Instance_Dev.gcs_url.startswith(f"gs://{row.OriginalBucketPath.replace('*', '')}")). \
                #     update({'gcs_url': str(IDC_Instance_Dev.gcs_url).replace('gs://', f'gs://{row.idc_converted_data_bucket}/')})

                # instances = sess.query(IDC_Instance_Dev). \
                #     filter(IDC_Instance_Dev.gcs_url.startswith(f"gs://{row.OriginalBucketPath.replace('*', '')}")).all()
                # for instance in instances:
                #     instance.gcs_url = instance.gcs_url.replace('gs://', f'gs://{row.idc_converted_data_bucket}/')
                sess.commit()
    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--spreadsheet_id', default = '1I31W1O0C3aJU_9ftYUXMTXOhJU1YDUot90CmAy0LzBA',
                        help='"id" portion of spreadsheet URL')
    parser.add_argument('--sheet_name', default = 'current', help='Sheet within spreadsheet to load')

    args = parser.parse_args()
    print('args: {}'.format(args))
    revise_urls(args)
