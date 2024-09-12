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

# Generate the original_collections_descriptions_end_user table in BQ, from a
# spreadsheet in Google Drive. These descriptions have "normal' hyperlinks...
# no warning about leaving a .gov website.
import settings
import argparse
import pandas as pd
from google.cloud import bigquery
import markdownify

def load_spreadsheet(args, schema=None):
    # Load the Google Sheets data into a Pandas DataFrame

    url = f'https://docs.google.com/spreadsheets/d/{args.spreadsheet_id}/gviz/tq?tqx=out:csv&sheet={args.sheet_name}'

    df = pd.read_csv(url)
    df = df.map(str)
    df = df.replace({'nan': None})

    # Initialize the BigQuery client
    client = bigquery.Client()

    if args.columns:
        for (columnName, columnData) in df.iteritems():
            if not columnName in args.columns:
                df = df.drop(columnName, axis=1)

    if not schema:
        # Create the BigQuery table schema based on the DataFrame columns
        # We assume all columns are STRINGs
        schema = []
        for column in df.columns:
            schema.append(bigquery.SchemaField(column, 'STRING'))

    # Convert HTML to Markdown and delete empty lines
    for i, row in df.iterrows():
        description = markdownify.markdownify(df.at[i, 'description'])
        lines = []
        for line in description.split('\n'):
            if line:
                line = line.replace('\\', '')
                lines.append(line)
        description = '\n'.join(lines)
        df.at[i,'description'] = description


    # Define the BigQuery table reference
    table_ref = client.dataset(args.bq_dataset_id, project=args.project).table(args.table_id)

    # Create the BigQuery table if it doesn't exist
    try:
        client.get_table(table_ref)
    except:
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)

    # Write the DataFrame data to BigQuery
    job_config = bigquery.LoadJobConfig(schema=schema, write_disposition='WRITE_TRUNCATE')
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    print('Data imported successfully!')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--spreadsheet_id', default = '1RCS_CBEe6MWLnciaHRiClclPO3HbgUk1EI2U3AJvM6Y',
                        help='"id" portion of spreadsheet URL')
    parser.add_argument('--sheet_name', default = 'idc_current', help='Sheet within spreadsheet to load')
    parser.add_argument('--project', default='idc-dev-etl', help='BQ project')
    parser.add_argument('--bq_dataset_id', default=f'idc_v{settings.CURRENT_VERSION}_dev', help='BQ datasey')
    parser.add_argument('--table_id', default='original_collections_descriptions_end_user', help='Table name to which to copy data')
    parser.add_argument('--columns', default=[], help='Columns in df to keep. Keep all if list is empty')

    args = parser.parse_args()
    print('args: {}'.format(args))

    load_spreadsheet(args)
    # export_table(args)
