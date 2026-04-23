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
# spreadsheet in Google Drive. These descriptions have "normal" hyperlinks...
# no warning about leaving a .gov website.
import settings
import argparse
import pandas as pd
from google.cloud import bigquery
import markdownify
from bq.bq_utilities import read_json_to_dataframe, dataframe_to_bq


# Get the descriptions of collections that are only sourced from IDC
def get_idc_descriptions(args, schema=None):
    # Load the Google Sheets data into a Pandas DataFrame

    file_path = f'{settings.BQ_JSON_PROJECT_PATH}/idc_analysis_results_descriptions.json'
    df = read_json_to_dataframe(file_path)

    return df


def convert_to_markdown(df):
    # Convert HTML to Markdown and delete empty lines
    for i, row in df.iterrows():
        description = markdownify.markdownify(df.at[i, 'description'])
        # Clean up hyperlinks
        description = description.replace('[','').replace(']',' ')
        # More clean up
        description = description.replace('**','')

        lines = []
        for line in description.split('\n'):
            if line:
                line = line.replace('\\', '')
                lines.append(line)
        description = '\n'.join(lines)
        df.at[i,'description'] = description

    return df

def output_to_bq(df):

    client = bigquery.Client()

    if args.columns:
        for (columnName, columnData) in df.iteritems():
            if not columnName in args.columns:
                df = df.drop(columnName, axis=1)

    # Create the BigQuery table schema based on the DataFrame columns
    # We assume all columns are STRINGs
    schema = []
    for column in df.columns:
        schema.append(bigquery.SchemaField(column, 'STRING'))
    # Define the BigQuery table reference
    table_ref = client.dataset(args.bq_dataset_id, project=args.project).table(args.table_id)

    client.delete_table(table_ref, not_found_ok=True )
    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table)

    # Write the DataFrame data to BigQuery
    job_config = bigquery.LoadJobConfig(schema=schema, write_disposition='WRITE_TRUNCATE')
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    print('Data imported successfully!')

def main(args):
    idc_descriptions = get_idc_descriptions(args)
    markdown_descriptions = convert_to_markdown(idc_descriptions)
    dataframe_to_bq(args, markdown_descriptions)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', default='idc-dev-etl', help='BQ project')
    parser.add_argument('--bq_dataset_id', default=f'idc_v{settings.CURRENT_VERSION}_dev', help='BQ datasey')
    parser.add_argument('--table_id', default='analysis_results_end_user_descriptions', help='Table name to which to copy data')

    args = parser.parse_args()
    print('args: {}'.format(args))

    main(args)
    # export_table(args)
