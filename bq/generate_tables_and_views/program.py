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
import pandas as pd

import settings
import argparse
from utilities.tcia_helpers import get_all_tcia_metadata
from google.cloud import bigquery

SCHEMA = [
    bigquery.SchemaField('collection_id', 'STRING', mode='NULLABLE', description='Collection id'),
    bigquery.SchemaField('program', 'STRING', mode='NULLABLE', description='Program name')]


def gen_table(args):
    # Get a list of the program of each IDC sourced collectiuon
    client = bigquery.Client()
    query = f'''
    SELECT collection_id, program
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.original_collections_metadata_idc_source`'''
    idc_programs = [row.values() for row in client.query(query)]

    # Get a list of the program of each TCIA sourced collection
    tcia_programs = [(row['collection_short_title'].lower().replace('-','_').replace(' ','_'), row['program'][0]) for row in \
        get_all_tcia_metadata(type="collections", query_param="&_fields=collection_short_title,program")]


    all_programs = idc_programs
    all_programs.extend(tcia_programs)
    df = pd.DataFrame(all_programs, columns=['collection_id', 'program'])

    # Define the BigQuery table reference
    table_ref = client.dataset(args.bq_dataset_id, project=args.project).table(args.table_id)

    # Create the BigQuery table schema based on the DataFrame columns
    # We assume all columns are STRINGs
    schema = []
    for column in df.columns:
        schema.append(bigquery.SchemaField(column, 'STRING'))

    # Create the BigQuery table if it doesn't exist
    try:
        client.get_table(table_ref)
    except:
        table = bigquery.Table(table_ref, schema=SCHEMA)
        client.create_table(table)

    # Write the DataFrame data to BigQuery
    job_config = bigquery.LoadJobConfig(schema=schema, write_disposition='WRITE_TRUNCATE')
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    print('Data imported successfully!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--spreadsheet_id', default = '1-sk8CMTDDj-deKv7sXglLvHUhDSNS1cRqUg5Oy5UpRY',
                                            help='"id" portion of spreadsheet URL')
    parser.add_argument('--sheet_name', default = 'Sheet1', help='Sheet within spreadsheet to load')
    parser.add_argument('--project', default='idc-dev-etl', help='BQ project')
    parser.add_argument('--bq_dataset_id', default=f'idc_v{settings.CURRENT_VERSION}_dev', help='BQ datasey')
    parser.add_argument('--table_id', default='program', help='Table name to which to copy data')
    parser.add_argument('--columns', default=['tcia_wiki_collection_id', 'program'], help='Columns in df to keep. Keep all if list is empty')

    args = parser.parse_args()
    print('args: {}'.format(args))

    gen_table(args)
    # export_table(args)
