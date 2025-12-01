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


# Get the descriptions of collections that are only sourced from IDC
def get_idc_descriptions(args, schema=None):
    # Load the Google Sheets data into a Pandas DataFrame

    url = f'https://docs.google.com/spreadsheets/d/{args.spreadsheet_id}/gviz/tq?tqx=out:csv&sheet={args.sheet_name}'

    df = pd.read_csv(url)
    df = df.map(str)
    df = df.replace({'nan': None})

    return df

# Get TCIA's descriptions of collections we get from them. For any collection for which there is also pathology, add a
# paragraph to TCIA's description directing to the IDC Zenodo page.
def get_tcia_descriptions(args):
    client = bigquery.Client()
    query = f"""
WITH path AS (
    SELECT DISTINCT ajpc.collection_id, source_url, 
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_public_and_current` ajpc
    JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_EXT_DATASET}.dicom_metadata` dm
    ON ajpc.sop_instance_uid = dm.sopinstanceuid
    WHERE dm.Modality='SM'
)
SELECT DISTINCT 
    REPLACE(REPLACE(LOWER(tcd.collection_id), '-', '_'), ' ', '_') collection_id, 
    if(path.collection_id is not NULL,
        CONCAT(tcd.description, 
        '<p>Please see the <a href="', path.source_url, '" target="_blank">', tcd.collection_id, ': ', ocmis.title, '</a>) wiki page to learn more about the histopathology images and to obtain any supporting metadata for this collection.</p>'),
        tcd.description
        ) description
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.tcia_collection_descriptions` tcd
    RIGHT JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_collections` ac
    ON tcd.collection_id = ac.collection_name
    LEFT JOIN path
    ON tcd.collection_id = path.collection_id
    LEFT JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.original_collections_metadata_idc_source` ocmis
    ON path.collection_id = ocmis.collection_name
    WHERE ac.Access = 'Public' AND tcd.collection_id IS NOT NULL AND (ac.metadata_sunset = 0)
    ORDER BY collection_id
    
    """

    df = client.query_and_wait(query).to_dataframe()
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

    # Create the BigQuery table if it doesn't exist
    # try:
    #     client.get_table(table_ref)
    # except:
    #     table = bigquery.Table(table_ref, schema=schema)
    #     client.create_table(table)

    # Write the DataFrame data to BigQuery
    job_config = bigquery.LoadJobConfig(schema=schema, write_disposition='WRITE_TRUNCATE')
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    print('Data imported successfully!')

def main(args):
    idc_descriptions = get_idc_descriptions(args)
    tcia_descriptions = get_tcia_descriptions(args)
    all_descriptions = pd.concat([idc_descriptions, tcia_descriptions], ignore_index=True)
    markdown_descriptions = convert_to_markdown(all_descriptions)
    output_to_bq(markdown_descriptions)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--spreadsheet_id', default = '1rto9Dh7GPrSv55lyo9n0lg9gIdszTnKbLl5k_yawV8E',
                        help='"id" portion of spreadsheet URL')
    parser.add_argument('--sheet_name', default = f'idc_v{settings.CURRENT_VERSION}', help='Sheet within spreadsheet to load')
    parser.add_argument('--project', default='idc-dev-etl', help='BQ project')
    parser.add_argument('--bq_dataset_id', default=f'idc_v{settings.CURRENT_VERSION}_dev', help='BQ datasey')
    parser.add_argument('--table_id', default='original_collections_descriptions_end_user_new', help='Table name to which to copy data')
    parser.add_argument('--columns', default=[], help='Columns in df to keep. Keep all if list is empty')

    args = parser.parse_args()
    print('args: {}'.format(args))

    main(args)
    # export_table(args)
