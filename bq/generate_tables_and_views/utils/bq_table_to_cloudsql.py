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

# This script exports the contents of a BQ table to a dataframe and then
# inserts that data into a table in a CloudSQL DB.

import argparse
from google.cloud import bigquery
from utilities.sqlalchemy_helpers import sa_session

def export_table(args):
    # Initialize the BigQuery client
    bq_client = bigquery.Client()

    # Export table contents to a dataframe
    dataset_ref = bigquery.DatasetReference(args.project, args.bq_dataset_id)
    table_ref = dataset_ref.table(args.table_id)
    table = bq_client.get_table(table_ref)
    df = bq_client.list_rows(table).to_dataframe()

    with sa_session() as sess:
        # Delete the CloudSQL table if it exists
        sess.execute(f"DROP TABLE IF EXISTS {args.table_id};")

        # Build the schema and (re)create the CloudSQL table
        schema = []
        columns = df.columns
        type_map = {
            'float64': 'NUMERIC',
            'int64': 'INTEGER',
            'datetime64': 'TIMESTAMP',
            'object': 'VARCHAR'
        }
        dtypes = [type_map[dtype.name] for dtype in df.dtypes]
        for column_name, dtype in zip(columns, dtypes):
            schema.append(f'{column_name} {dtype}')
        schema = ','.join(schema)
        sess.execute(f"CREATE TABLE {args.table_id} ({schema});")

        # Insert data into the Cloud SQL table
        for _, row in df.iterrows():
            values = ', '.join(f"'{value}'" for value in row.values)
            # cursor.execute(f"INSERT INTO `{table_name}` VALUES ({values});")
            sess.execute(f"INSERT INTO {args.table_id} VALUES ({values});")

        sess.commit()
    print('Export and import completed successfully!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', default='idc-dev-etl', help='BQ project')
    parser.add_argument('--bq_dataset_id', default='idc_dev_etl', help='BQ datasey')
    parser.add_argument('--table_id', default='programs', help='Table name to which to copy data')
    parser.add_argument('--columns', default=['tcia_wiki_collection_id', 'program'], help='Columns in df to keep. Keep all if list is empty')
    args = parser.parse_args()
    print('args: {}'.format(args))

    export_table(args)




