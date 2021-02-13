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

from os import environ
import argparse
from sqlalchemy import MetaData, Table
from sqlalchemy.engine import create_engine
from python_settings import settings

from python_settings import settings
import settings as etl_settings

settings.configure(etl_settings)
assert settings.configured




class DataClient:

    def __init__(self, engine):
        self.engine = engine
        self.metadata = MetaData(bind=self.engine)
        self.table_name = None

    @property
    def table(self):
        if self.table_name:
            return Table(self.table_name, self.metadata, autoload=True)
        return None

    def fetch_rows(self, query, table=None):
        """Fetch all rows via query."""
        rows = self.engine.execute(query).fetchall()
        return rows

    def insert_rows(self, rows, table=None, replace=None):
        """Insert rows into table."""
        if replace:
            self.engine.execute(f'TRUNCATE TABLE {table}')
        self.table_name = table
        self.engine.execute(self.table.insert(), rows)
        return self.construct_response(rows, table)

    @staticmethod
    def construct_response(rows, table):
        """Summarize results of an executed query."""
        columns = rows[0].keys()
        column_names = ", ".join(columns)
        num_rows = len(rows)
        return f'Inserted {num_rows} rows into `{table}` with {len(columns)} columns: {column_names}'


def get_BQ_table(args):
    bigquery_uri = f'bigquery://{args.project}/{args.bqdataset}'
    engine = create_engine(bigquery_uri)
    query = f'SELECT * FROM {args.bqdataset}.{args.bqtable}'
    rows = engine.execute(query).fetchall()

    return rows

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bqdataset', default='whc_dev', help='BQ dataset name')
    parser.add_argument('--bqtable', default='data_collections_metadata', help='BQ table name')
    parser.add_argument('--project', default='idc_peewee-dev-etl')

    args = parser.parse_args()
    print("{}".format(args))

    results = get_BQ_table(args)
    pass

