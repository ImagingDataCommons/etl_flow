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

import os
import logging
from python_settings import settings
import settings as etl_settings

settings.configure(etl_settings)
assert settings.configured

logger = logging.getLogger(settings.LOGGER_NAME)

from sqlalchemy.engine import create_engine
from config import bigquery_uri, rdbms_uri

from sqlalchemy import MetaData, Table

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

    def insert_rows(self, rows, table=None, replace=None):
        """Insert rows into table."""
        if replace:
            self.engine.execute(f'TRUNCATE TABLE {table}')
        self.table_name = table
        self.engine.execute(self.table.insert(), rows)
        return self.construct_response(rows, table)

    def fetch_rows(self, query):
        """Fetch all rows via query."""
        rows = self.engine.execute(query).fetchall()
        return rows

    @staticmethod
    def construct_response(rows, table):
        """Summarize results of an executed query."""
        columns = rows[0].keys()
        column_names = ", ".join(columns)
        num_rows = len(rows)
        return f'Inserted {num_rows} rows into `{table}` with {len(columns)} columns: {column_names}'

# bigquery_engine = create_engine(bigquery_uri,
#                                 credentials_path=gcp_credentials)
bigquery_engine = create_engine(bigquery_uri)

rdbms_engine = create_engine(rdbms_uri, echo=True, future=True)

if __name__ == "__main__":
    bqc = DataClient(bigquery_engine)
    dbc = DataClient(rdbms_engine)
    rows = bqc.fetch_rows(query)
    insert = dbc.insert_rows(rows, 'data_collections_metadata', replace=True)
    logger.info(insert)

