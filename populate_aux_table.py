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

from python_settings import settings
from idc.config import bigquery_uri, sql_uri
from idc.models import Auxilliary_Metadata, Version, Collection, Patient, Study, Series, Instance
from sqlalchemy import create_engine, text

# from sqlalchemy.ext.declarative import declarative_base

# Duplicate an auxilliary metadata table in BQ
# The BQ dataset containing the table to duplicate is specified in the .env file (maybe not the best place).
# The bigquery_uri engine is configured to access that dataset.

def populate_auxilliary_metadata_from_BQ(bq_engine, sql_engine, table):
    # if replace:
    #     sql_engine.execute(f'TRUNCATE TABLE {table}')
    query = f'SELECT DISTINCT tcia_api_collection_id from `{settings.GCP_PROJECT}.{settings.BIGQUERY_DATASET}.{settings.BIGQUERY_AUXILLIARY_METADATA}`'
    collections = bq_engine.execute(query).fetchall()

    for collection in collections:
        print(f'Collection: {collection["tcia_api_collection_id"]}')
        query = f"""
            SELECT * from `{settings.GCP_PROJECT}.{settings.BIGQUERY_DATASET}.{settings.BIGQUERY_AUXILLIARY_METADATA}`
            WHERE tcia_api_collection_id = \"{collection['tcia_api_collection_id']}\""""
        collection_rows= bq_engine.execute(query).fetchall()

        # table_name = table
        rows = [dict(c.items()) for c in collection_rows]
        with sql_engine.connect() as conn:
            conn.execute(table.__table__.insert(), rows)


def populate_all():
    sql_engine = create_engine(sql_uri, echo=True)
    bq_engine = create_engine(bigquery_uri)
    populate_auxilliary_metadata_from_BQ(bq_engine, sql_engine, Auxilliary_Metadata)
    pass

if __name__ == '__main__':
    populate_all()





