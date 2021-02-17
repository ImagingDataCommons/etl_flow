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

# import os
# import uuid
import time
import datetime
from google.cloud import bigquery
from python_settings import settings
# from utilities.bq_helpers import query_BQ
from idc.config import bigquery_uri, sql_uri
from idc.models import Auxilliary_Metadata, Version, Collection, Patient, Study, Series, Instance
# from idc.sqlalchemy_orm_models import Version, Collection
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select

# from sqlalchemy.ext.declarative import declarative_base

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

def populate_version(bq_engine, sql_engine):
    print('Populating versions')
    query = f"""
        SELECT * from `{settings.GCP_PROJECT}.{settings.BIGQUERY_DATASET}.versions`"""
    results = bq_engine.execute(query).fetchall()

    rows = [dict(row.items()) for row in results]
    with Session(sql_engine) as sess:
        sess.bulk_insert_mappings(Version,rows)
        sess.commit()
        sess.execute(text("select setval(pg_get_serial_sequence('version','id'), coalesce(max(id),0) + 1, false) FROM version"))
        sess.commit()


def populate_collection(bq_engine, sql_engine):
    print('Populating collections')
    query = f"""
        SELECT * from `{settings.GCP_PROJECT}.{settings.BIGQUERY_DATASET}.collections`"""
    results = bq_engine.execute(query).fetchall()
    rows = [dict(row.items()) for row in results]

    with Session(sql_engine) as sess:
        sess.bulk_insert_mappings(Collection,rows)
        sess.commit()
        sess.execute(text(
            "select setval(pg_get_serial_sequence('collection','id'), coalesce(max(id),0) + 1, false) FROM collection"))
        sess.commit()


def populate_patient(bq_engine, sql_engine):
    print('Populating patients')
    query = f"""
        SELECT * from `{settings.GCP_PROJECT}.{settings.BIGQUERY_DATASET}.patients`"""
    results = bq_engine.execute(query).fetchall()
    rows = [dict(row.items()) for row in results]

    with Session(sql_engine) as sess:
        sess.bulk_insert_mappings(Patient,rows)
        sess.commit()
        sess.execute(text(
            "select setval(pg_get_serial_sequence('patient','id'), coalesce(max(id),0) + 1, false) FROM patient"))
        sess.commit()
    # with sql_engine.connect() as conn:
    #     conn.execute(Patient.__table__.insert(), rows)


def populate_study(bq_engine, sql_engine):
    print('Populating studies')
    query = f"""
        SELECT * from `{settings.GCP_PROJECT}.{settings.BIGQUERY_DATASET}.studies`"""
    results = bq_engine.execute(query).fetchall()
    rows = [dict(row.items()) for row in results]

    with Session(sql_engine) as sess:
        sess.bulk_insert_mappings(Study,rows)
        sess.commit()
        sess.execute(text(
            "select setval(pg_get_serial_sequence('study','id'), coalesce(max(id),0) + 1, false) FROM study"))
        sess.commit()
    # with sql_engine.connect() as conn:
    #     conn.execute(Study.__table__.insert(), rows)


def populate_series(bq_engine, sql_engine):
    print('Populating series')
    query = f"""
        SELECT * from `{settings.GCP_PROJECT}.{settings.BIGQUERY_DATASET}.series`"""
    results = bq_engine.execute(query).fetchall()

    # table_name = table
    rows = [dict(row.items()) for row in results]
    with Session(sql_engine) as sess:
        sess.bulk_insert_mappings(Series,rows)
        sess.commit()
        sess.execute(text(
            "select setval(pg_get_serial_sequence('series','id'), coalesce(max(id),0) + 1, false) FROM series"))
        sess.commit()
    # with sql_engine.connect() as conn:
    #     conn.execute(Series.__table__.insert(), rows)


def populate_instance(bq_engine, sql_engine):
    print('Populating instances')
    client = bigquery.Client()
    limit = 100000
    offset = 0

    while True:
        print(f"Rows: {offset}-{offset+limit-1}, {time.asctime()}")
        sql = f"""
                SELECT * from `{settings.GCP_PROJECT}.{settings.BIGQUERY_DATASET}.instances`
                ORDER BY id LIMIT {limit} OFFSET {offset}"""
        query_job = client.query(sql)
        instance_rows = query_job.result()
        # instance_rows = bq_engine.execute(query).fetchall()

        rows = [dict(row.items()) for row in instance_rows]
        if len(rows) == 0:
            break
        with Session(sql_engine) as sess:
            sess.bulk_insert_mappings(Instance, rows)
            sess.commit()
        offset += limit

    with Session(sql_engine) as sess:
        sess.execute(text(
            "select setval(pg_get_serial_sequence('instance','id'), coalesce(max(id),0) + 1, false) FROM instance"))
        sess.commit()

        # with sql_engine.connect() as conn:
        #     conn.execute(Instance.__table__.insert(), rows)
        # offset += limit


def populate_all():
    sql_engine = create_engine(sql_uri, echo=True)
    bq_engine = create_engine(bigquery_uri)
    # populate_auxilliary_metadata_from_BQ(bq_engine, sql_engine, Auxilliary_Metadata)

    populate_version(bq_engine, sql_engine)
    populate_collection(bq_engine, sql_engine)
    populate_patient(bq_engine, sql_engine)
    populate_study(bq_engine, sql_engine)
    populate_series(bq_engine, sql_engine)
    populate_instance(bq_engine, sql_engine)
    pass

if __name__ == '__main__':
    populate_all()





