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

# Duplicate psql version, collection, patient, study, series and instance metadata tables in BQ. These are
# essentially a normalization of an auxilliary_metadata table
# The BQ dataset containing the tables to be duplicated is specified in the .env file (maybe not the best place).
# The bigquery_uri engine is configured to access that dataset.


import os
# import uuid
import time
import datetime
import logging
from logging import INFO
import argparse
from google.cloud import bigquery
from utilities.bq_helpers import BQ_table_exists, create_BQ_table, delete_BQ_Table, load_BQ_from_CSV
# from python_settings import settings
# from idc.config import bigquery_uri, sql_uri
# from idc.models import Auxilliary_Metadata, Version, Collection, Patient, Study, Series, Instance
# from sqlalchemy import create_engine, text
# from sqlalchemy.orm import Session

from python_settings import settings
import settings as etl_settings

settings.configure(etl_settings)
assert settings.configured
import psycopg2
from psycopg2.extras import DictCursor

# psql to bq type name conversions:
type_name_conversion = {
    "integer": "INTEGER",
    "boolean": "BOOLEAN",
    "character varying": "STRING",
    "nextval('instance_id_seq'::regclass)": "INTEGER",
    "timestamp without time zone": "DATETIME"
}


def get_schema(cur, args, table):
    # Get the psql schema fields
    cur.execute("""
    SELECT column_name, data_type, character_maximum_length, column_default, is_nullable
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE table_name = (%s)
    ORDER BY ordinal_position""", (table,))
    column_data = cur.fetchall()

    # Separate query for the descriptions
    cur.execute("""
    SELECT cols.column_name, (
        SELECT
            pg_catalog.col_description(c.oid, cols.ordinal_position::int)
        FROM
            pg_catalog.pg_class c
        WHERE
            c.oid = (SELECT ('"' || cols.table_name || '"')::regclass::oid)
            AND c.relname = cols.table_name
    ) AS column_comment
    FROM information_schema.columns cols
    WHERE
        cols.table_catalog    = 'idc'
        AND cols.table_name   = (%s)
        AND cols.table_schema = 'public'
    ORDER BY ordinal_position""", (table,))
    descriptions = cur.fetchall()

    schema = []
    for col,desc in zip(column_data, descriptions):
        try:
            s = {
                "name": col["column_name"],
                "description": desc[0],
                "mode": "NULLABLE" if col["is_nullable"] == "YES" else "REQUIRED",
                "type": type_name_conversion[col["data_type"]]
            }
            schema.append(s)
        except Exception as exc:
            errlogger.error("Error creating schema: %s", exc)
            raise exc


    return schema


def upload_version(cur, args):
    table = "version"
    print(f'Populating table {table}')
    schema = get_schema(cur, args, table)
    client = bigquery.Client(project=args.project)

    if BQ_table_exists(client, args.project, args.bqdataset_name, table):
        delete_BQ_Table(client, args.project, args.bqdataset_name, table)
    result = create_BQ_table(client, args.project, args.bqdataset_name, table, schema)

    cur.execute("""
        SELECT * from version
        WHERE idc_version_number = (%s)
        """, (args.version,))
    rows = cur.fetchall()

    # Make sure that the schema and retrieved data have the same order
    col_order = cur.description
    for col, sch in zip(col_order, schema):
        try:
            assert col.name == sch['name']
        except:
            errlogger.error('Order mismatch in version')
            raise
    data = "\n".join([",".join(map(str, row)) for row in rows])

    load_BQ_from_CSV(client, args.bqdataset_name, table, data, schema),


def upload_collection(cur, args):
    table = "collection"
    print(f'Populating table {table}')
    schema = get_schema(cur, args, table)
    client = bigquery.Client(project=args.project)

    if BQ_table_exists(client, args.project, args.bqdataset_name, table):
        delete_BQ_Table(client, args.project, args.bqdataset_name, table)
    result = create_BQ_table(client, args.project, args.bqdataset_name, table, schema)

    cur.execute("""
        SELECT collection.* FROM version
        JOIN collection
        ON version.id = collection.version_id
        WHERE version.id = (%s)
        """, (args.version,))
    rows = cur.fetchall()

    # Make sure that the schema and retrieved data have the same order
    col_order = cur.description
    for col, sch in zip(col_order, schema):
        try:
            assert col.name == sch['name']
        except:
            errlogger.error('Order mismatch in version')
            raise
    data = "\n".join([",".join(map(str, row)) for row in rows])

    load_BQ_from_CSV(client, args.bqdataset_name, table, data, schema)


def upload_patient(cur, args):
    table = "patient"
    print(f'Populating table {table}')
    schema = get_schema(cur, args, table)
    client = bigquery.Client(project=args.project)

    if BQ_table_exists(client, args.project, args.bqdataset_name, table):
        delete_BQ_Table(client, args.project, args.bqdataset_name, table)
    result = create_BQ_table(client, args.project, args.bqdataset_name, table, schema)

    cur.execute("""
        SELECT patient.* FROM version
        JOIN collection
        ON version.id = collection.version_id
        JOIN patient
        ON collection.id = patient.collection_id
        WHERE version.id = (%s)
        """, (args.version,))
    rows = cur.fetchall()

    # Make sure that the schema and retrieved data have the same order
    col_order = cur.description
    for col, sch in zip(col_order, schema):
        try:
            assert col.name == sch['name']
        except:
            errlogger.error('Order mismatch in version')
            raise
    data = "\n".join([",".join(map(str, row)) for row in rows])

    load_BQ_from_CSV(client, args.bqdataset_name, table, data, schema)


def upload_study(cur, args):
    table = "study"
    print(f'Populating table {table}')
    schema = get_schema(cur, args, table)
    client = bigquery.Client(project=args.project)

    if BQ_table_exists(client, args.project, args.bqdataset_name, table):
        delete_BQ_Table(client, args.project, args.bqdataset_name, table)
    result = create_BQ_table(client, args.project, args.bqdataset_name, table, schema)

    cur.execute("""
        SELECT study.* FROM version
        JOIN collection
        ON version.id = collection.version_id
        JOIN patient
        ON collection.id = patient.collection_id
        join study
        ON patient.id = study.patient_id
        WHERE version.id = (%s)
        """, (args.version,))
    rows = cur.fetchall()

    # Make sure that the schema and retrieved data have the same order
    col_order = cur.description
    for col, sch in zip(col_order, schema):
        try:
            assert col.name == sch['name']
        except:
            errlogger.error('Order mismatch in version')
            raise
    data = "\n".join([",".join(map(str, row)) for row in rows])

    load_BQ_from_CSV(client, args.bqdataset_name, table, data, schema)


def upload_series(cur, args):
    table = "series"
    print(f'Populating table {table}')
    schema = get_schema(cur, args, table)
    client = bigquery.Client(project=args.project)

    if BQ_table_exists(client, args.project, args.bqdataset_name, table):
        delete_BQ_Table(client, args.project, args.bqdataset_name, table)
    result = create_BQ_table(client, args.project, args.bqdataset_name, table, schema)

    # Upload series data per collection

    # Get the ids of all collections
    cur.execute("""
        SELECT collection.tcia_api_collection_id FROM version
        JOIN collection
        ON version.id = collection.version_id
        WHERE version.id = (%s)
        """, (args.version,))
    collection_ids = cur.fetchall()

    for collection_id in collection_ids:
        cur.execute("""
            SELECT series.* FROM version
            JOIN collection
            ON version.id = collection.version_id
            JOIN patient
            ON collection.id = patient.collection_id
            join study
            ON patient.id = study.patient_id
            join series
            ON study.id = series.study_id
            WHERE version.id = (%s) AND collection.tcia_api_collection_id = (%s)
            """, (args.version, collection_id[0],))
        rows = cur.fetchall()

        # Make sure that the schema and retrieved data have the same order
        col_order = cur.description
        for col, sch in zip(col_order, schema):
            try:
                assert col.name == sch['name']
            except:
                errlogger.error('Order mismatch in version')
                raise
        data = "\n".join([",".join(map(str, row)) for row in rows])

        load_BQ_from_CSV(client, args.bqdataset_name, table, data, schema)


def upload_instance(cur, args):
    table = "instance"
    print(f'Populating table {table}')
    schema = get_schema(cur, args, table)
    client = bigquery.Client(project=args.project)

    if BQ_table_exists(client, args.project, args.bqdataset_name, table):
        delete_BQ_Table(client, args.project, args.bqdataset_name, table)
    result = create_BQ_table(client, args.project, args.bqdataset_name, table, schema)

    count =0
    increment = 50000

    cur.execute("""
        SELECT * 
        FROM instance

        WHERE idc_version_number = (%s)
        """, (args.version,))

    while True:
        rows = cur.fetchmany(increment)
        if len(rows) == 0:
            break

        # Make sure that the schema and retrieved data have the same order
        col_order = cur.description
        for col, sch in zip(col_order, schema):
            try:
                assert col.name == sch['name']
            except:
                errlogger.error('Order mismatch in version')
                raise
        data = "\n".join([",".join(map(str, row)) for row in rows])

        load_BQ_from_CSV(client, args.bqdataset_name, table, data, schema)
        count += len(rows)
        print(f'Uploaded {count} instances')


def upload_to_bq(args):
    conn = psycopg2.connect(dbname=settings.DATABASE_NAME, user=settings.DATABASE_USERNAME,
                            password=settings.DATABASE_PASSWORD, host=settings.DATABASE_HOST)
    with conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            upload_version(cur, args)
            upload_collection(cur, args)
            upload_patient(cur, args)
            upload_study(cur, args)
            upload_series(cur, args)
            upload_instance(cur, args)
            pass

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=1, help='Version to upload')
    parser.add_argument('--project', default='idc-dev-etl')
    args = parser.parse_args()
    parser.add_argument('--bqdataset_name', default=f"idc_v{args.version}", help="BQ dataset of table")
    args = parser.parse_args()


    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/copy_staging_log.log'.format(os.environ['PWD']))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/copy_staging_err.log'.format(os.environ['PWD']))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    upload_to_bq(args)





