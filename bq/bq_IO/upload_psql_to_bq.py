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
import json
import logging
from logging import INFO
import argparse
from google.cloud import bigquery
from utilities.bq_helpers import BQ_table_exists, create_BQ_table, delete_BQ_Table, load_BQ_from_CSV, load_BQ_from_json

from python_settings import settings

import psycopg2
from psycopg2.extras import DictCursor, RealDictCursor

# psql to bq type name conversions:
type_name_conversion = {
    "integer": "INTEGER",
    "bigint": "INT64",
    "boolean": "BOOLEAN",
    "character varying": "STRING",
    "nextval('instance_id_seq'::regclass)": "INTEGER",
    "text": "STRING",
    "timestamp without time zone": "DATETIME",
    "USER-DEFINED": "ARRAY"
}

record_type_name_conversion = {
    "hashes": "STRING",
    "sources": "BOOLEAN",
    "versions": "INTEGER"
}

def get_schema(cur, args, table):
    # Get the psql schema fields
    query = f"""
        SELECT column_name, data_type, character_maximum_length, column_default, is_nullable
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE table_name = '{table}'
        ORDER BY ordinal_position
    """
    cur.execute(query)
    column_data = cur.fetchall()

    # Separate query for the descriptions
    query = f"""
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
            cols.table_catalog    = '{args.db}'
            AND cols.table_name   = '{table}'
            AND cols.table_schema = 'public'
        ORDER BY ordinal_position
    """
    cur.execute(query)
    descriptions = cur.fetchall()
    schema = []
    for col,desc in zip(column_data, descriptions):
        type = "RECORD" if col["data_type"] == "USER-DEFINED" else type_name_conversion[col["data_type"]]
        mode = "NULLABLE" if col["is_nullable"] == "YES" else "REQUIRED"
        #### Hack alert ####
        if col["column_name"] == 'source':
            type="STRING"
            mode="NULLABLE"

        try:
            s = {
                "name": col["column_name"],
                "description": desc['column_comment'],
                "mode": mode,
                "type": type,
            }
            if type == "RECORD":
                if col['column_name'] == 'sources':
                    s['fields'] = [
                        {
                            "name": 'tcia',
                            "type": record_type_name_conversion[col["column_name"]],
                            "mode": "NULLABLE"
                        },
                        {
                            "name": 'path',
                            "type": record_type_name_conversion[col["column_name"]],
                            "mode": "NULLABLE"
                        }
                    ]
                elif col['column_name'] == 'hashes':
                    s['fields'] = [
                        {
                            "name": 'tcia',
                            "type": record_type_name_conversion[col["column_name"]],
                            "mode": "NULLABLE"
                        },
                        {
                            "name": 'path',
                            "type": record_type_name_conversion[col["column_name"]],
                            "mode": "NULLABLE"
                        },
                        {
                            "name": 'all_sources',
                            "type": record_type_name_conversion[col["column_name"]],
                            "mode": "NULLABLE"
                        }
                    ]
                elif col['column_name'] == 'source_statuses':
                    s['fields'] = [
                        {
                            "name": 'tcia',
                            "type": 'RECORD',
                            "mode": "NULLABLE",
                            "fields": [
                                {
                                    "name": 'min_timestamp',
                                    "type": 'DateTime',
                                    "mode": "NULLABLE",
                                },
                                {
                                    "name": 'max_timestamp',
                                    "type": 'DateTime',
                                    "mode": "NULLABLE",
                                },
                                {
                                    "name": 'revised',
                                    "type": 'BOOLEAN',
                                    "mode": "NULLABLE",
                                },
                                {
                                    "name": 'done',
                                    "type": 'BOOLEAN',
                                    "mode": "NULLABLE",
                                },
                                {
                                    "name": 'is_new',
                                    "type": 'BOOLEAN',
                                    "mode": "NULLABLE",
                                },
                                {
                                    "name": 'expanded',
                                    "type": 'BOOLEAN',
                                    "mode": "NULLABLE",
                                },
                                {
                                    "name": 'version',
                                    "type": 'INTEGER',
                                    "mode": "NULLABLE",
                                }
                            ]
                        },
                        {
                            "name": 'path',
                            "type": 'RECORD',
                            "mode": "NULLABLE",
                            "fields": [
                                {
                                    "name": 'min_timestamp',
                                    "type": 'DateTime',
                                    "mode": "NULLABLE",
                                },
                                {
                                    "name": 'max_timestamp',
                                    "type": 'DateTime',
                                    "mode": "NULLABLE",
                                },
                                {
                                    "name": 'revised',
                                    "type": 'BOOLEAN',
                                    "mode": "NULLABLE",
                                },
                                {
                                    "name": 'done',
                                    "type": 'BOOLEAN',
                                    "mode": "NULLABLE",
                                },
                                {
                                    "name": 'is_new',
                                    "type": 'BOOLEAN',
                                    "mode": "NULLABLE",
                                },
                                {
                                    "name": 'expanded',
                                    "type": 'BOOLEAN',
                                    "mode": "NULLABLE",
                                },
                                {
                                    "name": 'version',
                                    "type": 'INTEGER',
                                    "mode": "NULLABLE",
                                }
                            ]
                        }
                    ]

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
        SELECT json_agg(json)
        FROM (
            SELECT * from version
        )  as json
        """)
    rows = cur.fetchall()
    json_rows = [json.dumps(row) for row in rows[0][0]]
    data = "\n".join(json_rows)
    load_BQ_from_json(client, args.project, args.bqdataset_name, table, data, schema)


def upload_program(cur, args):
    table = "program"
    print(f'Populating table {table}')
    schema = get_schema(cur, args, table)
    client = bigquery.Client(project=args.project)

    if BQ_table_exists(client, args.project, args.bqdataset_name, table):
        delete_BQ_Table(client, args.project, args.bqdataset_name, table)
    result = create_BQ_table(client, args.project, args.bqdataset_name, table, schema)

    cur.execute("""
        SELECT json_agg(json)
        FROM (
            SELECT * from program
        )  as json
        """)
    rows = cur.fetchall()
    json_rows = [json.dumps(row) for row in rows[0][0]]
    data = "\n".join(json_rows)
    load_BQ_from_json(client, args.project, args.bqdataset_name, table, data, schema)


def upload_collection(cur, args):
    table = "collection"
    print(f'Populating table {table}')
    schema = get_schema(cur, args, table)
    client = bigquery.Client(project=args.project)

    if BQ_table_exists(client, args.project, args.bqdataset_name, table):
        delete_BQ_Table(client, args.project, args.bqdataset_name, table)
    result = create_BQ_table(client, args.project, args.bqdataset_name, table, schema)

    cur.execute("""
        SELECT json_agg(json)
        FROM (
            SELECT * from collection
        )  as json
        """)
    rows = cur.fetchall()
    json_rows = [json.dumps(row) for row in rows[0][0]]
    data = "\n".join(json_rows)
    load_BQ_from_json(client, args.project, args.bqdataset_name, table, data, schema)




def upload_patient(cur, args):
    table = "patient"
    print(f'Populating table {table}')
    schema = get_schema(cur, args, table)
    client = bigquery.Client(project=args.project)

    if BQ_table_exists(client, args.project, args.bqdataset_name, table):
        delete_BQ_Table(client, args.project, args.bqdataset_name, table)
    result = create_BQ_table(client, args.project, args.bqdataset_name, table, schema)

    cur.execute("""
        SELECT json_agg(json)
        FROM (
            SELECT * from patient
        )  as json
        """)
    rows = cur.fetchall()
    json_rows = [json.dumps(row) for row in rows[0][0]]
    data = "\n".join(json_rows)
    load_BQ_from_json(client, args.project, args.bqdataset_name, table, data, schema)


def upload_study(cur, args):
    table = "study"
    print(f'Populating table {table}')
    schema = get_schema(cur, args, table)
    client = bigquery.Client(project=args.project)

    if BQ_table_exists(client, args.project, args.bqdataset_name, table):
        delete_BQ_Table(client, args.project, args.bqdataset_name, table)
    result = create_BQ_table(client, args.project, args.bqdataset_name, table, schema)

    cur.execute("""
        SELECT json_agg(json)
        FROM (
            SELECT * from study
        )  as json
        """)
    rows = cur.fetchall()
    json_rows = [json.dumps(row) for row in rows[0][0]]
    data = "\n".join(json_rows)
    load_BQ_from_json(client, args.project, args.bqdataset_name, table, data, schema)


def upload_series(cur, args):
    table = "series"
    print(f'Populating table {table}')
    schema = get_schema(cur, args, table)
    client = bigquery.Client(project=args.project)

    if BQ_table_exists(client, args.project, args.bqdataset_name, table):
        delete_BQ_Table(client, args.project, args.bqdataset_name, table)
    result = create_BQ_table(client, args.project, args.bqdataset_name, table, schema)

    count =0
    increment = 50000
    cur.execute("""
            SELECT row_to_json(json)
            FROM (
                SELECT * from series
            )  as json
            """)

    while True:
        rows = cur.fetchmany(increment)
        if len(rows) == 0:
            break
        json_rows = [json.dumps(row[0]) for row in rows]
        data = "\n".join(json_rows)
        load_BQ_from_json(client, args.project, args.bqdataset_name, table, data, schema)
        count += len(rows)
        print(f'Uploaded {count} series')


def upload_instance(cur, args):
    table = "instance"
    print(f'Populating table {table}')
    schema = get_schema(cur, args, table)
    client = bigquery.Client(project=args.project)

    if BQ_table_exists(client, args.project, args.bqdataset_name, table):
        delete_BQ_Table(client, args.project, args.bqdataset_name, table)
    result = create_BQ_table(client, args.project, args.bqdataset_name, table, schema)

    count =0
    increment = 250000
    cur.execute("""
            SELECT row_to_json(json)
            FROM (
                SELECT * from instance
            )  as json
            """)

    while True:
        rows = cur.fetchmany(increment)
        if len(rows) == 0:
            break
        json_rows = [json.dumps(row[0]) for row in rows]
        data = "\n".join(json_rows)
        load_BQ_from_json(client, args.project, args.bqdataset_name, table, data, schema)
        count += len(rows)
        print(f'Uploaded {count} series')

def upload_retired(cur, args):
    table = "retired"
    print(f'Populating table {table}')
    schema = get_schema(cur, args, table)
    client = bigquery.Client(project=args.project)

    if BQ_table_exists(client, args.project, args.bqdataset_name, table):
        delete_BQ_Table(client, args.project, args.bqdataset_name, table)
    result = create_BQ_table(client, args.project, args.bqdataset_name, table, schema)

    cur.execute("""
        SELECT json_agg(json)
        FROM (
            SELECT * from retired
        )  as json
        """)
    rows = cur.fetchall()
    json_rows = [json.dumps(row) for row in rows[0][0]]
    data = "\n".join(json_rows)
    load_BQ_from_json(client, args.project, args.bqdataset_name, table, data, schema)


def upload_table(cur, args, table):
    print(f'Populating table {table}')
    schema = get_schema(cur, args, table)
    client = bigquery.Client(project=args.project)

    if BQ_table_exists(client, args.project, args.bqdataset_name, table):
        delete_BQ_Table(client, args.project, args.bqdataset_name, table)
    result = create_BQ_table(client, args.project, args.bqdataset_name, table, schema)

    count =0
    increment = 250000
    query = f"""
            SELECT row_to_json(json)
            FROM (
                SELECT * from {table}
            )  as json
            """
    cur.execute(query)

    while True:
        rows = cur.fetchmany(increment)
        if len(rows) == 0:
            break
        json_rows = [json.dumps(row[0]) for row in rows]
        data = "\n".join(json_rows)
        load_BQ_from_json(client, args.project, args.bqdataset_name, table, data, schema)
        count += len(rows)
        print(f'Uploaded {count} {table}s')




def upload_to_bq(args):
    conn = psycopg2.connect(dbname=args.db, user=args.user, port=args.port,
                            password=args.password, host=args.host)
    with conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            for table in args.tables:
                upload_table(cur, args, table)
            pass

# if __name__ == '__main__':
#     parser = argparse.ArgumentParser()
#     parser.add_argument('--version', default=5, help='Version to upload')
#     parser.add_argument('--db', default='idc_v0', help="Database to access")
#     parser.add_argument('--project', default='idc-dev-etl')
#     args = parser.parse_args()
#     parser.add_argument('--bqdataset_name', default=f"idc_v{args.version}", help="BQ dataset of table")
#     parser.add_argument('--user', default=settings.CLOUD_USERNAME)
#     parser.add_argument('--password', default=settings.CLOUD_PASSWORD)
#     parser.add_argument('--host', default=settings.CLOUD_HOST)
#     parser.add_argument('--port', default=settings.CLOUD_PORT)
#     args = parser.parse_args()
#
#     print('args: {}'.format(args))
#
#
#     rootlogger = logging.getLogger('root')
#     root_fh = logging.FileHandler('{}/logs/copy_staging_log.log'.format(os.environ['PWD']))
#     rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
#     rootlogger.addHandler(root_fh)
#     root_fh.setFormatter(rootformatter)
#     rootlogger.setLevel(INFO)
#
#     errlogger = logging.getLogger('root.err')
#     err_fh = logging.FileHandler('{}/logs/copy_staging_err.log'.format(os.environ['PWD']))
#     errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
#     errlogger.addHandler(err_fh)
#     err_fh.setFormatter(errformatter)
#
#     upload_to_bq(args)





