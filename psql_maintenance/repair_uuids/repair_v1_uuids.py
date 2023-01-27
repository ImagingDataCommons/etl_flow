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

# One time use script to repair V1 instance, series and study UUIDs that were
# incorrectly copied from the V1 aux table.

import sys
import os
import argparse
import logging
from logging import INFO
from google.cloud import bigquery, storage

from python_settings import settings
import settings as etl_settings

settings.configure(etl_settings)
assert settings.configured
import psycopg2
from psycopg2.extras import DictCursor


def populate_uuid_table(cur, args, level):
    client = bigquery.Client()
    query = f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
 --           WHERE  table_schema = 'public'
 --            AND table_name = '{level}_uuids')
              WHERE table_name = '{level}_uuids_2')"""
    cur.execute(query)

    table_exists = cur.fetchone()['exists']
    if not table_exists:
        query = f"""
            CREATE TABLE {level}_uuids_2 (
                collection text,
                uid text,
                uuid text)"""
        cur.execute(query)

        # Get the table data from BQ
        query = f"""
            SELECT *
            FROM {args.project}.{args.bqdataset}.{level}_uuids_2"""
        query_job = client.query(query)

        uuids = [f"('{row[0]}','{row[1]}','{row[2]}')" for row in query_job]
        uuids = ','.join(uuids)

        # Insert the uuids into the psql table
        query = f"""
            INSERT INTO {level}_uuids_2 VALUES {uuids}"""
        cur.execute(query)

        cur.connection.commit()

def repair_instance_uuids(cur, args):
    metadata = populate_uuid_table(cur, args, 'instance')

    query = f"""
                UPDATE instance
                SET instance_uuid = u.uuid
                from instance_uuids_2 as u
                where sop_instance_uid = u.uid and not instance_uuid = u.uuid"""
    cur.execute(query)

    cur.connection.commit()


def repair_series_uuids(cur, args):
    metadata = populate_uuid_table(cur, args, 'series')

    query = f"""
                UPDATE series
                SET series_uuid = u.uuid
                from series_uuids_2 as u
                where series_instance_uid = u.uid and not series_uuid = u.uuid"""
    cur.execute(query)

    cur.connection.commit()


def repair_study_uuids(cur, args):
    metadata = populate_uuid_table(cur, args, 'study')

    query = f"""
                UPDATE study
                SET study_uuid = u.uuid
                from study_uuids_2 as u
                where study_instance_uid = u.uid  and not study_uuid = u.uuid"""
    cur.execute(query)

    cur.connection.commit()

def repair_v1_uuids(args):
    conn = psycopg2.connect(dbname=settings.DATABASE_NAME, user=settings.DATABASE_USERNAME,
                            password=settings.DATABASE_PASSWORD, host=settings.DATABASE_HOST)
    with conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            repair_study_uuids(cur, args)
            repair_series_uuids(cur, args)
            repair_instance_uuids(cur, args)


if __name__ == '__main__':
    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/rename_v1_blobs_log.log'.format(os.environ['PWD']))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/rename_v1_blobs_err.log'.format(os.environ['PWD']))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    parser = argparse.ArgumentParser()
    parser.add_argument('--project', default="idc-dev-etl")
    parser.add_argument('--bqdataset', default='whc_dev')
    parser.add_argument('--table', default='auxiliary_metadata_with_correct_uuids')
    parser.add_argument('--bucket', default='idc_dev')
    parser.add_argument('--dones', default='./logs/rename_v1_blobs_dones.log')
    args = parser.parse_args()

    print("{}".format(args), file=sys.stdout)

    repair_v1_uuids(args)
