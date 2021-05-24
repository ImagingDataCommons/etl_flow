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

# One time use script to repair V2 instance, series and study UUIDs. Replace
# hex format UUIDs with standard, 8-4-4-4-12 format

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





def repair_study_uuids(cur, args):
    level = 'study'
    query = f"""
                UPDATE {level}{args.suffix}
                SET {level}_uuid = gen_random_uuid()
                where {level}_timestamp > to_date('2020/06/02', 'YYYY/MM/DD')
                and length({level}_uuid) < 36"""
    cur.execute(query)

    cur.connection.commit()

def repair_series_uuids(cur, args):
    level = 'series'
    query = f"""
                 UPDATE {level}{args.suffix}
                 SET {level}_uuid = gen_random_uuid()
                 where {level}_timestamp > to_date('2020/06/02', 'YYYY/MM/DD')
                 and length({level}_uuid) < 36"""
    cur.execute(query)

    cur.connection.commit()

def repair_instance_uuids(cur, args):
    level = 'instance'
    query = f"""
                UPDATE {level}{args.suffix}
                SET {level}_uuid = 
                    substr(gcs_url,14,8) || '-' ||
                    substr(gcs_url,22,4) || '-' ||
                    substr(gcs_url,26,4) || '-' ||
                    substr(gcs_url,30,4) || '-' ||
                    substr(gcs_url,34,12)
                where {level}_timestamp > to_date('2020/06/02', 'YYYY/MM/DD')"""
    cur.execute(query)

    cur.connection.commit()

def repair_v2_uuids(args):
    conn = psycopg2.connect(dbname=settings.DATABASE_NAME, user=settings.DATABASE_USERNAME,
                            password=settings.DATABASE_PASSWORD, host=settings.DATABASE_HOST)
    with conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            repair_study_uuids(cur, args)
            repair_series_uuids(cur, args)
            repair_instance_uuids(cur, args)


if __name__ == '__main__':
    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/rename_v2_blobs_log.log'.format(os.environ['PWD']))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/rename_v2_blobs_err.log'.format(os.environ['PWD']))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    parser = argparse.ArgumentParser()
    parser.add_argument('--suffix', default='')
    args = parser.parse_args()

    print("{}".format(args), file=sys.stdout)

    repair_v2_uuids(args)
