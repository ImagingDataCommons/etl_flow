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

# Validate that UUIDs that we have generated but not yet registered with CRDC are
# not already in use by CRDC

import os
import json
import logging
from logging import INFO
import argparse
import requests
from utilities.tcia_helpers import get_url

from python_settings import settings

import psycopg2
from psycopg2.extras import DictCursor, RealDictCursor

def is_unique(uuid):
    url = f"https://nci-crdc.datacommons.io/index/index/{uuid}"
    result =  requests.get(url)
    unique = result.status_code == 404 and result.json() == {"error": "No bundle found"}
    return unique


def validate_instance_uuids(cur,args):
    table = "instance"
    print(f'Populating table {table}')

    try:
        dones = set(open(f'./logs/validate_instance_uuids_done.log').read().splitlines())
    except:
        dones = []

    done = open(f'./logs/validate_instance_uuids_done.log', 'a')
    failure = open(f'./logs/validate_instance_uuids_failures.log', 'a')

    count = 0
    increment = 50000
    query = """
                SELECT uuid
                FROM instance
                WHERE init_idc_version > 1
            """

    cur.execute(query)

    while True:
        uuids = [row['uuid'] for row in cur.fetchmany(increment)]
        if len(uuids) == 0:
            break
        for uuid in uuids:
            if uuid not in dones:
                if is_unique(uuid):
                    done.write(f'{uuid}\n')
                else:
                    failure.write(f'{uuid}\n')
        count += len(uuids)
        print(f'Validated {count} uuids')


def validate_unique_uuids(args):
    conn = psycopg2.connect(dbname=args.db, user=settings.LOCAL_DATABASE_USERNAME,
                            password=settings.LOCAL_DATABASE_PASSWORD, host=settings.LOCAL_DATABASE_HOST)
    with conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            # upload_version(cur, args)
            # upload_collection(cur, args)
            # upload_patient(cur, args)
            # upload_study(cur, args)
            # upload_series(cur, args)
            validate_instance_uuids(cur, args)
            pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=3, help='Version to upload')
    args = parser.parse_args()
    parser.add_argument('--db', default=f'idc_v{args.version}', help="Database to access")
    parser.add_argument('--project', default='idc-dev-etl')
    args = parser.parse_args()

    print('args: {}'.format(args))

    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/validate_uuid_uniqueness_log.log'.format(os.environ['PWD']))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/validate_uuid_uniqueness_err.log'.format(os.environ['PWD']))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    validate_unique_uuids(args)

