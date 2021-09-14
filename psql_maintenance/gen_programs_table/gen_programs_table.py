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

import argparse
import sys
import os
import json
from time import sleep
from googleapiclient.errors import HttpError
from google.cloud import storage
from utilities.gch_helpers import get_dicom_store, get_dataset, create_dicom_store, create_dataset
import logging
from logging import INFO
from googleapiclient import discovery

from python_settings import settings

import psycopg2
from psycopg2.extras import DictCursor, RealDictCursor


def gen_table(args):
    conn = psycopg2.connect(dbname='idc_v4', user=settings.CLOUD_USERNAME, port=settings.CLOUD_PORT,
                            password=settings.CLOUD_PASSWORD, host=settings.CLOUD_HOST)
    with conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            collections = open(args.src_file).read().splitlines()

            try:
                query = f"""
                    DROP table {args.dst_table}"""
                cur.execute(query)
            except:
                pass

            query = f"""
                CREATE TABLE {args.dst_table} (
                    tcia_wiki_collection_id VARCHAR PRIMARY KEY,
                    program VARCHAR NOT NULL)
                """
            cur.execute(query)


            for collection in collections:
                query = f"""
                    INSERT INTO {args.dst_table} (tcia_wiki_collection_id, program)
                    VALUES ('{collection.split(',')[0]}', '{collection.split(',')[1]}')
                """
                cur.execute(query)

            conn.commit()


if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--src_file', default='./TCIA_collection_program_assignment.csv')
    parser.add_argument('--dst_table', default='program')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)

    gen_table(args)