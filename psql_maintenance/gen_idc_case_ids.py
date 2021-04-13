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

# One time use script to generate IDC case uuids for patients already in
# the DB.

import sys
import os
import argparse
from pathlib import Path
import time
from datetime import datetime, timezone, timedelta
import logging
from logging import INFO
from uuid import uuid4
from idc.models import Version, Collection, Patient, Study, Series, Instance, sql_engine
from sqlalchemy import select
from sqlalchemy.orm import Session

# from python_settings import settings
# import settings as etl_settings
#
# settings.configure(etl_settings)
# assert settings.configured
# import psycopg2
# from psycopg2.extras import DictCursor


def gen_v1_v2_ids(sess, args, version1, version1_collection, version2, version2_collection):
    rootlogger.info("Collection %s; versions 1 and 2; ", version2_collection.tcia_api_collection_id)
    for v2_patient in version2_collection.patients:
        v1_patient = next(patient for patient in version1_collection.patients \
                          if patient.submitter_case_id==v2_patient.submitter_case_id)
        v2_patient.idc_case_id = v1_patient.idc_case_id = uuid4()
    sess.commit()


def gen_v2_ids(sess, args, version2, version2_collection):
    rootlogger.info("Collection %s; version 2; ", version2_collection.tcia_api_collection_id)
    for v2_patient in version2_collection.patients:
         v2_patient.idc_case_id = uuid4()
    sess.commit()


def gen_ids(sess, args, version1, version2):
    # Collections to skip
    skips = open(args.skips).read().splitlines()

    version1_collections = {collection.tcia_api_collection_id:collection for collection in version1.collections}
    for collection in version2.collections:
        if not collection.tcia_api_collection_id in skips:
            if collection.tcia_api_collection_id in version1_collections:
                gen_v1_v2_ids(sess, args, version1, version1_collections[collection.tcia_api_collection_id],
                             version2, collection)
            else:
                gen_v2_ids(sess, args, version2,  collection)
        else:
            rootlogger.info('***Skipped %s', collection.tcia_api_collection_id)


def gen_idc_case_ids(args):
    # Find the version to which case IDs are to be added
    with Session(sql_engine) as sess:
        stmt = select(Version).distinct()
        result = sess.execute(stmt)

        rows = []
        for row in result:
            rows.append(row)
        if rows[0][0].idc_version_number == 1:
            # We've at least started working on vnext
            version1 = rows[0][0]
            version2 = rows[1][0]
        else:
            version1 = rows[1][0]
            version2 = rows[0][0]

        gen_ids(sess, args, version1, version2)

if __name__ == '__main__':
    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/gen_idc_case_ids_log.log'.format(os.environ['PWD']))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/gen_idc_case_ids_err.log'.format(os.environ['PWD']))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    parser = argparse.ArgumentParser()
    parser.add_argument('--skips', default='./logs/idc_case_ids_skips.txt', help='Collections to skip')
    parser.add_argument('--num_processes', default=0, help="Number of concurrent processes")
    args = parser.parse_args()

    # Directory to which to download files from TCIA/NBIA
    print("{}".format(args), file=sys.stdout)

    gen_idc_case_ids(args)
