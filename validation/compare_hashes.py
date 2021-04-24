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

# One time use script to gen Merkle (hierarchical) hash for series, studies, patients, collections and versions.

import sys
import os
import argparse
import logging
from logging import INFO
from utilities.tcia_helpers import get_hash, get_access_token

from python_settings import settings
import settings as etl_settings

settings.configure(etl_settings)
assert settings.configured
import psycopg2
from psycopg2.extras import DictCursor



def compare_series_hashes(cur, args, study_instance_uid):
    query = f"""
        SELECT series_instance_uid, series_hash
        FROM study{args.suffix} as st
        JOIN series{args.suffix} as se
        ON st.id = se.study_id
        WHERE st.idc_version_number=2 AND st.study_instance_uid ='{study_instance_uid}'
      """

    cur.execute(query)
    series = cur.fetchall()
    access_token = get_access_token(url = "https://public-dev.cancerimagingarchive.net/nbia-api/oauth/token")['access_token']

    for row in series:
        result = get_hash({'Series': row[0]}, access_token=access_token)
        if not result.status_code == 200:
            print('\t\t\t{:32} IDC: {}, error: {}, reason: {}'.format(row[0], row[1], result.status_code, result.reason))
            rootlogger.info('\t\t\t%-32s IDC: %s, error: %s, reason: %s', row[0], row[1], result.status_code, result.reason)
        else:
            nbia_hash = result.text
            print('\t\t\t{:32} IDC: {}, NBIA: {}; {}'.format(row[0], row[1], nbia_hash, row[1]==nbia_hash))
            rootlogger.info('\t\t\t%-32s IDC: %s, NBIA: %s; %s', row[0], row[1], nbia_hash, row[1]==nbia_hash)


def compare_study_hashes(cur, args, tcia_api_collection_id, submitter_case_id):
    query = f"""
        SELECT study_instance_uid, study_hash
        FROM collection{args.suffix} as c 
        JOIN patient{args.suffix} as p 
        ON c.id = p.collection_id
        JOIN study{args.suffix} as st
        ON p.id = st.patient_id
        WHERE p.idc_version_number=2 AND c.tcia_api_collection_id = '{tcia_api_collection_id}' AND p.submitter_case_id ='{submitter_case_id}'
      """

    cur.execute(query)
    studies = cur.fetchall()
    access_token = get_access_token(url = "https://public-dev.cancerimagingarchive.net/nbia-api/oauth/token")['access_token']

    for row in studies:
        result = get_hash({'Study': row[0]}, access_token=access_token)
        if not result.status_code == 200:
            print('\t\t{:32} IDC: {}, error: {}, reason: {}'.format(row[0], row[1], result.status_code, result.reason))
            rootlogger.info('\t\t%-32s IDC: %s, error: %s, reason: %s', row[0], row[1], result.status_code, result.reason)
        else:
            nbia_hash = result.text
            print('\t\t{:32} IDC: {}, NBIA: {}; {}'.format(row[0], row[1], nbia_hash, row[1]==nbia_hash))
            rootlogger.info('\t\t%-32s IDC: %s, NBIA: %s; %s', row[0], row[1], nbia_hash, row[1]==nbia_hash)
            if not row[1]==nbia_hash:
                if nbia_hash == 'd41d8cd98f00b204e9800998ecf8427e':
                    print('\t\t{:32} Skip expansion'.format(""))
                    rootlogger.info('\t\t%-32s Skip expansion', "")
                else:
                    compare_series_hashes(cur, args, row[0])


def compare_patient_hashes(cur, args, tcia_api_collection_id):
    query = f"""
        SELECT submitter_case_id, patient_hash
        FROM collection{args.suffix} as c 
        JOIN patient{args.suffix} as p 
        ON c.id = p.collection_id
        WHERE p.idc_version_number=2 AND c.tcia_api_collection_id = '{tcia_api_collection_id}'
      """

    cur.execute(query)
    patients = cur.fetchall()
    access_token = get_access_token(url = "https://public-dev.cancerimagingarchive.net/nbia-api/oauth/token")['access_token']

    for row in patients:
        result = get_hash({'Collection': tcia_api_collection_id, 'PatientID': row[0]}, access_token=access_token)
        if not result.status_code == 200:
            print('\t{:32} IDC: {}, error: {}, reason: {}'.format(row[0], row[1], result.status_code, result.reason))
            rootlogger.info('\t%-32s IDC: %s, error: %s, reason: %s', row[0], row[1], result.status_code, result.reason)
        else:
            nbia_hash = result.text
            print('\t{:32} IDC: {}, NBIA: {}; {}'.format(row[0], row[1], nbia_hash, row[1]==nbia_hash))
            rootlogger.info('\t%-32s IDC: %s, NBIA: %s; %s', row[0], row[1], nbia_hash, row[1]==nbia_hash)
            if not row[1]==nbia_hash:
                if nbia_hash == 'd41d8cd98f00b204e9800998ecf8427e':
                    print('\t{:32} Skip expansion'.format(""))
                    rootlogger.info('\t%-32s Skip expansion', "")
                else:
                    compare_study_hashes(cur, args, tcia_api_collection_id, row[0])


def compare_collection_hashes(cur, args):
    query = f"""
        SELECT tcia_api_collection_id, collection_hash
        FROM collection{args.suffix}
        WHERE idc_version_number=2
      """

    cur.execute(query)
    collections = cur.fetchall()
    access_token = get_access_token(url = "https://public-dev.cancerimagingarchive.net/nbia-api/oauth/token")['access_token']

    for row in collections:
        result = get_hash({'Collection': row[0]}, access_token=access_token)
        if not result.status_code == 200:
            print('{:32} IDC: {}, error: {}, reason: {}'.format(row[0], row[1], result.status_code, result.reason))
            rootlogger.info('%-32s IDC: %s, error: %s, reason: %s', row[0], row[1], result.status_code, result.reason)
        else:
            nbia_hash = result.text
            print('{:32} IDC: {}, NBIA: {}; {}'.format(row[0], row[1], nbia_hash, row[1]==nbia_hash))
            rootlogger.info('%-32s IDC: %s, NBIA: %s; %s', row[0], row[1], nbia_hash, row[1]==nbia_hash)
            if not row[1]==nbia_hash:
                if nbia_hash == 'd41d8cd98f00b204e9800998ecf8427e':
                    print('{:32} Skip expansion'.format(""))
                    rootlogger.info('%-32s Skip expansion', "")
                else:
                    compare_patient_hashes(cur, args, row[0])


def compare_hashes(args):
    conn = psycopg2.connect(dbname=settings.DATABASE_NAME, user=settings.DATABASE_USERNAME,
                            password=settings.DATABASE_PASSWORD, host=settings.DATABASE_HOST)
    with conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            compare_collection_hashes(cur, args)
            # compare_series_hashes(cur, args)
            # compare_study_hashes(cur, args)
            # compare_patient_hashes(cur, args)
            pass



if __name__ == '__main__':
    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/compare_hashes_log.log'.format(os.environ['PWD']))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/compare_hashes_err.log'.format(os.environ['PWD']))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    parser = argparse.ArgumentParser()
    parser.add_argument('--suffix', default="")
    args = parser.parse_args()

    print("{}".format(args), file=sys.stdout)

    compare_hashes(args)
