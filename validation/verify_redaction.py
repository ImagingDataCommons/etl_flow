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
# The BQ dataset containing the tables to be duplicated is specified in the .env.idc-dev-etl file (maybe not the best place).
# The bigquery_uri engine is configured to access that dataset.


import json
from google.cloud import bigquery
from utilities.tcia_helpers import get_TCIA_patients_per_collection, get_TCIA_studies_per_patient, \
    get_TCIA_series_per_study, get_TCIA_instance_uids_per_series, get_TCIA_single_instance
import os
import logging
from logging import INFO
import argparse

import pydicom
from pydicom.errors import InvalidDicomError

from python_settings import settings

import psycopg2
from psycopg2.extras import DictCursor, RealDictCursor

def verify_redactions(args):
    conn = psycopg2.connect(dbname=args.db, user=args.user, port=args.port,
                            password=args.password, host=args.host)
    with conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            query = f"""
                    SELECT row_to_json(json)
                    FROM (
                        SELECT * from redacted_collections
                    )  as json
                    """
            cur.execute(query)

            rows = cur.fetchall()
            collections = sorted([row[0]['tcia_api_collection_id'] for row in rows])
            for collection in collections:
                patients = [patient['PatientId'] for patient in get_TCIA_patients_per_collection(collection)]
                print(f'\t{collection} has {len(patients)} patients')
                studies = [study['StudyInstanceUID'] for study in get_TCIA_studies_per_patient(collection, patients[0])]
                print(f'\t\t{patients[0]} has {len(studies)} studies')
                seriess = [series['SeriesInstanceUID'] for series in get_TCIA_series_per_study(collection, patients[0], studies[0])]
                print(f'\t\t\t{studies[0]} has {len(seriess)} series')
                instances = [instance['SOPInstanceUID'] for instance in get_TCIA_instance_uids_per_series(seriess[0])]
                print(f'\t\t\t\t{seriess[0]} has {len(instances)} instances')
                result = get_TCIA_single_instance(seriess[0], instances[0])
                if result.status_code == 200:
                    with open('dcm.dcm', 'wb') as f:
                        f.write(result.content)
                    SOPInstanceUID = pydicom.dcmread('dcm.dcm',
                                                     stop_before_pixels=True).SOPInstanceUID
                    if SOPInstanceUID == instances[0]:
                        print(f'\t\t\t\t\tCollection {collection} NOT redacted')
                    else:
                        print(f'\t\t\t\t\tCollection {collection} NOT redacted but read error?')
                else:
                    print(f'\t\t\t\t\tCollection {collection} redacted')
                pass





if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=7, help='Version to upload')
    args = parser.parse_args()
    parser.add_argument('--db', default=f'idc_v{args.version}', help="Database to access")
    parser.add_argument('--project', default='idc-dev-etl')
    args = parser.parse_args()
    parser.add_argument('--bqdataset_name', default=f"idc_v{args.version}", help="BQ dataset of table")
    parser.add_argument('--server', default='CLOUD')
    parser.add_argument('--user', default=settings.CLOUD_USERNAME)
    parser.add_argument('--password', default=settings.CLOUD_PASSWORD)
    parser.add_argument('--host', default=settings.CLOUD_HOST)
    parser.add_argument('--port', default=settings.CLOUD_PORT)
    args = parser.parse_args()

    print('args: {}'.format(args))


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

    verify_redactions(args)





