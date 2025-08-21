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

# Script updates init_idc_version and rev_idc_version in the "excluded"
# (but about to be public) collections. The general form can be adapted to
# perform similar tasks.


import sys
import argparse
from utilities.sqlalchemy_helpers import sa_session
from utilities.logging_config import successlogger,progresslogger,errlogger
from python_settings import settings


import os
from pathlib import Path
import argparse
from utilities.logging_config import successlogger, progresslogger, errlogger
import pydicom
from ingestion.utilities.utils import create_prestaging_bucket, md5_hasher
from google.cloud import bigquery, storage
from google.api_core.exceptions import Conflict
from python_settings import settings
import settings as etl_settings
if not settings.configured:
    settings.configure(etl_settings)
    assert settings.configured
from subprocess import run

def update_collection(sess):
    client = bigquery.Client()
    query = f"""
WITH wths AS(
    SELECT c.uuid
    FROM collection c
    WHERE collection_id IN (
        'CBIS-DDSM',
        'CC-Radiomics-Phantom',
        'CC-Radiomics-Phantom-2',
        'CC-Radiomics-Phantom-3',
        'QIBA-CT-Liver-Phantom',
        'QIN PET Phantom',
        'RIDER PHANTOM MRI',
        'RIDER PHANTOM PET-CT'
    )
)
UPDATE collection
SET init_idc_version=22, rev_idc_version=22
FROM wths
WHERE collection.uuid = wths.uuid
RETURNING * 
"""
    try:
        keep = sess.execute(query).all()
        successlogger.info(f'{len(keep)} collections')
        for row in keep:
            print(row.collection_id, row.uuid, row.init_idc_version, row.rev_idc_version)
    except Exception as e:
        print(f"Error precounting: {e}")
        raise
    return


def update_patient(sess):
    client = bigquery.Client()
    query = f"""
WITH wths AS(
    SELECT p.uuid
    FROM collection c
    JOIN collection_patient c_p
    ON c.uuid = c_p.collection_uuid
    JOIN patient p
    ON c_p.patient_uuid = p.uuid
    WHERE collection_id IN (
        'CBIS-DDSM',
        'CC-Radiomics-Phantom',
        'CC-Radiomics-Phantom-2',
        'CC-Radiomics-Phantom-3',
        'QIBA-CT-Liver-Phantom',
        'QIN PET Phantom',
        'RIDER PHANTOM MRI',
        'RIDER PHANTOM PET-CT'
    )
)
UPDATE patient
SET init_idc_version=22, rev_idc_version=22
FROM wths
WHERE patient.uuid = wths.uuid
RETURNING * 
"""
    try:
        keep = sess.execute(query).all()
        successlogger.info(f'{len(keep)} patients')
        for row in keep:
            print(row.submitter_case_id, row.uuid, row.init_idc_version, row.rev_idc_version)
    except Exception as e:
        print(f"Error precounting: {e}")
        raise
    return


def update_study(sess):
    client = bigquery.Client()
    query = f"""
WITH wths AS(
    SELECT st.uuid
    FROM collection c
    JOIN collection_patient c_p
    ON c.uuid = c_p.collection_uuid
    JOIN patient p
    ON c_p.patient_uuid = p.uuid
    JOIN patient_study p_s
    ON p.uuid = p_s.patient_uuid
    JOIN study st
    ON p_s.study_uuid = st.uuid
    WHERE collection_id IN (
        'CBIS-DDSM',
        'CC-Radiomics-Phantom',
        'CC-Radiomics-Phantom-2',
        'CC-Radiomics-Phantom-3',
        'QIBA-CT-Liver-Phantom',
        'QIN PET Phantom',
        'RIDER PHANTOM MRI',
        'RIDER PHANTOM PET-CT'
    )
)
UPDATE study
SET init_idc_version=22, rev_idc_version=22
FROM wths
WHERE study.uuid = wths.uuid
RETURNING * 
"""
    try:
        keep = sess.execute(query).all()
        successlogger.info(f'{len(keep)} studies')
        for row in keep:
            print(row.study_instance_uid, row.uuid, row.init_idc_version, row.rev_idc_version)
    except Exception as e:
        print(f"Error precounting: {e}")
        raise
    return


def update_series(sess):
    client = bigquery.Client()
    query = f"""
WITH wths AS(
    SELECT se.uuid
    FROM collection c
    JOIN collection_patient c_p
    ON c.uuid = c_p.collection_uuid
    JOIN patient p
    ON c_p.patient_uuid = p.uuid
    JOIN patient_study p_s
    ON p.uuid = p_s.patient_uuid
    JOIN study st
    ON p_s.study_uuid = st.uuid
    JOIN study_series s_s
    ON st.uuid = s_s.study_uuid
    JOIN series se
    ON s_s.series_uuid = se.uuid
    WHERE collection_id IN (
        'CBIS-DDSM',
        'CC-Radiomics-Phantom',
        'CC-Radiomics-Phantom-2',
        'CC-Radiomics-Phantom-3',
        'QIBA-CT-Liver-Phantom',
        'QIN PET Phantom',
        'RIDER PHANTOM MRI',
        'RIDER PHANTOM PET-CT'
    )
)
UPDATE series
SET init_idc_version=22, rev_idc_version=22
FROM wths
WHERE series.uuid = wths.uuid
RETURNING * 
"""
    try:
        keep = sess.execute(query).all()
        successlogger.info(f'{len(keep)} series')
        for row in keep:
            print(row.series_instance_uid, row.uuid, row.init_idc_version, row.rev_idc_version)
    except Exception as e:
        print(f"Error precounting: {e}")
        raise
    return


def update_instance(sess):
    client = bigquery.Client()
    query = f"""
WITH wths AS(
    SELECT i.uuid
    FROM collection c
    JOIN collection_patient c_p
    ON c.uuid = c_p.collection_uuid
    JOIN patient p
    ON c_p.patient_uuid = p.uuid
    JOIN patient_study p_s
    ON p.uuid = p_s.patient_uuid
    JOIN study st
    ON p_s.study_uuid = st.uuid
    JOIN study_series s_s
    ON st.uuid = s_s.study_uuid
    JOIN series se
    ON s_s.series_uuid = se.uuid
    JOIN series_instance s_i
    ON se.uuid = s_i.series_uuid
    JOIN instance i
    ON s_i.instance_uuid = i.uuid
    WHERE collection_id IN (
        'CBIS-DDSM',
        'CC-Radiomics-Phantom',
        'CC-Radiomics-Phantom-2',
        'CC-Radiomics-Phantom-3',
        'QIBA-CT-Liver-Phantom',
        'QIN PET Phantom',
        'RIDER PHANTOM MRI',
        'RIDER PHANTOM PET-CT'
    )
)
UPDATE instance
SET init_idc_version=22, rev_idc_version=22
FROM wths
WHERE instance.uuid = wths.uuid
RETURNING * 
"""
    try:
        keep = sess.execute(query).all()
        successlogger.info(f'{len(keep)} instances')
        for row in keep:
            print(row.sop_instance_uid, row.uuid, row.init_idc_version, row.rev_idc_version)
    except Exception as e:
        print(f"Error precounting: {e}")
        raise
    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--collection_id', default='QIBA-CT-Liver-Phantom', help='Collection ID to rename')
    parser.add_argument('--download_path', default='/mnt/disks/idc-etl/aspera', help='Directory containing downloaded instances')
    args = parser.parse_args()

    with sa_session(echo=True) as sess:
        update_collection(sess)
        update_patient(sess)
        update_study(sess)
        update_series(sess)
        update_instance(sess)

        sess.commit()




