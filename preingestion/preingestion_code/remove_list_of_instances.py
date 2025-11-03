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

# Hierarchically removes instances having SOPInstanceUIDs in a list, then hierarchically removes higher level elements
# Does not update the hashes

# import os
# import io
# import sys
# import argparse
# import csv
# from idc.models import Base, IDC_Collection, IDC_Patient, IDC_Study, IDC_Series
# from ingestion.utilities.utils import get_merkle_hash, list_skips
# from utilities.logging_config import successlogger, errlogger, progresslogger
# from python_settings import settings
# from sqlalchemy.orm import Session
# from sqlalchemy import create_engine, update
# from google.cloud import storage
from utilities.sqlalchemy_helpers import sa_session
import pandas as pd


def remove_instances(sess, instances):
    instance_list = ','.join(f"'{w}'" for w in instances)
    query = f"""
DELETE FROM idc_instance 
WHERE sop_instance_uid IN ({instance_list})
RETURNING *
    """
    result = sess.execute(query).fetchall()
    return


def remove_series(sess, instances):
    remove_instances(sess, instances)

    query = f"""
DELETE FROM idc_series
WHERE NOT EXISTS (
    SELECT FROM idc_instance
    WHERE idc_series.series_instance_uid = idc_instance.series_instance_uid
    )
RETURNING *
    """
    result = sess.execute(query).fetchall()
    return

def remove_studies(sess, instances):
    remove_series(sess, instances)

    query = f"""
DELETE FROM idc_study
WHERE NOT EXISTS (
    SELECT FROM idc_series
    WHERE idc_study.study_instance_uid = idc_series.study_instance_uid
    )
RETURNING *
    """
    result = sess.execute(query).fetchall()
    return


def remove_patients(sess, instances):
    remove_studies(sess, instances)

    query = f"""
DELETE FROM idc_patient
WHERE NOT EXISTS (
    SELECT FROM idc_study
    WHERE idc_patient.submitter_case_id = idc_study.submitter_case_id
    )
RETURNING *
    """
    result = sess.execute(query).fetchall()
    return


def remove_collections(sess, instances):
    remove_patients(sess, instances)

    query = f"""
DELETE FROM idc_collection
WHERE NOT EXISTS (
    SELECT FROM idc_patient
    WHERE idc_collection.collection_id = idc_patient.collection_id
    )
RETURNING *
    """
    result = sess.execute(query).fetchall()

    sess.commit()
    return

    
def perform_partial_deletion(sess, args, sep):

    with sa_session(echo=False) as sess:
        manifest_data = pd.read_csv(f"gs://{args.src_bucket}/{args.subdir}/{args.manifest_id}", sep=sep, header=0)
        instances = manifest_data['SOPInstanceUID'].to_list()
        instances.sort()
        remove_collections(sess, instances)
    sess.commit()