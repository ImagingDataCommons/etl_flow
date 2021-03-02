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

# Validate a previously generated IDC version
# Mostly verifies that counts of objects match
# counts reported by the IDC/NBIA API

import sys
import os
import argparse
from pathlib import Path
import time
from datetime import datetime, timezone, timedelta
import logging
from logging import INFO
import pydicom
import hashlib
from subprocess import run, PIPE
import shutil
from multiprocessing import Process, Queue
from queue import Empty
from base64 import b64decode
from pydicom.errors import InvalidDicomError
from uuid import uuid4
from google.cloud import storage
from idc.models import Version, Collection, Patient, Study, Series, Instance, sql_engine
from sqlalchemy import select
from sqlalchemy.orm import Session
from utilities.tcia_helpers import  get_TCIA_collections, get_TCIA_patients_per_collection, get_TCIA_studies_per_patient, \
    get_TCIA_series_per_study, get_TCIA_instance_uids_per_series, get_TCIA_instances_per_series, get_collection_values_and_counts
from utilities.identify_third_party_series import get_data_collection_doi, get_analysis_collection_dois
from google.api_core.exceptions import Conflict


def validate_instances(sess, args, version, collection, patient, study, series):
    rootlogger.info('Validating instances in series %s', series.series_instance_uid)
    tcia_instances = get_TCIA_instances_per_series(series.series_instance_uid)
    if not len(tcia_instances) == len(series.instances):
        errlogger.error('Different number of instance; NBIA: %s, IDC: %s in %s/%s/%s/%s/%s', len(tcia_instances), len(series.instances),
                version.idc_version_number, collection.tcia_api_collection_id, patient.submitter_case_id, study.study_instance_uid, series.series_instance_uid)
        exit(-1)


def validate_series(sess, args, version, collection, patient, study):
    rootlogger.info('Validating series in study %s', study.study_instance_uid)
    tcia_seriess = get_TCIA_series_per_study(collection.tcia_api_collection_id, patient.submitter_case_id, study.study_instance_uid)
    if not len(tcia_seriess) == len(study.seriess):
        errlogger.error('Different number of series; NBIA: %s, IDC: %s in %s/%s/%s/%s', len(tcia_seriess), len(study.seriess),
                version.idc_version_number, collection.tcia_api_collection_id, patient.submitter_case_id, study.study_instance_uid)
        exit(-1)
    for tcia_series in tcia_seriess:
        series  = next(series for series in study.seriess if  series.series_instance_uid == tcia_series['SeriesInstanceUID'])
        if not tcia_series['ImageCount'] == len(series.instances):
            errlogger.error('Different number of instance; NBIA: %s, IDC: %s in %s/%s/%s/%s/%s', len(tcia_series['ImageCount']), len(series.instances),
                    version.idc_version_number, collection.tcia_api_collection_id, patient.submitter_case_id, study.study_instance_uid, series.series_instance_uid)
            exit(-1)



def validate_studies(sess, args, version, collection, patient):
    rootlogger.info('Validating studies in patient %s', patient.submitter_case_id)
    tcia_studies = get_TCIA_studies_per_patient(collection.tcia_api_collection_id, patient.submitter_case_id)
    if not len(tcia_studies) == len(patient.studies):
        errlogger.error('Different number of studies; NBIA: %s, IDC: %s in %s/%s/%s', len(tcia_studies), len(patient.studies),
                version.idc_version_number, collection.tcia_api_collection_id, patient.submitter_case_id)
        exit(-1)
    for tcia_study in tcia_studies:
        study  = next(study for study in patient.studies if study.study_instance_uid == tcia_study['StudyInstanceUID'])
        validate_series(sess, args, version, collection, patient, study)


def validate_patients(sess, args, version, collection):
    rootlogger.info('Validating patients in collection %s', collection.tcia_api_collection_id)
    tcia_patients = get_TCIA_patients_per_collection(collection.tcia_api_collection_id)
    if not len(tcia_patients) == len(collection.patients):
        errlogger.error('Different number of patients; NBIA: %s, IDC: %s in %s/%s', len(tcia_patients), len(collection.patients),
                version.idc_version_number, collection.tcia_api_collection_id)
        exit(-1)
    for tcia_patient in tcia_patients:
        patient = next(patient for patient in collection.patients if patient.submitter_case_id == tcia_patient['PatientId'])
        validate_studies(sess, args, version, collection, patient)


def validate_collections(sess, args, version):
    tcia_collections = get_collection_values_and_counts()
    if not len(tcia_collections) == len(version.collections):
        errlogger.error('Different number of collections; NBIA: %s, IDC: %s', len(result), len(version.collections))
        exit(-1)
    for tcia_collection in tcia_collections:
        collection = next(collection for collection in version.collections if collection.tcia_api_collection_id == tcia_collection)
        validate_patients(sess, args, version, collection)


def validate_version(args):
    with Session(sql_engine) as sess:
        version = sess.query(Version).filter_by(idc_version_number=args.vnext).one()
        validate_collections(sess, args, version)


if __name__ == '__main__':
    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/vallog.log'.format(os.environ['PWD']))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/valerr.log'.format(os.environ['PWD']))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    parser = argparse.ArgumentParser()
    parser.add_argument('--vnext', default=2, help='Next version to generate')
    parser.add_argument('--bucket', default='idc_dev', help='Bucket in which to save instances')
    parser.add_argument('--staging_bucket', default='idc_dev_staging', help='Copy instances here before forwarding to --bucket')
    parser.add_argument('--prestaging_bucket_prefix', default=f'idc_v2_', help='Copy instances here before forwarding to --staging_bucket')
    parser.add_argument('--num_processes', default=8, help="Number of concurrent processes")
    parser.add_argument('--skips', default='{}/idc/skips.txt'.format(os.environ['PWD']) )
    parser.add_argument('--bq_dataset', default='mvp_wave2', help='BQ dataset')
    parser.add_argument('--bq_aux_name', default='auxilliary_metadata', help='Auxilliary metadata table name')
    parser.add_argument('--project', default='idc-dev-etl')
    args = parser.parse_args()

    # Directory to which to download files from TCIA/NBIA
    args.dicom = 'dicom'
    print("{}".format(args), file=sys.stdout)

    validate_version(args)
