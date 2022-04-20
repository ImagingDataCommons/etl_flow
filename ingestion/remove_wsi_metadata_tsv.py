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

# Removes data from the wsi_collection/_patient/_study/_series/_instance DB tables.
# Metadata is extracted from a TSV file having columns Filename, "SOP Instance UID",
# "Patient ID", "Clinical Trial Protocol ID", "Study Instance UID", and "Series Instance UID".
# "Clinical Trial Protocol ID" is considered to be the collection ID.
#
# The expectation is that the TSV file will contain metadata on non-TCIA instances that
# are to be removed from a subsequent IDC version.

import os
import sys
import argparse
from python_settings import settings

import csv

from idc.models import Base, WSI_Version, WSI_Collection, WSI_Patient, WSI_Study, WSI_Series, WSI_Instance
from ingestion.utils import get_merkle_hash

from google.cloud import storage


import logging
from logging import INFO, DEBUG
from base64 import b64decode
# import settings as etl_settings
from python_settings import settings
# settings.configure(etl_settings)

from ingestion.utils import list_skips

from sqlalchemy.orm import Session
from sqlalchemy import create_engine, update
from google.cloud import storage


def remove_instance_from_series(client, args, sess, series, row):
    instance_id = row['SOP Instance UID'].strip()
    try:
        instance = next(instance for instance in series.instances if instance.sop_instance_uid == instance_id)
        series.instances.remove(instance)
        sess.delete(instance)
        return
    except StopIteration:
        # Instance no longer in series
        return

def remove_series_from_study(client, args, sess, study, row):
    series_id = row['Series Instance UID'].strip()
    try:
        series = next(series for series in study.seriess if series.series_instance_uid == series_id)
        remove_instance_from_series(client, args, sess, series, row)
        if series.instances:
            # Series is not empty. Keep it.
            hashes = [instance.hash for instance in series.instances]
            series.hash = get_merkle_hash(hashes)
        else:
            # Series is empty now. Remove it from study
            study.seriess.remove(series)
            sess.delete(series)
        return
    except StopIteration:
        # Series no longer in study
        return


def remove_study_from_patient(client, args, sess, patient, row):
    study_id = row['Study Instance UID'].strip()
    try:
        study = next(study for study in patient.studies if study.study_instance_uid == study_id)
        remove_series_from_study(client, args, sess, study, row)
        if study.seriess:
            # Study is not empty. Keep it.
            hashes = [series.hash for series in study.seriess]
            study.hash = get_merkle_hash(hashes)
        else:
            # Study is empty now. Remove it from patient
            patient.studies.remove(study)
            sess.delete(study)
        return
    except StopIteration:
        # Study no longer in patient
        return


def remove_patient_from_collection(client, args, sess, collection, row):
    patient_id = row['Patient ID'].strip()
    try:
        patient = next(patient for patient in collection.patients if patient.submitter_case_id == patient_id)
        remove_study_from_patient(client, args, sess, patient, row)
        if patient.studies:
            # Patient not empty. Keep it.
            hashes = [study.hash for study in patient.studies]
            patient.hash = get_merkle_hash(hashes)
        else:
            # The patient is empty. Remove it from collection
            collection.patients.remove(patient)
            sess.delete(patient)
        return
    except StopIteration:
        # Patient no longer in collection
        return


def remove_collection_from_version(client, args, sess, version, row):
    collection_id = row['Clinical Trial Protocol ID'].strip()
    try:
        collection = next(collection for collection in version.collections if collection.collection_id == collection_id)
        remove_patient_from_collection(client, args, sess, collection, row)
        if collection.patients:
            # Collection is not empty. Keep it.
            hashes = [patient.hash for patient in collection.patients]
            collection.hash = get_merkle_hash(hashes)
        else:
            # Collection is empty. Remove it from the version.
            version.collections.remove(collection)
            sess.delete(collection)
        return
    except StopIteration:
        # Collection no longer in DB
        return


def remove_version(client, args, sess):
    # The WSI metadata is not actually versioned. It is really a snapshot
    # of WSI data that is expected to be in the current/next IDC version.
    # It is only versioned to the extent that it is associated with a
    # particular version of the DB
    # There should be only a single "version", having version=0
    version = sess.query(WSI_Version).filter(WSI_Version.version == 0).first()
    # version = sess.query(WSI_Version).filter(WSI_Version.version == settings.CURRENT_VERSION).first()
    if version:
        with open(args.tsv_file, newline='', ) as tsv:
            reader = csv.DictReader(tsv, delimiter='\t')
            rows = len(list(reader))
            tsv.seek(0)
            reader = csv.DictReader(tsv, delimiter='\t')
            for row in reader:
                print(f'{reader.line_num - 1}/{rows}: {row}')
                remove_collection_from_version(client, args, sess, version, row)
                hashes = [collection.hash for collection in version.collections]
                version.hash = get_merkle_hash(hashes)
        sess.commit()
    return


def prebuild(args):
    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{settings.CLOUD_DATABASE}'
    # sql_engine = create_engine(sql_uri, echo=True)
    sql_engine = create_engine(sql_uri)

    with Session(sql_engine) as sess:
        client = storage.Client()
        bucket = client.bucket(args.src_bucket)
        bucket.blob(args.tsv_blob).download_to_filename(args.tsv_file)
        remove_version(client, args, sess)
    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # parser.add_argument('--version', default=8, help='Version to work on')
    parser.add_argument('--client', default=storage.Client())
    # parser.add_argument('--db', default=f'idc_v{settings.CURRENT_VERSION}', help='Database on which to operate')
    # parser.add_argument('--project', default='idc-dev-etl')
    parser.add_argument('--src_bucket', default='htan-transfer')
    parser.add_argument('--tsv_blob', default = 'HTAN-V1-Converted/Converted_20220228/identifiers.txt',\
                        help='A GCS blob that contains a TSV manifest of WSI DICOMs to be ingested')
    parser.add_argument('--tsv_file', default = 'logs/tsv_files.txt')
    parser.add_argument('--skipped_groups', default=['redacted_collections', 'excluded_collections'], \
                        help="Collection groups that should not be ingested")
    parser.add_argument('--skipped_collections', default=[],\
      help='Additional collections that should not be ingested.')
    parser.add_argument('--dones', default='{}/logs/wsi_build_dones.txt'.format(os.environ['PWD']), help="Completed collections")
    args = parser.parse_args()

    print("{}".format(args), file=sys.stdout)

    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/wsi_metadata_log.log'.format(os.environ['PWD'], settings.CURRENT_VERSION))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/wsi_metadata_err.log'.format(os.environ['PWD'], settings.CURRENT_VERSION))
    errformatter = logging.Formatter('{%(pathname)s:%(lineno)d} %(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    # rootlogger.info('Args: %s', args)
    prebuild(args)
