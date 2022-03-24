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

# Adds/replaces data to the wsi_collection/_patient/_study/_series/_instance DB tables.
# Metadata is extracted from a TSV file having columns Filename, "SOP Instance UID",
# "Patient ID", "Clinical Trial Protocol ID", "Study Instance UID", and "Series Instance UID".
# "Clinical Trial Protocol ID" is considered to be the collection ID.
#
# The expectation is that the TSV file will contain metadata of non-TCIA instances that is to
# to be in the next IDC version. The  wsi_collection/_patient/_study/_series/_instance tables
# are always a snapshot of non-TCIA data in IDC.

import os
import sys
import argparse
from fnmatch import fnmatch
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


def build_instance(client, args, sess, series, row):
    instance_id = row['SOP Instance UID'].strip()
    try:
        instance = next(instance for instance in series.instances if instance.sop_instance_uid == instance_id)
    except StopIteration:
        instance = WSI_Instance()
        instance.sop_instance_uid = instance_id
        series.instances.append(instance)
    blob_name = f'HTAN-V1-Converted/Converted_20220228/{row["Filename"].strip().split("/", 1)[1]}'
    instance.url = f'gs://{args.src_bucket}/{blob_name}'
    bucket = client.bucket(args.src_bucket)
    blob = bucket.blob(blob_name)
    blob.reload()

    instance.hash = b64decode(blob.md5_hash).hex()


def build_series(client, args, sess, study, row):
    series_id = row['Series Instance UID'].strip()
    try:
        series = next(series for series in study.seriess if series.series_instance_uid == series_id)
    except StopIteration:
        series = WSI_Series()
        series.series_instance_uid = series_id
        study.seriess.append(series)
        # sess.commit()
    build_instance(client, args, sess, series, row)
    hashes = [instance.hash for instance in series.instances]
    series.hash = get_merkle_hash(hashes)
    return


def build_study(client, args, sess, patient, row):
    study_id = row['Study Instance UID'].strip()
    try:
        study = next(study for study in patient.studies if study.study_instance_uid == study_id)
    except StopIteration:
        study = WSI_Study()
        study.study_instance_uid = study_id
        patient.studies.append(study)
        # sess.commit()
    build_series(client, args, sess, study, row)
    hashes = [series.hash for series in study.seriess]
    study.hash = get_merkle_hash(hashes)
    return


def build_patient(client, args, sess, collection, row):
    patient_id = row['Patient ID'].strip()
    try:
        patient = next(patient for patient in collection.patients if patient.submitter_case_id == patient_id)
    except StopIteration:
        patient = WSI_Patient()
        patient.submitter_case_id = patient_id
        collection.patients.append(patient)
        # sess.commit()
    build_study(client, args, sess, patient, row)
    hashes = [study.hash for study in patient.studies]
    patient.hash = get_merkle_hash(hashes)
    return


def build_collection(client, args, sess, version, row, skips):
    collection_id = row['Clinical Trial Protocol ID'].strip()
    if not collection_id in skips:
        try:
            collection = next(collection for collection in version.collections if collection.collection_id == collection_id)
        except StopIteration:
            collection = WSI_Collection()
            collection.collection_id = collection_id
            version.collections.append(collection)
            # sess.commit()
        build_patient(client, args, sess, collection, row)
        hashes = [patient.hash for patient in collection.patients]
        collection.hash = get_merkle_hash(hashes)
        return



def build_version(client, args, sess, skips):
    try:
        dones = open(args.dones).read().splitlines()
    except:
        dones = []
    try:
        included = open(args.included).read().splitlines()
    except:
        included = ["*"]


    # The WSI metadata is not actually versioned. It is really a snapshot
    # of WSI data that is expected to be in the current/next IDC version.
    # It is only versioned to the extent that it is associated with a
    # particular version of the DB
    # There should be only a single "version", having version=0
    version = sess.query(WSI_Version).filter(WSI_Version.version == 0).first()
    # version = sess.query(WSI_Version).filter(WSI_Version.version == settings.CURRENT_VERSION).first()
    if not version:
        version = WSI_Version()
        version.version = 0
        sess.add(version)

    with open(args.tsv_file, newline='', ) as tsv:
        reader = csv.DictReader(tsv, delimiter='\t')
        rows = len(list(reader))
        tsv.seek(0)
        reader = csv.DictReader(tsv, delimiter='\t')
        for row in reader:
            print(f'{reader.line_num-1}/{rows}: {row}')
            build_collection(client, args, sess, version, row, skips)
            hashes = [collection.hash for collection in version.collections]
            version.hash = get_merkle_hash(hashes)
    sess.commit()


def prebuild(args):
    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{settings.CLOUD_DATABASE}'
    # sql_engine = create_engine(sql_uri, echo=True)
    sql_engine = create_engine(sql_uri)


    with Session(sql_engine) as sess:
        client = storage.Client()
        skips = list_skips(sess, Base, args.skipped_groups, args.skipped_collections)
        bucket = client.bucket(args.src_bucket)
        bucket.blob(args.tsv_blob).download_to_filename(args.tsv_file)
        build_version(client, args, sess, skips)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # parser.add_argument('--version', default=8, help='Version to work on')
    parser.add_argument('--client', default=storage.Client())
    # parser.add_argument('--db', default=f'idc_v{args.version}', help='Database on which to operate')
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
