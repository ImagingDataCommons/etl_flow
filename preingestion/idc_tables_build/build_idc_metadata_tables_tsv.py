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

# Adds/replaces data to the idc_collection/_patient/_study/_series/_instance DB tables.
# Metadata is extracted from a TSV file having columns Filename, "SOP Instance UID",
# "Patient ID", "Clinical Trial Protocol ID", "Study Instance UID", and "Series Instance UID".
# "Clinical Trial Protocol ID" is considered to be the collection ID.
#
# The expectation is that the TSV file will contain metadata of non-TCIA instances that is to
# to be in the next IDC version. The  idc_collection/_patient/_study/_series/_instance tables
# are always a snapshot of IDC sourced data.
import io
import os
import sys
import argparse
import csv
from idc.models import Base, WSI_Collection, WSI_Patient, WSI_Study, WSI_Series, WSI_Instance
from ingestion.utilities.utils import get_merkle_hash, list_skips

from logging import INFO, DEBUG
from utilities.logging_config import successlogger, errlogger, progresslogger
from base64 import b64decode
from python_settings import settings

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
        successlogger.info('\t\t\t\tInstance %s', instance_id)

    blob_name = f'{args.src_path}/{row["Filename"].strip()}' if args.src_path else \
        f'{row["Filename"].strip().split("/", 1)[1]}'
    instance.url = f'gs://{args.src_bucket}/{blob_name}'
    bucket = client.bucket(args.src_bucket)
    blob = bucket.blob(blob_name)
    blob.reload()
    new_hash = b64decode(blob.md5_hash).hex()
    if instance.hash != new_hash:
        instance.hash = new_hash
    progresslogger.info('\t\t\t\tInstance %s', instance_id)


def build_series(client, args, sess, study, row):
    series_id = row['Series Instance UID'].strip()
    try:
        series = next(series for series in study.seriess if series.series_instance_uid == series_id)
    except StopIteration:
        series = WSI_Series()
        series.series_instance_uid = series_id
        study.seriess.append(series)
        successlogger.info('\t\t\tSeries %s', series_id)
    build_instance(client, args, sess, series, row)
    return


def build_study(client, args, sess, patient, row):
    study_id = row['Study Instance UID'].strip()
    try:
        study = next(study for study in patient.studies if study.study_instance_uid == study_id)
    except StopIteration:
        study = WSI_Study()
        study.study_instance_uid = study_id
        patient.studies.append(study)
        successlogger.info('\t\tStudy %s', study_id)
    build_series(client, args, sess, study, row)
    return


def build_patient(client, args, sess, collection, row):
    patient_id = row['Patient ID'].strip()
    try:
        patient = next(patient for patient in collection.patients if patient.submitter_case_id == patient_id)
    except StopIteration:
        patient = WSI_Patient()
        patient.submitter_case_id = patient_id
        collection.patients.append(patient)
        successlogger.info('\tPatient %s', patient_id)
    build_study(client, args, sess, patient, row)
    return


def build_collection(client, args, sess,row, skips):
    collection_id = row['Clinical Trial Protocol ID'].strip()
    if not collection_id in skips:
        if collection_id=='NCT00047385':
            collection_id = 'NLST'
        # Get the collection from the DB
        collection = sess.query(WSI_Collection).filter(WSI_Collection.collection_id == collection_id).first()
        if not collection:
            # The collection is not currently in the DB, so add it
            collection = WSI_Collection()
            collection.collection_id = collection_id
            sess.add(collection)
            successlogger.info('Collection %s',collection_id)
        build_patient(client, args, sess, collection, row)
        return


def prebuild(args):
    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{settings.CLOUD_DATABASE}'
    # sql_engine = create_engine(sql_uri, echo=True)
    sql_engine = create_engine(sql_uri)


    with Session(sql_engine) as sess:
        client = storage.Client()
        skips = list_skips(sess, Base, args.skipped_groups, args.skipped_collections)

        # Get the manifest of htan files/blobs
        bucket = client.bucket(args.src_bucket)
        result = bucket.blob(f'{args.tsv_blob_path}').download_as_text()
        with io.StringIO(result) as tsv:
        # with open(args.tsv_file, newline='', ) as tsv:
            reader = csv.DictReader(tsv, delimiter='\t')
            rows = len(list(reader))
            tsv.seek(0)
            reader = csv.DictReader(tsv, delimiter='\t')
            for row in reader:
                # print(f'{reader.line_num-1}/{rows}: {row}')
                build_collection(client, args, sess, row, skips)
        # gen_hashes(args, sess)
        sess.commit()
    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--version', default=settings.CURRENT_VERSION)
    parser.add_argument('--src_bucket', default='nlst-pathology-conversion-results-new', help='Bucket containing WSI instances')
    parser.add_argument('--src_path', default='', \
        help='Folder in src_bucket that is the root of WSI data to be indexed. The Filename in the tsv is \
         concatenated onto this.')
    parser.add_argument('--tsv_blob_path', default = 'identifiers_NLST.txt',\
                        help='A GCS blob that contains a TSV manifest of WSI DICOMs to be ingested')
    parser.add_argument('--skipped_groups', default=[], nargs='*', \
                        help="A list of collection groups that should not be ingested. "\
                             "Can include open_collections, cr_collections, defaced_collections, redacted_collections, excluded_collections. "\
                             "Note that this value is interpreted as a list.")
    parser.add_argument('--skipped_collections', type=str, default=['HTAN-Vanderbilt'], nargs='*', \
      help='A list of additional collections that should not be ingested.')
    # parser.add_argument('--log_dir', default=f'{settings.LOGGING_BASE}/{settings.BASE_NAME}')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    args.client=storage.Client()

    prebuild(args)

