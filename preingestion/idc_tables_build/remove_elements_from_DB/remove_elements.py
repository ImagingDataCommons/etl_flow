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

# Removes an instance, series, study, patient or collection from
# the idc_xxx hierarchy

import os
import io
import sys
import argparse
import csv
from idc.models import Base, IDC_Collection
from ingestion.utilities.utils import get_merkle_hash, list_skips
from utilities.logging_config import successlogger, errlogger, progresslogger
from python_settings import settings
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, update
from google.cloud import storage


def remove_instance(client, args, sess, series, instance):
    try:
        series.instances.remove(instance)
        sess.delete(instance)
        progresslogger.info('\t\t\t\tInstance %s', instance.sop_instance_uid)
        return
    except StopIteration:
        # Instance no longer in series
        return

def remove_series(client, args, sess, study, series):
    try:
        for instance in series.instances:
            remove_instance(client, args, sess, series, instance)
        if series.instances:
            # Series is not empty. Keep it.
            hashes = [instance.hash for instance in series.instances]
            series.hash = get_merkle_hash(hashes)
        else:
            # Series is empty now. Remove it from study and delete it
            study.seriess.remove(series)
            sess.delete(series)
            progresslogger.info('\t\t\tSeries %s', series.series_instance_uid)
        return
    except StopIteration:
        # Series no longer in study
        return


def remove_study(client, args, sess, patient, study):
    try:
        for series in study.seriess:
            remove_series(client, args, sess, study, series)
        if study.seriess:
            # Study is not empty. Keep it.
            hashes = [series.hash for series in study.seriess]
            study.hash = get_merkle_hash(hashes)
        else:
            # Study is empty now. Remove it from patient and delete it
            patient.studies.remove(study)
            sess.delete(study)
            progresslogger.info('\t\tStudy %s', study.study_instance_uid)
        return
    except StopIteration:
        # Study no longer in patient
        return


def remove_patient(client, args, sess, collection, patient):
    try:
        for study in patient.studies:
            remove_study(client,args, sess, patient, study)
        if patient.studies:
            # Patient not empty. Keep it.
            hashes = [study.hash for study in patient.studies]
            patient.hash = get_merkle_hash(hashes)
        else:
            # The patient is empty. Remove it from collection and delete it
            collection.patients.remove(patient)
            sess.delete(patient)
            progresslogger.info('\tPatient %s', patient.submitter_case_id)
        return
    except StopIteration:
        # Patient no longer in collection
        return


def remove_collection(client, args, sess, collection):
    try:
        # collection = next(collection for collection in version.collections if collection.collection_id == collection_id)
        for patient in collection.patients:
            remove_patient(client, args, sess, collection, patient)
        # remove_patient(client, args, sess, collection, row)
        if collection.patients:
            # Collection is not empty. Keep it.
            hashes = [patient.hash for patient in collection.patients]
            collection.hash = get_merkle_hash(hashes)
        else:
            # Collection is empty. Delete it
            sess.delete(collection)
            progresslogger.info('Collection %s', collection.collection_id)
        return
    except StopIteration:
        # Collection no longer in DB
        return


# def prebuild(args):
#     sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{settings.CLOUD_DATABASE}'
#     # sql_engine = create_engine(sql_uri, echo=True)
#     sql_engine = create_engine(sql_uri)
#
#     with Session(sql_engine) as sess:
#         client = storage.Client()
#         skips = list_skips(sess, Base, args.skipped_groups, args.skipped_collections)
#
#         bucket = client.bucket(args.src_bucket)
#         result = bucket.blob(f'{args.src_path}/{args.tsv_blob}').download_as_text()
#         with io.StringIO(result) as tsv:
#             # with open(args.tsv_file, newline='', ) as tsv:
#             reader = csv.DictReader(tsv, delimiter='\t')
#             rows = len(list(reader))
#             tsv.seek(0)
#             reader = csv.DictReader(tsv, delimiter='\t')
#             for row in reader:
#                 # print(f'{reader.line_num-1}/{rows}: {row}')
#                 remove_collection(client, args, sess, row, skips)
#         sess.commit()
#     return
#     #     bucket.blob(args.tsv_blob).download_to_filename(args.tsv_file)
#     #     remove_version(client, args, sess)
#     # return


# if __name__ == '__main__':
#     parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
#     parser.add_argument('--src_bucket', default='htan-transfer', help='Bucket containing WSI instances')
#     parser.add_argument('--src_path', default='HTAN-V1-Converted/Converted_20220416', help='Folder in src_bucket that is the root of WSI data to be indexed ')
#     parser.add_argument('--tsv_blob', default = 'identifiers.txt',\
#                         help='A GCS blob that contains a TSV manifest of WSI DICOMs to be ingested')
#     parser.add_argument('--skipped_groups', default=[], nargs='*', \
#                         help="A list of collection groups that should not be ingested. "\
#                              "Can include open_collections, cr_collections, defaced_collections, redacted_collections, excluded_collections. "\
#                              "Note that this is value is interpreted as a list.")
#     parser.add_argument('--skipped_collections', type=str, default=['HTAN-HMS', 'HTAN-Vanderbilt', 'HTAN-WUSTL'], nargs='*', \
#       help='A list of additional collections that should not be ingested.')
#     # parser.add_argument('--log_dir', default=f'{settings.LOGGING_BASE}/{settings.BASE_NAME}')
#
#     args = parser.parse_args()
#     print("{}".format(args), file=sys.stdout)
#     args.client=storage.Client()
#
#     prebuild(args)
