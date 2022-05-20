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
import io
import sys
import argparse
import csv
from idc.models import Base, WSI_Collection
from ingestion.utilities.utils import get_merkle_hash, list_skips
from utilities.logging_config import successlogger, errlogger, progresslogger
from python_settings import settings
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, update
from google.cloud import storage


def remove_instance_from_series(client, args, sess, series, row):
    instance_id = row['SOP Instance UID'].strip()
    try:
        instance = next(instance for instance in series.instances if instance.sop_instance_uid == instance_id)
        # Remove the instance from the series and delete it
        series.instances.remove(instance)
        sess.delete(instance)
        progresslogger.info('\t\t\t\tInstance %s', instance.sop_instance_uid)
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
            # Series is empty now. Remove it from study and delete it
            study.seriess.remove(series)
            sess.delete(series)
            progresslogger.info('\t\t\tSeries %s', series.series_instance_uid)
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
            # Study is empty now. Remove it from patient and delete it
            patient.studies.remove(study)
            sess.delete(study)
            progresslogger.info('\t\tStudy %s', study.study_instance_uid)
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
            # The patient is empty. Remove it from collection and delete it
            collection.patients.remove(patient)
            sess.delete(patient)
            progresslogger.info('\tPatient %s', patient.submitter_case_id)
        return
    except StopIteration:
        # Patient no longer in collection
        return


def remove_collection(client, args, sess, row, skips):
    collection_id = row['Clinical Trial Protocol ID'].strip()
    if not collection_id in skips:
        try:
            # collection = next(collection for collection in version.collections if collection.collection_id == collection_id)
            collection = sess.query(WSI_Collection).filter(WSI_Collection.collection_id == collection_id).first()
            remove_patient_from_collection(client, args, sess, collection, row)
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


# def remove_version(client, args, sess):
#     # The WSI metadata is not actually versioned. It is really a snapshot
#     # of WSI data that is expected to be in the current/next IDC version.
#     # It is only versioned to the extent that it is associated with a
#     # particular version of the DB
#     # There should be only a single "version", having version=0
#     version = sess.query(WSI_Version).filter(WSI_Version.version == 0).first()
#     # version = sess.query(WSI_Version).filter(WSI_Version.version == settings.CURRENT_VERSION).first()
#     if version:
#         with open(args.tsv_file, newline='', ) as tsv:
#             reader = csv.DictReader(tsv, delimiter='\t')
#             rows = len(list(reader))
#             tsv.seek(0)
#             reader = csv.DictReader(tsv, delimiter='\t')
#             for row in reader:
#                 print(f'{reader.line_num - 1}/{rows}: {row}')
#                 remove_collection_from_version(client, args, sess, version, row)
#                 hashes = [collection.hash for collection in version.collections]
#                 version.hash = get_merkle_hash(hashes)
#         sess.commit()
#     return


def prebuild(args):
    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{settings.CLOUD_DATABASE}'
    # sql_engine = create_engine(sql_uri, echo=True)
    sql_engine = create_engine(sql_uri)

    with Session(sql_engine) as sess:
        client = storage.Client()
        skips = list_skips(sess, Base, args.skipped_groups, args.skipped_collections)

        bucket = client.bucket(args.src_bucket)
        result = bucket.blob(f'{args.src_path}/{args.tsv_blob}').download_as_text()
        with io.StringIO(result) as tsv:
            # with open(args.tsv_file, newline='', ) as tsv:
            reader = csv.DictReader(tsv, delimiter='\t')
            rows = len(list(reader))
            tsv.seek(0)
            reader = csv.DictReader(tsv, delimiter='\t')
            for row in reader:
                # print(f'{reader.line_num-1}/{rows}: {row}')
                remove_collection(client, args, sess, row, skips)
        sess.commit()
    return
    #     bucket.blob(args.tsv_blob).download_to_filename(args.tsv_file)
    #     remove_version(client, args, sess)
    # return


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--src_bucket', default='htan-transfer', help='Bucket containing WSI instances')
    parser.add_argument('--src_path', default='HTAN-V1-Converted/Converted_20220416', help='Folder in src_bucket that is the root of WSI data to be indexed ')
    parser.add_argument('--tsv_blob', default = 'identifiers.txt',\
                        help='A GCS blob that contains a TSV manifest of WSI DICOMs to be ingested')
    parser.add_argument('--skipped_groups', default=[], nargs='*', \
                        help="A list of collection groups that should not be ingested. "\
                             "Can include open_collections, cr_collections, defaced_collections, redacted_collections, excluded_collections. "\
                             "Note that this is value is interpreted as a list.")
    parser.add_argument('--skipped_collections', type=str, default=['HTAN-HMS', 'HTAN-Vanderbilt', 'HTAN-WUSTL'], nargs='*', \
      help='A list of additional collections that should not be ingested.')
    # parser.add_argument('--log_dir', default=f'{settings.LOGGING_BASE}/{settings.BASE_NAME}')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    args.client=storage.Client()
    #
    # if not os.path.exists(settings.LOGGING_BASE):
    #     os.mkdir(settings.LOGGING_BASE)
    # if not os.path.exists(args.log_dir):
    #     os.mkdir(args.log_dir)
    #
    # successlogger = logging.getLogger('root.success')
    # successlogger.setLevel(INFO)
    # for hdlr in successlogger.handlers[:]:
    #     successlogger.removeHandler(hdlr)
    # success_fh = logging.FileHandler('{}/success.log'.format(args.log_dir))
    # successlogger.addHandler(success_fh)
    # successformatter = logging.Formatter('%(message)s')
    # success_fh.setFormatter(successformatter)
    #
    # progresslogger = logging.getLogger('root.progress')
    # progresslogger.setLevel(INFO)
    # for hdlr in progresslogger.handlers[:]:
    #     progresslogger.removeHandler(hdlr)
    # success_fh = logging.FileHandler('{}/progress.log'.format(args.log_dir))
    # progresslogger.addHandler(success_fh)
    # successformatter = logging.Formatter('%(message)s')
    # success_fh.setFormatter(successformatter)
    #
    #
    # errlogger = logging.getLogger('root.err')
    # for hdlr in errlogger.handlers[:]:
    #     errlogger.removeHandler(hdlr)
    # err_fh = logging.FileHandler('{}/error.log'.format(args.log_dir))
    # errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    # errlogger.addHandler(err_fh)
    # err_fh.setFormatter(errformatter)

    prebuild(args)
