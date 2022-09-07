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

# Generate a
import json
import argparse
from collection_list import collection_list
from utilities.logging_config import successlogger, progresslogger, errlogger
from idc.models import Base, Version, Collection, Patient, Study, Series, Instance, All_Included_Collections
from google.cloud import storage
from sqlalchemy.orm import Session
from python_settings import settings
from sqlalchemy import create_engine, update

def gen_series_object(args, sess, collection, patient, study, series):
    level = "Series"
    if not args.dst_bucket.blob(f"{study.uuid}/{series.uuid}/").exists():
        print(f'\t\t\t{level} {series.uuid} started')
        # Create a combined "folder" and "bundle" blob
        contents = "\n".join([f"{study.uuid}/{series.uuid}/{instance.uuid}.dcm" for instance in series.instances])
        blob = args.dst_bucket.blob(f"{study.uuid}/{series.uuid}/").upload_from_string(contents)
        if not args.dst_bucket.blob(f"{study.uuid}/{series.uuid}/").exists():
            errlogger.error(f"{study.uuid}/{series.uuid}/ doesn't exist")
        print(f'\t\t\t{level} {series.uuid} completed')
    else:

        print(f'\t\t\t{level} {series.uuid} skipped')
    return


def gen_study_object(args, sess, collection, patient, study):
    level = "Study"
    if not args.dst_bucket.blob(f"{study.uuid}/").exists():
        print(f'\t\t{level} {study.uuid} started')
        for series in study.seriess:
            if series.sources.tcia:
                gen_series_object(args, sess, collection, patient, study, series)
        # Create a combined "folder" and "bundle" blob
        contents = "\n".join([f"{study.uuid}/{series.uuid}/" for series in study.seriess])
        blob = args.dst_bucket.blob(f"{study.uuid}/").upload_from_string(contents)
        if not args.dst_bucket.blob(f"{study.uuid}/").exists():
            errlogger.error(f"{study.uuid}/ doesn't exist")
        print(f'\t\t{level} {study.uuid} completed')
    else:
        print(f'\t\t{level} {study.uuid} skipped')
    return


def gen_patient_object(args, sess, collection, patient):
    level = "Patient"
    for study  in patient.studies:
        if study.sources.tcia:
            gen_study_object(args, sess, collection, patient, study)
    print(f'\t{level} {patient.uuid} completed')
    return

def gen_collection_object(args, sess, collection):
    level = "Collection"
    for patient in collection.patients:
        gen_patient_object(args, sess, collection, patient)
    print(f'{level} {collection.uuid} completed')
    return

def gen_studies(args):

    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{settings.CLOUD_DATABASE}'
    # sql_engine = create_engine(sql_uri, echo=True)
    sql_engine = create_engine(sql_uri)
    # Create the tables if they do not already exist
    Base.metadata.create_all(sql_engine)

    with Session(sql_engine) as sess:
        for collection_id in collection_list:
            collections = sess.query(Collection).filter(Collection.collection_id == collection_id)
            for collection in collections:
                gen_collection_object(args, sess, collection)


if __name__ == '__main__':
    client = storage.Client()
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=9, help='Version to work on')
    # parser.add_argument('--collections', default=['APOLLO-5-LSCC', 'CPTAC-SAR', 'MIDRC-RICORD-1C', 'TCGA-READ'])
    # parser.add_argument('--hfs_levels', default=['study', 'series'], help='Name blobs as study/series/instance if study, series/instance if series')
    parser.add_argument('--dst_bucket_name', default='whc_prop1', help='Bucket into which to copy blobs')
    args = parser.parse_args()

    args.id = 0  # Default process ID
    print(f'args: {json.dumps(args.__dict__, indent=2)}')
    args.dst_bucket = client.bucket(args.dst_bucket_name)

    gen_studies(args)
