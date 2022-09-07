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

import json
import argparse

from utilities.logging_config import successlogger, progresslogger, errlogger
from idc.models import Base, Version, Collection, Patient, Study, Series, Instance, All_Included_Collections
from google.cloud import storage
from sqlalchemy.orm import Session
from python_settings import settings
from sqlalchemy import create_engine, update

def gen_instance_object(args, sess, collection, patient, study, series, instance):
    level = "Instance"
    if not args.dst_bucket.blob(f"{collection.uuid}/{patient.uuid}/{study.uuid}/{series.uuid}/{instance.uuid}/").exists():
        # print(f'\t\t{level} {instance.uuid} started')
        if instance.source.name == 'tcia':
            bucket = sess.query(All_Included_Collections.pub_tcia_url).filter(All_Included_Collections.tcia_api_collection_id == collection.collection_id).first().pub_tcia_url
        else:
            bucket = sess.query(All_Included_Collections.pub_path_url).filter(All_Included_Collections.tcia_api_collection_id == collection.collection_id).first().pub_path_url

        contents = { "child": f"{bucket}/{instance.uuid}.dcm" }
        blob = args.dst_bucket.blob(f"{collection.uuid}/{patient.uuid}/{study.uuid}/{series.uuid}/{instance.uuid}").upload_from_string(json.dumps(contents))
        print(f'\t\t\t\t{level} {instance.uuid} completed')
    else:
        print(f'\t\t\t\t{level} {instance.uuid} skipped')
    return


def gen_series_object(args, sess, collection, patient, study, series):
    level = "Series"
    if not args.dst_bucket.blob(f"{collection.uuid}/{patient.uuid}/{study.uuid}/{series.uuid}/").exists():
        print(f'\t\t\t{level} {series.uuid} started')
        for instance in series.instances:
            gen_instance_object(args, sess, collection, patient, study, series, instance)
        contents = {
            "path": f"{collection.uuid}/{patient.uuid}/{study.uuid}/{series.uuid}/",
            "children":
            [
                 f"{instance.uuid}/" for instance in series.instances
            ]
        }
        blob = args.dst_bucket.blob(f"{collection.uuid}/{patient.uuid}/{study.uuid}/{series.uuid}/").upload_from_string(json.dumps(contents))
        print(f'\t\t\t{level} {series.uuid} completed')
    else:
        print(f'\t\t\t{level} {series.uuid} skipped')
    return


def gen_study_object(args, sess, collection, patient, study):
    level = "Study"
    if not args.dst_bucket.blob(f"{collection.uuid}/{patient.uuid}/{study.uuid}/").exists():
        print(f'\t\t{level} {study.uuid} started')
        for series in study.seriess:
            gen_series_object(args, sess, collection, patient, study, series)
        contents = {
            "path": f"{collection.uuid}/{patient.uuid}/{study.uuid}/",
            "children":
            [
                 f"{series.uuid}/" for series in study.seriess
            ]
        }
        blob = args.dst_bucket.blob(f"{collection.uuid}/{patient.uuid}/{study.uuid}/").upload_from_string(json.dumps(contents))
        print(f'\t\t{level} {study.uuid} completed')
    else:
        print(f'\t\t{level} {study.uuid} skipped')
    return


def gen_patient_object(args, sess, collection, patient):
    level = "Patient"
    if not args.dst_bucket.blob(f"{collection.uuid}/{patient.uuid}/").exists():
        print(f'\t{level} {patient.uuid} started')
        for study  in patient.studies:
            gen_study_object(args, sess, collection, patient, study)
        contents = {
            "path": f"{collection.uuid}/{patient.uuid}/",
            "children":
            [
                 f"{study.uuid}/" for study in patient.studies
            ]
        }
        blob = args.dst_bucket.blob(f"{collection.uuid}/{patient.uuid}/").upload_from_string(json.dumps(contents))
        print(f'\t{level} {patient.uuid} completed')
    else:
        print(f'\t{level} {patient.uuid} skipped')
    return

def gen_collection_object(args, sess, collection):
    level = "Collection"
    if not args.dst_bucket.blob(f"{collection.uuid}/").exists():
        print(f'{level} {collection.uuid} started')
        for patient in collection.patients:
            gen_patient_object(args, sess, collection, patient)
        contents = {
            "path": f"{collection.uuid}/",
            "children":
            [
                 f"{patient.uuid}/" for patient in collection.patients
            ]
        }
        blob = args.dst_bucket.blob(f"{collection.uuid}/").upload_from_string(json.dumps(contents))
        print(f'{level} {collection.uuid} completed')
    else:
        print(f'{level} {collection.uuid} skipped')
    return

def gen_collections(args):
    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{settings.CLOUD_DATABASE}'
    # sql_engine = create_engine(sql_uri, echo=True)
    sql_engine = create_engine(sql_uri)
    # Create the tables if they do not already exist
    Base.metadata.create_all(sql_engine)

    with Session(sql_engine) as sess:
        for collection_id in args.collections:
            collections = sess.query(Collection).filter(Collection.collection_id == collection_id)
            for collection in collections:
                gen_collection_object(args, sess, collection)


if __name__ == '__main__':
    client = storage.Client()
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=9, help='Version to work on')
    parser.add_argument('--collections', default=['APOLLO-5-LSCC', 'CPTAC-SAR', 'MIDRC-RICORD-1C', 'TCGA-READ'])
    # parser.add_argument('--hfs_levels', default=['study', 'series'], help='Name blobs as study/series/instance if study, series/instance if series')
    parser.add_argument('--dst_bucket_name', default='whc_prop3', help='Bucket into which to copy blobs')
    args = parser.parse_args()

    args.id = 0  # Default process ID
    print(f'args: {json.dumps(args.__dict__, indent=2)}')
    args.dst_bucket = client.bucket(args.dst_bucket_name)

    gen_collections(args)
