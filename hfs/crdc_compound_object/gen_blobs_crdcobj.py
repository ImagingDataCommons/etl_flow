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

# Genrate study and series folder objects which each contain a manifest of child objects
import json
import argparse
from collection_list_crdcobj import collection_list
from utilities.logging_config import successlogger, progresslogger, errlogger
from idc.models import Base, Version, Collection, Patient, Study, Series, Instance, All_Included_Collections
from google.cloud import storage
from sqlalchemy.orm import Session
import settings
from sqlalchemy import create_engine, update

def gen_series_object(args, sess, collection, patient, study, series):
    level = "Series"
    if not args.dst_bucket.blob(f"{series.uuid}.idc").exists():
        print(f'\t\t\t{level} {series.uuid} started')
        # Create a combined "folder" and "bundle" blob
        contents = {
            'encoding_version': '1.0',
            'description': 'IDC CRDC DICOM series compound object',
            'object_type': 'DICOM series',
            'id': series.uuid,
            'name': series.series_instance_uid,
            'self_uri': f'drs://dg.4DFC/{series.uuid}',
            'access_methods': [
                {
                    'method': 'children',
                    "mime_type": 'application/dicom',
                    'description': 'List of DRS URIs of instances in this series',
                    'contents': [
                        {
                            'name': i.sop_instance_uid,
                            'drs_uri': f'drs://dg.4DFC/{i.uuid}'
                        } for i in series.instances
                    ],
                },
                {
                    'method': 'folder_object',
                    "mime_type": 'application/json',
                    'description': 'DRS URI that resolves to a gs or s3 folder corresponding to this series',
                    'contents': [
                        {
                            'name': f'{series.series_instance_uid}/',
                            'drs_uri': f'drs://dg.4DFC/some_TBD_uuid'
                        }
                    ]
                },
                {
                    'method': 'archive_package',
                    "mime_type": 'application/zip',
                    'description': 'DRS URI that resolves to a zip archive of the instances in this series',
                    'contents': [
                        {
                            'name': f'{series.series_instance_uid}.zip',
                            'drs_uri': f'drs://dg.4DFC/some_TBD_uuid'
                        }
                    ]
                }
            ],
        }
        blob = args.dst_bucket.blob(f"{series.uuid}/").upload_from_string(json.dumps({}))
        if not args.dst_bucket.blob(f"{series.uuid}/").exists():
            errlogger.error(f"{series.uuid}/ doesn't exist")
        blob = args.dst_bucket.blob(f"{series.uuid}/crdcobj.json").upload_from_string(json.dumps(contents))
        if not args.dst_bucket.blob(f"{series.uuid}/crdcobj.json").exists():
            errlogger.error(f"{series.uuid}/crdcobj.json doesn't exist")

        print(f'\t\t\t{level} {series.uuid} completed')
    else:

        print(f'\t\t\t{level} {series.uuid} skipped')
    return


def gen_study_object(args, sess, collection, patient, study):
    level = "Study"
    print(f'\t\t{level} {study.uuid} started')
    for series in study.seriess:
        if series.sources.tcia:
            gen_series_object(args, sess, collection, patient, study, series)
    print(f'\t\t{level} {study.uuid} completed')


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

def gen_all(args):

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
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='Version to work on')
    parser.add_argument('--dst_bucket_name', default='crdcobj', help='Bucket into which to copy blobs')
    args = parser.parse_args()

    args.id = 0  # Default process ID
    print(f'args: {json.dumps(args.__dict__, indent=2)}')
    args.dst_bucket = client.bucket(args.dst_bucket_name)

    gen_all(args)
