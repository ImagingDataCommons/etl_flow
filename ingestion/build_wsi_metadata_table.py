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

# Build a table of collection/patient/study/series/instance metadata for the WSIs
# Assumes that bucket containing the pathology WSI blobs is gcsfuse mounted (the
# mount point is a paramater). This allows pydicom that extract DICOM UIDs
# from each blob without having to import the entire (large) blob.

import os
import sys
import argparse
import pydicom

import logging
from logging import INFO, DEBUG
from base64 import b64decode


from pydicom.errors import InvalidDicomError
from idc.models import WSI_metadata
from sqlalchemy import select, bindparam
from sqlalchemy.orm import Session

from python_settings import settings

import settings as etl_settings

settings.configure(etl_settings)

from sqlalchemy import create_engine, update
from sqlalchemy.ext.declarative import declarative_base
from google.cloud import storage

def get_blob_hash(args, blob_name):
    blob = args.src_bucket.blob(blob_name)
    blob.reload()
    hash = b64decode(blob.md5_hash).hex()
    return hash

def get_blob_size(args, blob_name):
    blob = args.src_bucket.blob(blob_name)
    blob.reload()
    size = blob.size
    return size

def insert_metadata(sess, args, collection, root, files):
    metadata = []
    for file in files:
        if file != "DICOMDIR":
            gcs_url = os.path.join(root, file)
            ds = pydicom.dcmread(gcs_url)
            submitter_case_id = ds.PatientID
            study_instance_uid = ds.StudyInstanceUID.name
            series_instance_uid = ds.SeriesInstanceUID.name
            sop_instance_uid = ds.SOPInstanceUID.name
            blob_name = '/'.join((root.split('/',5)[-1],file))
            hash = get_blob_hash(args, blob_name)
            size = get_blob_size(args, blob_name)
            metadata.append(
                dict (
                    collection_id = collection,
                    submitter_case_id=submitter_case_id,
                    study_instance_uid=study_instance_uid,
                    series_instance_uid=series_instance_uid,
                    sop_instance_uid=sop_instance_uid,
                    gcs_url=blob_name,
                    hash=hash,
                    size=size
                )
            )
            print('{}/{}/{}/{}/{}, {}'.format(collection, submitter_case_id, study_instance_uid, series_instance_uid, sop_instance_uid, blob_name))
    sess.bulk_insert_mappings(WSI_metadata, metadata)


def update_sizes(args):
    sql_uri = f'postgresql+psycopg2://{settings.DATABASE_USERNAME}:{settings.DATABASE_PASSWORD}@{settings.DATABASE_HOST}:{settings.DATABASE_PORT}/{args.db}'
    # sql_engine = create_engine(sql_uri, echo=True)
    sql_engine = create_engine(sql_uri, echo=True)

    declarative_base().metadata.create_all(sql_engine)

    with Session(sql_engine) as sess:
        result = sess.execute(select(WSI_metadata))
        for row in result:
            size = get_blob_size(args, row[0].gcs_url)
            row[0].size = size
        sess.flush()
        sess.commit()


def update_hashes(args):
    sql_uri = f'postgresql+psycopg2://{settings.DATABASE_USERNAME}:{settings.DATABASE_PASSWORD}@{settings.DATABASE_HOST}:{settings.DATABASE_PORT}/{args.db}'
    # sql_engine = create_engine(sql_uri, echo=True)
    sql_engine = create_engine(sql_uri, echo=True)

    declarative_base().metadata.create_all(sql_engine)

    with Session(sql_engine) as sess:
        result = sess.execute(select(WSI_metadata))
        for row in result:
            hash = get_blob_hash(args, row[0].gcs_url)
            row[0].hash = hash
        sess.flush()
        sess.commit()




# def get_collection_metadata(sess, args, collection, topdir):
#     for root, dirs, files in os.walk(topdir):
#         insert_metadata(sess, args, collection, root, files)
#         for dir in dirs:
#             get_collection_metadata(sess, args, collection, os.path.join(root, dir))
def get_collection_metadata(sess, args, collection, topdir):
    for root, dirs, files in os.walk(topdir):
        # print(root)
        insert_metadata(sess, args, collection, root, files)
        # insert_metadata(sess, args, collection, root, files)
        # for dir in dirs:
        #     get_collection_metadata(sess, args, collection, os.path.join(root, dir))


def prebuild(args):
    sql_uri = f'postgresql+psycopg2://{settings.DATABASE_USERNAME}:{settings.DATABASE_PASSWORD}@{settings.DATABASE_HOST}:{settings.DATABASE_PORT}/{args.db}'
    # sql_engine = create_engine(sql_uri, echo=True)
    sql_engine = create_engine(sql_uri)

    declarative_base().metadata.create_all(sql_engine)

    todos = open(args.todos).read().splitlines()

    with Session(sql_engine) as sess:
        # collections = [x[0] for x in os.walk(args.gcsfuse_dir)]
        collections = os.listdir(path=args.gcsfuse_dir)
        for collection in collections:
            if  collection in todos:
                get_collection_metadata(sess, args, collection, os.path.join(args.gcsfuse_dir,collection))

    sess.commit()




if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=3, help='Version to work on')
    parser.add_argument('--client', default=storage.Client())
    args = parser.parse_args()
    parser.add_argument('--db', default=f'idc_path_v{args.version}', help='Database on which to operate')
    parser.add_argument('--project', default='idc-dev-etl')
    parser.add_argument('--gcsfuse_dir', default='/mnt/disks/idc-etl/wsi_bucket')
    parser.add_argument('--src_bucket', default=storage.Bucket(args.client,'af-dac-wsi-conversion-results'))
    parser.add_argument('--num_processes', default=0, help="Number of concurrent processes")
    parser.add_argument('--todos', default='{}/logs/path_ingest_v{}_todo.txt'.format(os.environ['PWD'], args.version), help="Collections to include")
    args = parser.parse_args()

    print("{}".format(args), file=sys.stdout)

    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/wsi_metadata_log.log'.format(os.environ['PWD'], args.version))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(DEBUG)

    errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/wsi_metadata_err.log'.format(os.environ['PWD'], args.version))
    errformatter = logging.Formatter('{%(pathname)s:%(lineno)d} %(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    rootlogger.info('Args: %s', args)
    prebuild(args)
    # update_hashes(args)
    # update_sizes(args)
