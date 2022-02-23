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
#### This is the third and final step in populating a DICOM store ####
# We now insert the version of an instance if the instance has been
# revised but is not retired. This will replace the revised instances
# which we deleted in the previous step.

import os
import argparse
import logging
import json
from logging import INFO
from google.api_core.exceptions import Conflict
from idc.models import Base, Version, Patient, Study, Series, Instance, Collection, CR_Collections, Defaced_Collections, Open_Collections, Redacted_Collections
from gch.obsolete.with_redacted.import_buckets_with_redacted import import_dicom_instances, wait_done
from ingestion.utils import empty_bucket
import settings as etl_settings
from python_settings import settings
if not settings.configured:
    settings.configure(etl_settings)
import google
from google.cloud import storage
from google.auth.transport import requests

from sqlalchemy import create_engine
from sqlalchemy_utils import register_composites
from sqlalchemy.orm import Session

def get_collection_groups(sess):
    dev_staging_buckets = {}
    pub_staging_buckets = {}
    collections = sess.query(CR_Collections.tcia_api_collection_id, CR_Collections.dev_url, CR_Collections.pub_url)
    for collection in  collections:
        dev_staging_buckets[collection.tcia_api_collection_id] = collection.dev_url
        pub_staging_buckets[collection.tcia_api_collection_id] = collection.pub_url
    collections = sess.query(Defaced_Collections.tcia_api_collection_id, Defaced_Collections.dev_url, Defaced_Collections.pub_url)
    for collection in  collections:
        dev_staging_buckets[collection.tcia_api_collection_id] = collection.dev_url
        pub_staging_buckets[collection.tcia_api_collection_id] = collection.pub_url
    collections = sess.query(Open_Collections.tcia_api_collection_id, Open_Collections.dev_url, Open_Collections.pub_url)
    for collection in  collections:
        dev_staging_buckets[collection.tcia_api_collection_id] = collection.dev_url
        pub_staging_buckets[collection.tcia_api_collection_id] = collection.pub_url
    collections = sess.query(Redacted_Collections.tcia_api_collection_id, Redacted_Collections.dev_url, Redacted_Collections.pub_url)
    for collection in  collections:
        dev_staging_buckets[collection.tcia_api_collection_id] = collection.dev_url
        pub_staging_buckets[collection.tcia_api_collection_id] = collection.pub_url
    return dev_staging_buckets, pub_staging_buckets


def create_staging_bucket(args):
    client = storage.Client(project='idc-dev-etl')

    # Try to create the destination bucket
    new_bucket = client.bucket(args.staging_bucket)
    new_bucket.iam_configuration.uniform_bucket_level_access_enabled = True
    new_bucket.versioning_enabled = False
    try:
        result = client.create_bucket(new_bucket, location='US-CENTRAL1')
        # return(0)
    except Conflict:
        # Bucket exists
        pass
    except Exception as e:
        # Bucket creation failed somehow
        errlogger.error("Error creating bucket %s: %s",args.staging_bucket, e)
        return(-1)

# Populate a bucket with instances to be inserted in the DICOM store
def populate_staging_bucket(args, uids):
    client = storage.Client()
    create_staging_bucket(args)
    empty_bucket(args.staging_bucket)
    dst_bucket = client.bucket(args.staging_bucket)
    for row in uids:
        bucket = client.bucket(row['bucket'])
        blob = bucket.blob(f'{row["uuid"]}.dcm')
        dst_blob = dst_bucket.blob(f'{row["uuid"]}.dcm')
        if not dst_blob.exists():
            bucket.copy_blob(blob, dst_bucket)


def insert_instances(args, sess, dicomweb_sess):
    try:
        # Get the previously copied blobs
        done_instances = set(open(f'{args.log_dir}/insert_success.log').read().splitlines())
    except:
        done_instances = set()

        # Change logging file. File name includes bucket ID.
    for hdlr in successlogger.handlers[:]:
        successlogger.removeHandler(hdlr)
    success_fh = logging.FileHandler(f'{args.log_dir}/insert_success.log')
    successlogger.addHandler(success_fh)
    successformatter = logging.Formatter('%(message)s')
    success_fh.setFormatter(successformatter)

    for hdlr in errlogger.handlers[:]:
        errlogger.removeHandler(hdlr)
    err_fh = logging.FileHandler(f'{args.log_dir}/insert_error.log')
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    # Collections that are included in the DICOM store are in one of four groups
    collections = sorted(
        [row.tcia_api_collection_id for row in sess.query(Open_Collections.tcia_api_collection_id).union(
            sess.query(Defaced_Collections.tcia_api_collection_id),
            sess.query(CR_Collections.tcia_api_collection_id),
            sess.query(Redacted_Collections.tcia_api_collection_id)).all()])

    # dev_staging_buckets tells us which staging bucket has a collection's instances
    dev_staging_buckets, _ = get_collection_groups(sess)

    try:
        uids = json.load(open('logs/inserted_uids.txt'))
    except:
        rows =  sess.query(Collection.collection_id,Study.study_instance_uid, Series.series_instance_uid,
            Instance.sop_instance_uid,Instance.uuid).join(Version.collections).join(Collection.patients).\
            join(Patient.studies).join(Study.seriess).join(Series.instances).filter(Instance.init_idc_version !=
            Instance.rev_idc_version).filter(Instance.final_idc_version == 0).\
            filter(Version.version == 7).all()
        uids = [{'collection_id':row.collection_id, 'study_instance_uid':row.study_instance_uid, 'series_instance_uid':row.series_instance_uid,
                 'sop_instance_uid':row.sop_instance_uid,'uuid':row.uuid,
                 'bucket':dev_staging_buckets[row.collection_id]  } for row in rows if row.collection_id in collections]
        with open('logs/inserted_uids.txt','w') as f:
            json.dump(uids,f)

    # We import instances from a bucket
    # populate_staging_bucket(args, uids)

    print('Importing {}'.format(args.staging_bucket))
    content_uri = '{}/*'.format(args.staging_bucket)
    response = import_dicom_instances(args.dst_project, args.dataset_region, args.gch_dataset_name,
                                      args.dicomstore, content_uri)
    print(f'Response: {response}')
    result = wait_done(response, args, args.period)

    # Don't forget to delete the staging bucket

def repair_store(args):
    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{args.db}'
    sql_engine = create_engine(sql_uri, echo=True) # Use this to see the SQL being sent to PSQL
    # sql_engine = create_engine(sql_uri)
    args.sql_uri = sql_uri # The subprocesses need this uri to create their own SQL engine

    # Create the tables if they do not already exist
    Base.metadata.create_all(sql_engine)

    # Enable the underlying psycopg2 to deal with composites
    conn = sql_engine.connect()
    register_composites(conn)

    scoped_credentials, dst_project = google.auth.default(
        ["https://www.googleapis.com/auth/cloud-platform"]
    )
    # Creates a requests Session object with the credentials.
    dicomweb_sess = requests.AuthorizedSession(scoped_credentials)

    with Session(sql_engine) as sess:
        insert_instances(args, sess, dicomweb_sess)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=7, help='Version to work on')
    parser.add_argument('--client', default=storage.Client())
    args = parser.parse_args()
    parser.add_argument('--db', default=f'idc_v7', help='Database on which to operate')
    parser.add_argument('--dst_project', default='idc-dev-etl')
    parser.add_argument('--dataset_region', default='us-central1')
    parser.add_argument('--gch_dataset_name', default='idc')
    parser.add_argument('--dicomstore', default=f'v{args.version}-with-redacted')
    parser.add_argument('--log_dir', default=f'/mnt/disks/idc-etl/logs/repair_dicom_store_with_redacted')
    parser.add_argument('--period',default=60)
    parser.add_argument('--staging_bucket', default='dicom_store_insert_staging_bucket')
    args = parser.parse_args()
    args.id = 0 # Default process ID

    if not os.path.exists('{}'.format(args.log_dir)):
        os.mkdir('{}'.format(args.log_dir))
        st = os.stat('{}'.format(args.log_dir))
        os.chmod('{}'.format(args.log_dir), st.st_mode | 0o222)

    proglogger = logging.getLogger('root.prog')
    prog_fh = logging.FileHandler(f'{os.environ["PWD"]}/logs/log.log')
    progformatter = logging.Formatter('%(levelname)s:prog:%(message)s')
    proglogger.addHandler(prog_fh)
    prog_fh.setFormatter(progformatter)
    proglogger.setLevel(INFO)

    successlogger = logging.getLogger('root.success')
    successlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')

    repair_store(args)