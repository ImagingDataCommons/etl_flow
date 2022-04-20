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

# We have now populated a DICOM store with all data in the version, and exported that data to BQ.
# We now need to delete all the data from redacted collections so that the DICOM store can be
# used for viewing, etc.

import argparse
import logging
import os
from logging import INFO
from python_settings import settings
from idc.models import Patient, Study, Collection, Redacted_Collections
import google
from google.auth.transport import requests
from sqlalchemy import create_engine
from sqlalchemy_utils import register_composites
from sqlalchemy.orm import Session


def instance_exists(args, dicomweb_sess, study_instance_uid, series_instance_uid, sop_instance_uid):
    # URL to the Cloud Healthcare API endpoint and version
    base_url = "https://healthcare.googleapis.com/v1"
    url = "{}/projects/{}/locations/{}".format(base_url, settings.GCH_PROJECT, settings.GCH_REGION)
    dicomweb_path = "{}/datasets/{}/dicomStores/{}/dicomWeb/studies/{}/series/{}/instances?SOPInstanceUID={}".format(
        url, settings.GCH_DATASET, settings.GCH_DICOMSTORE, study_instance_uid, series_instance_uid, sop_instance_uid
    )
    # Set the required application/dicom+json; charset=utf-8 header on the request
    headers = {"Content-Type": "application/dicom+json; charset=utf-8"}

    response = dicomweb_sess.get(dicomweb_path, headers=headers)
    if response.status_code == 200:
        # successlogger.info('%s found',sop_instance_uid)
        print('%s found',sop_instance_uid)
        # done_instances.add(sop_instance_uid)
        return True
    else:
        # errlogger.error('%s not found',sop_instance_uid)
        print('%s not found',sop_instance_uid)
        return False


def delete_study(args, dicomweb_session, study_instance_uid):
    # URL to the Cloud Healthcare API endpoint and version
    base_url = "https://healthcare.googleapis.com/v1"
    url = "{}/projects/{}/locations/{}".format(base_url, settings.GCH_PROJECT, settings.GCH_REGION)
    dicomweb_path = "{}/datasets/{}/dicomStores/{}/dicomWeb/studies/{}".format(
        url, settings.GCH_DATASET, settings.GCH_DICOMSTORE, study_instance_uid
    )
    # Set the required application/dicom+json; charset=utf-8 header on the request
    headers = {"Content-Type": "application/dicom+json; charset=utf-8"}

    response = dicomweb_session.delete(dicomweb_path, headers=headers)
    # response.raise_for_status()
    if response.status_code == 200:
        successlogger.info(study_instance_uid)
    else:
        errlogger.error(study_instance_uid)


def delete_instances(args, sess, dicomweb_sess):
    try:
        # Get the previously copied blobs
        done_studies = set(open(f'{args.log_dir}/delete_redacted_success.log').read().splitlines())
    except:
        done_studies = set()

    # Change logging file. File name includes bucket ID.
    for hdlr in successlogger.handlers[:]:
        successlogger.removeHandler(hdlr)
    success_fh = logging.FileHandler(f'{args.log_dir}/delete_redacted_success.log')
    successlogger.addHandler(success_fh)
    successformatter = logging.Formatter('%(message)s')
    success_fh.setFormatter(successformatter)

    for hdlr in errlogger.handlers[:]:
        errlogger.removeHandler(hdlr)
    err_fh = logging.FileHandler(f'{args.log_dir}/delete_redacted_error.log')
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    # Redacted collection ids
    collections = sorted(
        [row.tcia_api_collection_id for row in
            sess.query(Redacted_Collections.tcia_api_collection_id).all()])

    # We first try to delete any instance for which there are multiple versions from the collections in
    # open_collections, cr_collections, defaced_collections and redacted_collectons
    rows = sess.query(Collection.collection_id, Study.study_instance_uid). \
        distinct().join(Collection.patients).join(Patient.studies). \
        filter(Collection.collection_id.in_(collections)).all()
    redacted_studies = [{'collection_id': row.collection_id, 'study_instance_uid': row.study_instance_uid} for row in rows]
    n=0
    for row in redacted_studies:
        if not row['study_instance_uid'] in done_studies:
            delete_study(args, dicomweb_sess, row['study_instance_uid'])
            print(f"{n}: {row['collection_id']}/{row['study_instance_uid']}  deleted")
        else:
            print(f"{n}: {row['collection_id']}/{row['study_instance_uid']} previously deleted")
        n+=1
    pass

def delete_redacted(args):
    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{settings.CLOUD_DATABASE}'
    sql_engine = create_engine(sql_uri, echo=True) # Use this to see the SQL being sent to PSQL
    # sql_engine = create_engine(sql_uri)
    args.sql_uri = sql_uri # The subprocesses need this uri to create their own SQL engine

    # Enable the underlying psycopg2 to deal with composites
    conn = sql_engine.connect()
    register_composites(conn)

    scoped_credentials, project = google.auth.default(
        ["https://www.googleapis.com/auth/cloud-platform"]
    )
    # Creates a requests Session object with the credentials.
    dicomweb_sess = requests.AuthorizedSession(scoped_credentials)

    with Session(sql_engine) as sess:
        delete_instances(args, sess, dicomweb_sess)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--log_dir', default=f'/mnt/disks/idc-etl/logs/delete_redacted_from_dicom_store')
    args = parser.parse_args()

    if not os.path.exists('{}'.format(args.log_dir)):
        os.mkdir('{}'.format(args.log_dir))
        st = os.stat('{}'.format(args.log_dir))

    successlogger = logging.getLogger('root.success')
    successlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')

    delete_redacted(args)