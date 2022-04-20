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
#### This is the second step in populating a DICOM store ####
# Because the buckets contain multiple versions of some instances, and a DICOM
# store can hold only one version, it is indeterminant which version is imported.
# Therefore, after importing the buckets, we first delete any instance that has
# more than one version. We also delete any instance that has a final version
# (final_idc_version != 0). This latter is to ensure that there are not retired instances
# that are no longer in the current IDC version.

import os
import argparse
import logging
import json
from logging import INFO

from idc.models import Base, Version, Patient, Study, Series, Instance, Collection, CR_Collections, Defaced_Collections, Open_Collections, Redacted_Collections
import settings as etl_settings
from python_settings import settings
import google
from google.cloud import storage
from google.auth.transport import requests

from sqlalchemy import create_engine, select
from sqlalchemy_utils import register_composites
from sqlalchemy.orm import Session
from python_settings import settings


def instance_exists(args, dicomweb_sess, study_instance_uid, series_instance_uid, sop_instance_uid):
    # URL to the Cloud Healthcare API endpoint and version
    base_url = "https://healthcare.googleapis.com/v1"
    url = "{}/projects/{}/locations/{}".format(base_url, settings.PUB_PROJECT, settings.GCH_REGION)
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


def delete_instance(args, dicomweb_session, study_instance_uid, series_instance_uid, sop_instance_uid):
    # URL to the Cloud Healthcare API endpoint and version
    base_url = "https://healthcare.googleapis.com/v1"
    url = "{}/projects/{}/locations/{}".format(base_url, settings.PUB_PROJECT, settings.GCH_REGION)
    dicomweb_path = "{}/datasets/{}/dicomStores/{}/dicomWeb/studies/{}/series/{}/instances/{}".format(
        url, settings.GCH_DATASET, settings.GCH_DICOMSTORE, study_instance_uid, series_instance_uid, sop_instance_uid
    )
    # Set the required application/dicom+json; charset=utf-8 header on the request
    headers = {"Content-Type": "application/dicom+json; charset=utf-8"}

    response = dicomweb_session.delete(dicomweb_path, headers=headers)
    if response.status_code == 200:
        successlogger.info(sop_instance_uid)
    else:
        errlogger.error(sop_instance_uid)



def delete_instances(args, sess, dicomweb_sess):
    try:
        # Get the previously copied blobs
        done_instances = set(open(f'{args.log_dir}/delete_success.log').read().splitlines())
    except:
        done_instances = set()

    # Change logging file. File name includes bucket ID.
    for hdlr in successlogger.handlers[:]:
        successlogger.removeHandler(hdlr)
    success_fh = logging.FileHandler(f'{args.log_dir}/delete_success.log')
    successlogger.addHandler(success_fh)
    successformatter = logging.Formatter('%(message)s')
    success_fh.setFormatter(successformatter)

    for hdlr in errlogger.handlers[:]:
        errlogger.removeHandler(hdlr)
    err_fh = logging.FileHandler(f'{args.log_dir}/delete_error.log')
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    # Collections that are included in the DICOM store are in one of four groups
    # Specifically we do not include excluded_collections
    collections = sorted(
        [row.tcia_api_collection_id for row in
            sess.query(Open_Collections.tcia_api_collection_id).union(
            sess.query(Defaced_Collections.tcia_api_collection_id),
            sess.query(CR_Collections.tcia_api_collection_id),
            sess.query(Redacted_Collections.tcia_api_collection_id)).all()])

    # We first try to delete any instance for which there are multiple versions from the collections in
    # open_collections, cr_collections, defaced_collections and redacted_collectons
    try:
        rev_uids = json.load(open(f'{args.log_dir}/revised_uids.txt'))
    except:
        rows =  sess.query(Collection.collection_id,Study.study_instance_uid, Series.series_instance_uid, Instance.sop_instance_uid).\
            join(Collection.patients).join(Patient.studies).join(
            Study.seriess).join(Series.instances).filter(Instance.init_idc_version != Instance.rev_idc_version).all()
        rev_uids = [{'study_instance_uid':row.study_instance_uid, 'series_instance_uid':row.series_instance_uid,
                 'sop_instance_uid':row.sop_instance_uid } for row in rows if row.collection_id in collections]
        with open(f'{args.log_dir}/revised_uids.txt','w') as f:
            json.dump(rev_uids,f)
    n=0
    for row in rev_uids:
        if not row['sop_instance_uid'] in done_instances:
            if instance_exists(args, dicomweb_sess, row['study_instance_uid'],
                               row['series_instance_uid'], row['sop_instance_uid']):
                delete_instance(args, dicomweb_sess, row['study_instance_uid'],
                                row['series_instance_uid'], row['sop_instance_uid'])
                print(f"{n}: Instance {row['sop_instance_uid']}  deleted")
            else:
                print(f"{n}: Instance {row['sop_instance_uid']} not in DICOM store")
        else:
            print(f"{n}: Instance {row['sop_instance_uid']} previously deleted")
        n+=1

    # The above will not delete fully retired instances for which there is a single version
    # We delete those now.
    try:
        ret_uids = json.load(open(f'{args.log_dir}/retired_uids.txt'))
    except:
        rows =  sess.query(Collection.collection_id, Study.study_instance_uid, Series.series_instance_uid, Instance.sop_instance_uid).\
            join(Collection.patients).join(Patient.studies).join(Study.seriess).join(Series.instances).filter(Instance.final_idc_version != 0).filter(Instance.init_idc_version == Instance.rev_idc_version).all()
        ret_uids = [{'study_instance_uid':row.study_instance_uid, 'series_instance_uid':row.series_instance_uid,
                 'sop_instance_uid':row.sop_instance_uid } for row in rows if row.collection_id in collections]
        with open(f'{args.log_dir}/retired_uids.txt','w') as f:
            json.dump(ret_uids,f)
    n=0
    for row in ret_uids:
        if not row['sop_instance_uid'] in done_instances:
            if instance_exists(args, dicomweb_sess, row['study_instance_uid'],
                               row['series_instance_uid'], row['sop_instance_uid']):
                delete_instance(args, dicomweb_sess, row['study_instance_uid'],
                                row['series_instance_uid'], row['sop_instance_uid'])
                print(f"{n}: Instance {row['sop_instance_uid']}  deleted")
            else:
                print(f"{n}: Instance {row['sop_instance_uid']} not in DICOM store")
        else:
            print(f"{n}: Instance {row['sop_instance_uid']} previously deleted")
        n+=1
    pass

def repair_store(args):
    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{settings.CLOUD_DATABASE}'
    # sql_engine = create_engine(sql_uri, echo=True) # Use this to see the SQL being sent to PSQL
    sql_engine = create_engine(sql_uri)
    args.sql_uri = sql_uri # The subprocesses need this uri to create their own SQL engine

    # Create the tables if they do not already exist
    Base.metadata.create_all(sql_engine)

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
    # parser.add_argument('--version', default=8, help='Version to work on')
    parser.add_argument('--client', default=storage.Client())
    args = parser.parse_args()
    # parser.add_argument('--db', default=f'idc_v{args.version}', help='Database on which to operate')
    # parser.add_argument('--dst_project', default='canceridc-data')
    # parser.add_argument('--gch_dataset_region', default='us')
    # parser.add_argument('--gch_dataset_name', default='idc')
    # parser.add_argument('--dicomstore', default=f'v{args.version}')
    parser.add_argument('--log_dir', default=f'/mnt/disks/idc-etl/logs/repair_dicom_store_with_redacted_v{settings.CURRENT_VERSION}')
    args = parser.parse_args()
    args.id = 0 # Default process ID

    if not os.path.exists('{}'.format(args.log_dir)):
        os.mkdir('{}'.format(args.log_dir))
        st = os.stat('{}'.format(args.log_dir))
        # os.chmod('{}'.format(args.log_dir), st.st_mode | 0o222)

    # proglogger = logging.getLogger('root.prog')
    # # prog_fh = logging.FileHandler(f'{os.environ["PWD"]}/logs/log.log')
    # prog_fh = logging.FileHandler(f'{args.log_dir}/log.log')
    # progformatter = logging.Formatter('%(levelname)s:prog:%(message)s')
    # proglogger.addHandler(prog_fh)
    # prog_fh.setFormatter(progformatter)
    # proglogger.setLevel(INFO)

    successlogger = logging.getLogger('root.success')
    successlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')

    repair_store(args)