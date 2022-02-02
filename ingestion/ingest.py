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

# Populate the DB with data for the next IDC version

import os
import argparse
import logging
from logging import INFO, DEBUG
from datetime import datetime, timedelta
import shutil
from multiprocessing import Lock, shared_memory
from idc.models import Base, Version, Collection
from utilities.tcia_helpers import get_access_token

from ingestion.version import clone_version, build_version

from python_settings import settings

from google.cloud import storage

from sqlalchemy import create_engine
from sqlalchemy_utils import register_composites
from sqlalchemy.orm import Session

from ingestion.all_sources import All
from http.client import HTTPConnection
HTTPConnection.debuglevel = 0



rootlogger = logging.getLogger('root')
errlogger = logging.getLogger('root.err')

def ingest(args):
    HTTPConnection.debuglevel = 0
    logging.getLogger("urllib3").setLevel(logging.ERROR)

    # rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/v{}_log.log'.format(os.environ['PWD'], args.version))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    # errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/v{}_err.log'.format(os.environ['PWD'], args.version))
    errformatter = logging.Formatter('{%(pathname)s:%(lineno)d} %(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    rootlogger.debug('Args: %s', args)

    # Create a local working directory into which data
    # from TCIA is copied
    if os.path.isdir('{}'.format(args.dicom)):
        shutil.rmtree('{}'.format(args.dicom))
    os.mkdir('{}'.format(args.dicom))


    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{args.db}'
    # sql_engine = create_engine(sql_uri, echo=True) # Use this to see the SQL being sent to PSQL
    sql_engine = create_engine(sql_uri)
    args.sql_uri = sql_uri # The subprocesses need this uri to create their own SQL engine

    # Create the tables if they do not already exist
    Base.metadata.create_all(sql_engine)

    # Enable the underlying psycopg2 to deal with composites
    conn = sql_engine.connect()
    register_composites(conn)

    with Session(sql_engine) as sess:
        # Get the target version, if it exists
        access = shared_memory.ShareableList(get_access_token())
        args.access = access
        all_sources = All(args.id, sess, args.version, args.access, Lock())

        version = sess.query(Version).filter(Version.version == args.version).first()
        if not version:
            previous_version = sess.query(Version).filter(Version.version == args.previous_version).first()
            if not previous_version:
                # if it doesn't exist then we are creating V1
                version = Version()
                version.expanded=False
                version.done=False
                version.is_new=True
                version.revised=False
                version.hashes = ["","",""]
                version.sources = [False,False]
                version.version = str(args.version)
                version.previous_version = args.previous_version
                sess.add(version)
                sess.commit()
            else:
                # Previous version exists. Create next version as a
                # clone of previous version
                if not previous_version.done:
                    errlogger.error('Previous version %s is not done',args.previous_version)
                    return -1
                else:
                    version = clone_version(previous_version, args.version)
                    version.version = str(args.version)
                    version.previous_version = str(args.previous_version)
                    version.expanded = False
                    version.done = False
                    version.is_new = False
                    version.revised = False
                    version.hashes = ("","","")
                    version.revised = [True, True] # Assume something has changed
                    version.sources= [False,False]
                    version.min_timestamp = datetime.utcnow()
                    version.max_timestamp = None
                    sess.commit()

        if not version.done:
            build_version(sess, args, all_sources, version)
        else:
            rootlogger.info("    version %s previously built", args.version)
        return

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--previous_version', default=7, help='Previous version')
    parser.add_argument('--version', default=8, help='Version to work on')
    parser.add_argument('--client', default=storage.Client())
    args = parser.parse_args()
    parser.add_argument('--db', default=f'idc_v7', help='Database on which to operate')
    parser.add_argument('--project', default='idc-dev-etl')
    parser.add_argument('--wsi_src_bucket', default=storage.Bucket(args.client,'af-dac-wsi-conversion-results'), help='Bucket in which to find WSI DICOMs')
    parser.add_argument('--prestaging_bucket_prefix', default=f'idc_v{args.version}_', help='Copy instances here before forwarding to --staging_bucket')
    parser.add_argument('--num_processes', default=0, help="Number of concurrent processes")
    # parser.add_argument('--todos', default='{}/logs/path_ingest_v{}_todos.txt'.format(os.environ['PWD'], args.version ), help="Collections to include")
    parser.add_argument('--skips', default=f"{os.environ['PWD']}/logs/ingest_v{args.version}_skips.txt", help="Collections to skip")
    parser.add_argument('--server', default="", help="NBIA server to access. Set to NLST for NLST ingestion")
    parser.add_argument('--dicom', default='/mnt/disks/idc-etl/dicom', help='Directory in which to expand downloaded zip files')
    args = parser.parse_args()
    args.id = 0 # Default process ID

    print("{}".format(args), file=sys.stdout)

    rootlogger = logging.getLogger('root')

    errlogger = logging.getLogger('root.err')

    ingest(args)
