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
import logging
from logging import INFO, DEBUG
from datetime import datetime, timedelta
import shutil
from multiprocessing import Lock
from idc.models import Base, Version, Collection
from sqlalchemy.orm import Session

from ingestion.version import clone_version, build_version

from python_settings import settings

from sqlalchemy import create_engine
from sqlalchemy_utils import register_composites

from ingestion.all_sources import All
from ingestion.sources_mtm import All_mtm

rootlogger = logging.getLogger('root')
errlogger = logging.getLogger('root.err')

def test_source(args, all_sources):
    from types import  SimpleNamespace
    metadata = all_sources.collections()

    d = {'collection_id':list(metadata.values())[0]['collection_id']}
    object = SimpleNamespace(**d)
    metadata = all_sources.patients(object)

    d = {'submitter_case_id':list(metadata.keys())[0]}
    object = SimpleNamespace(**d)
    metadata = all_sources.studies(object)

    d = {'study_instance_uid':list(metadata.keys())[0]}
    object = SimpleNamespace(**d)
    metadata = all_sources.series(object)

    d = {'series_instance_uid':list(metadata.keys())[0]}
    object = SimpleNamespace(**d)
    metadata = all_sources.instances(object)

    pass



def ingest(args):
    # rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/v{}_log.log'.format(os.environ['PWD'], args.version))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(DEBUG)

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
    sql_engine = create_engine(sql_uri, echo=True) # Use this to see the SQL being sent to PSQL
    # sql_engine = create_engine(sql_uri)
    args.sql_uri = sql_uri # The subprocesses need this uri to create their own SQL engine

    # Create the tables if they do not already exist
    Base.metadata.create_all(sql_engine)

    # Enable the underlying psycopg2 to deal with composites
    conn = sql_engine.connect()
    register_composites(conn)

    with Session(sql_engine) as sess:
        # Get the target version, if it exists
        if args.build_mtm_db:
            # When building the many-to-many DB, we mine some existing one to many DB
            sql_uri_mtm = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/idc_v{args.version}'
            sql_engine_mtm = create_engine(sql_uri_mtm)
            # sql_engine_mtm = create_engine(sql_uri_mtm, echo=True)
            conn_mtm = sql_engine_mtm.connect()

            register_composites(conn_mtm)

            # Use this to see the SQL being sent to PSQL
            all_sources = All_mtm(sess, Session(sql_engine_mtm), args.version)
            # test_source(args, all_sources)

        else:
            all_sources = All(sess, args.version, Lock())
        # all_sources.lock = Lock()

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
                    if args.build_mtm_db:
                        version_metadata = all_sources.versions(args.version)
                        version.hashes = version_metadata[args.version]['hashes']
                        version.sources = (True, True)
                        version.min_timestamp = version_metadata[args.version]['min_timestamp']
                        version.max_timestamp = version_metadata[args.version]['max_timestamp']
                    else:
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
