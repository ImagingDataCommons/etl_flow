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
import sys
import argparse
import logging
from logging import INFO, DEBUG, ERROR
from datetime import datetime, timedelta
import shutil
from multiprocessing import Lock, shared_memory
from idc.models import Base, Version, Collection
from utilities.tcia_helpers import get_access_token

from ingestion.utils import list_skips
from ingestion.version import clone_version, build_version

from python_settings import settings

from sqlalchemy import create_engine
from sqlalchemy_utils import register_composites
from sqlalchemy.orm import Session

from ingestion.all_sources import All
from http.client import HTTPConnection
HTTPConnection.debuglevel = 0



rootlogger = logging.getLogger('root')
successlogger = logging.getLogger('root.success')
debuglogger = logging.getLogger('root.prog')
errlogger = logging.getLogger('root.err')

def ingest(args):
    # HTTPConnection.debuglevel = 0
    logging.getLogger("urllib3").setLevel(logging.ERROR)

    # # rootlogger = logging.getLogger('root')
    # root_fh = logging.FileHandler('{}/logs/v{}_log.log'.format(os.environ['PWD'], settings.CURRENT_VERSION))
    # rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    # rootlogger.addHandler(root_fh)
    # root_fh.setFormatter(rootformatter)
    # rootlogger.setLevel(INFO)

    # successlogger = logging.getLogger('root.success')
    # success_fh = logging.FileHandler('{}/logs/v{}_log.log'.format(os.environ['PWD'], settings.CURRENT_VERSION))
    success_fh = logging.FileHandler(f'{args.log_dir}/success.log')
    successformatter = logging.Formatter('%(levelname)s:success:%(message)s')
    successlogger.addHandler(success_fh)
    success_fh.setFormatter(successformatter)
    successlogger.setLevel(INFO)

    # debuglogger = logging.getLogger('root.debug')
    # debug_fh = logging.FileHandler('{}/logs/v{}_log.log'.format(os.environ['PWD'], settings.CURRENT_VERSION))
    debug_fh = logging.FileHandler(f'{args.log_dir}/debug.log')
    debugformatter = logging.Formatter('%(levelname)s:debug:%(message)s')
    debuglogger.addHandler(debug_fh)
    debug_fh.setFormatter(debugformatter)
    debuglogger.setLevel(DEBUG)

    # errlogger = logging.getLogger('root.err')
    # err_fh = logging.FileHandler('{}/logs/v{}_err.log'.format(os.environ['PWD'], settings.CURRENT_VERSION))
    err_fh = logging.FileHandler(f'{args.log_dir}/error.log')
    errformatter = logging.Formatter('{%(pathname)s:%(lineno)d} %(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)
    errlogger.setLevel(ERROR)

    rootlogger.debug('Args: %s', args)

    # Create a local working directory into which data
    # from TCIA is copied
    if os.path.isdir('{}'.format(args.dicom)):
        shutil.rmtree('{}'.format(args.dicom))
    os.mkdir('{}'.format(args.dicom))


    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{settings.CLOUD_DATABASE}'
    # sql_engine = create_engine(sql_uri, echo=True) # Use this to see the SQL being sent to PSQL
    sql_engine = create_engine(sql_uri)
    # args.sql_uri = sql_uri # The subprocesses need this uri to create their own SQL engine

    # Create the tables if they do not already exist
    Base.metadata.create_all(sql_engine)

    # Enable the underlying psycopg2 to deal with composites
    conn = sql_engine.connect()
    register_composites(conn)

    with Session(sql_engine) as sess:
        # Get a sharable NBIA access token
        access = shared_memory.ShareableList(get_access_token())
        args.access = access

        args.skipped_tcia_collections = list_skips(sess, Base, args.skipped_tcia_groups, args.skipped_tcia_collections, args.included_tcia_collections)
        args.skipped_path_collections = list_skips(sess, Base, args.skipped_path_groups, args.skipped_path_collections, args.included_path_collections)

        # Now create a table of collections for which tcia or path ingestion or both, are to be skipped.
        # Populate with tcia skips
        skipped_collections = \
            {collection_id:[True, False] for collection_id in args.skipped_tcia_collections}
        # Now add path skips
        for collection_id in args.skipped_path_collections:
            if collection_id in skipped_collections:
                skipped_collections[collection_id][1] = True
            else:
                skipped_collections[collection_id] = [False, True]
        args.skipped_collections = skipped_collections
        all_sources = All(args.pid, sess, settings.CURRENT_VERSION, args.access,
                          args.skipped_tcia_collections, args.skipped_path_collections, Lock())

        version = sess.query(Version).filter(Version.version == settings.CURRENT_VERSION).first()
        if not version:
            previous_version = sess.query(Version).filter(Version.version == settings.PREVIOUS_VERSION).first()
            if not previous_version:
                # if it doesn't exist then we are creating V1
                version = Version()
                version.expanded=False
                version.done=False
                version.is_new=True
                version.revised=False
                version.hashes = ["","",""]
                version.sources = [False,False]
                version.version = str(settings.CURRENT_VERSION)
                version.previous_version = settings.PREVIOUS_VERSION
                sess.add(version)
                sess.commit()
            else:
                # Previous version exists. Create next version as a
                # clone of previous version
                if not previous_version.done:
                    errlogger.error('Previous version %s is not done',settings.PREVIOUS_VERSION)
                    return -1
                else:
                    version = clone_version(previous_version, settings.CURRENT_VERSION)
                    version.version = str(settings.CURRENT_VERSION)
                    version.previous_version = str(settings.PREVIOUS_VERSION)
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
            rootlogger.info("    version %s previously built", settings.CURRENT_VERSION)
        return

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # parser.add_argument('--previous_version', default=8, help='Previous version')
    # parser.add_argument('--version', default=9, help='Version to work on')
    # parser.add_argument('--client', default=storage.Client())
    # args = parser.parse_args()
    # parser.add_argument('--db', default=f'idc_v{settings.CURRENT_VERSION}', help='Database on which to operate')
    # parser.add_argument('--project', default='idc-dev-etl')
    parser.add_argument('--num_processes', default=16, help="Number of concurrent processes")

    parser.add_argument('--skipped_tcia_groups', default=['redacted_collections', 'excluded_collections'],\
                        help="List of tables containing tcia_api_collection_ids of tcia collections to be skipped")
    parser.add_argument('--skipped_tcia_collections', default=['NLST', 'HCC-TACE-Seg'], help='List of additional tcia collections to be skipped')
    parser.add_argument('--included_tcia_collections', default=[], help='List of tcia collections to exclude from skipped')
    parser.add_argument('--prestaging_tcia_bucket_prefix', default=f'idc_v{settings.CURRENT_VERSION}_tcia_', help='Copy tcia instances here before forwarding to --staging_bucket')

    parser.add_argument('--skipped_path_groups', default=['redacted_collections', 'excluded_collections'],\
                        help="List of tables containind tcia_api_collection_ids of path collections to be skipped")
    parser.add_argument('--skipped_path_collections', default=['HCC-TACE-Seg'], help='List of additional path collections to be skipped')
    parser.add_argument('--included_path_collections', default=['TCGA-GBM', 'TCGA-HNSC', 'TCGA-LGG', 'CPTAC-GBM', 'CPTAC-HNSCC'], help='List of path collections to exclude from skipped')
    parser.add_argument('--server', default="", help="NBIA server to access. Set to NLST for NLST ingestion")
    parser.add_argument('--prestaging_path_bucket_prefix', default=f'idc_v{settings.CURRENT_VERSION}_path_', help='Copy path instances here before forwarding to --staging_bucket')

    parser.add_argument('--dicom', default='/mnt/disks/idc-etl/dicom', help='Directory in which to expand downloaded zip files')
    parser.add_argument('--log_dir', default=f'/mnt/disks/idc-etl/logs/ingestion')
    args = parser.parse_args()
    args.pid = 0 # Default process ID

    print("{}".format(args), file=sys.stdout)

    # rootlogger = logging.getLogger('root')
    successlogger = logging.getLogger('root.success')
    debuglogger = logging.getLogger('root.prog')
    errlogger = logging.getLogger('root.err')

    ingest(args)
