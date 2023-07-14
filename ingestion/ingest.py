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
from logging import DEBUG, INFO
from datetime import datetime, timedelta
import shutil
from multiprocessing import Lock, shared_memory
from idc.models import Base, Version, Collection
from utilities.tcia_helpers import get_access_token
from utilities.sqlalchemy_helpers import sa_session
from utilities.logging_config import successlogger, errlogger, progresslogger, rootlogger
from ingestion.utilities.utils import list_skips
from ingestion.version import clone_version, build_version
from python_settings import settings
from ingestion.all_sources import All

DICOM_DIR = '/mnt/disks/idc-etl/dicom' # Directory in which to expand downloaded zip files')


def ingest(args):
    # Create a local working directory into which data
    # from TCIA is copied
    if os.path.isdir('{}'.format(args.dicom_dir)):
        shutil.rmtree('{}'.format(args.dicom_dir))
    os.mkdir('{}'.format(args.dicom_dir))

    with sa_session() as sess:
        # Get a sharable NBIA access token
        access = shared_memory.ShareableList(get_access_token())
        args.access = access

        args.skipped_tcia_collections = list_skips(sess, 'tcia', args.skipped_tcia_collections, args.included_tcia_collections)
        args.skipped_idc_collections = list_skips(sess, 'idc', args.skipped_idc_collections, args.included_idc_collections)

        # Now create a table of collections for which tcia or idc ingestion or both, are to be skipped.
        # Populate with tcia skips
        skipped_collections = \
            {collection_id:[True, False] for collection_id in args.skipped_tcia_collections}
        # Now add idc skips
        for collection_id in args.skipped_idc_collections:
            if collection_id in skipped_collections:
                skipped_collections[collection_id][1] = True
            else:
                skipped_collections[collection_id] = [False, True]
        args.skipped_collections = skipped_collections
        all_sources = All(args.pid, sess, settings.CURRENT_VERSION, args.access,
                          args.skipped_tcia_collections, args.skipped_idc_collections, Lock())

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
            successlogger.info("    version %s previously built", settings.CURRENT_VERSION)
        return

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--num_processes', type=int, default=12, help="Number of concurrent processes")

    # parser.add_argument('--skipped_tcia_groups', nargs='*', default=['redacted_collections', 'excluded_collections'],\
    #                     help="List of tables containing tcia_api_collection_ids of tcia collections to be skipped")
    parser.add_argument('--skipped_tcia_collections', nargs='*', \
            default=['NLST', 'APOLLO-5-ESCA', 'APOLLO-5-LSCC', 'APOLLO-5-LUAD', 'APOLLO-5-PAAD', 'APOLLO-5-THYM'], \
                        help='List of additional tcia collections to be skipped')
    # parser.add_argument('--included_tcia_collections', nargs='*', default=[], help='List of tcia collections to exclude from skipped groups')
    parser.add_argument('--prestaging_tcia_bucket_prefix', default=f'idc_v{settings.CURRENT_VERSION}_tcia_', help='Copy tcia instances here before forwarding to --staging_bucket')

    # parser.add_argument('--skipped_idc_groups', nargs='*', default=['redacted_collections', 'excluded_collections'],\
    #                     help="List of tables containing tcia_api_collection_ids of idc collections to be skipped")
    parser.add_argument('--skipped_idc_collections', nargs='*',\
            default=[], \
                        help='List of additional idc collections to be skipped')
    # # parser.add_argument('--included_idc_collections', nargs='*', \
    # #         default=[], help='List of idc collections to include (exclude from skipped groups)')
    # parser.add_argument('--server', default="", help="NBIA server to access. Set to NLST for NLST ingestion")
    parser.add_argument('--prestaging_idc_bucket_prefix', default=f'idc_v{settings.CURRENT_VERSION}_idc_', help='Copy idc instances here before forwarding to --staging_bucket')

    parser.add_argument('--stop_after_collection_summary', type=bool, default=False, \
                        help='Stop after printing a summary of collection dispositions')

    args = parser.parse_args()
    args.pid = 0 # Default process ID
    args.dicom_dir = DICOM_DIR

    print("{}".format(args), file=sys.stdout)

    rootlogger.setLevel(INFO)
    successlogger.setLevel(INFO)
    progresslogger.setLevel(INFO)

    ingest(args)
