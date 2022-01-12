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

# Remove versions of one or more collection

import os
import sys
import argparse
from google.cloud import storage
from idc.models import Version, Collection
from ingestion.egest import egest_collection
import settings as etl_settings
from python_settings import settings
if not settings.configured:
    settings.configure(etl_settings)
assert settings.configured

from sqlalchemy import create_engine
from sqlalchemy_utils import register_composites
from sqlalchemy.orm import Session

import logging
from logging import DEBUG


if __name__ == '__main__':
    print('Why were sizes==0?')
    # exit(-1)

    parser = argparse.ArgumentParser()
    # ]
    parser.add_argument('--previous_version', default=6, help='Previous version')
    parser.add_argument('--version', default=7, help='Version to work on')
    parser.add_argument('--client', default=storage.Client())
    args = parser.parse_args()
    parser.add_argument('--db', default=f'idc_v7', help='Database on which to operate')
    parser.add_argument('--project', default='idc-dev-etl')
    parser.add_argument('--collections', default='{}/logs/egest_collections.txt'.format(os.environ['PWD'], args.version ), help="Collections to skip")
    # parser.add_argument('--source', default=TCIA, help="Source (type of data) from which to ingest: 'Pathology' or 'TCIA'")
    parser.add_argument('--server', default="", help="NBIA server to access. Set to NLST for NLST ingestion")
    parser.add_argument('--dicom', default='/mnt/disks/idc-etl/dicom', help='Directory in which to expand downloaded zip files')
    parser.add_argument('--build_mtm_db', default=False, help='True if we are building many-to-many DB')
    args = parser.parse_args()
    args.id = 0 # Default process ID

    print("{}".format(args), file=sys.stdout)

    rootlogger = logging.getLogger('root')
    errlogger = logging.getLogger('root.err')
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


    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{args.db}'
    sql_engine = create_engine(sql_uri, echo=True) # Use this to see the SQL being sent to PSQL
    # sql_engine = create_engine(sql_uri)
    args.sql_uri = sql_uri # The subprocesses need this uri to create their own SQL engine

    # Enable the underlying psycopg2 to deal with composites
    conn = sql_engine.connect()
    register_composites(conn)

    with Session(sql_engine) as sess:

        collections = open(args.collections).read().splitlines()

        version = sess.query(Version).filter(Version.version == args.version).first()
        # for collection_id in collections:
        #     collection = sess.query(Collection).filter(Collection.collection_id==collection_id and
        #         collection.version == args.version).first()
        #     egest_collection(sess, args, collection)
        for collection in version.collections:
            if collection.collection_id in collections:
                version.collections.remove(collection)
                rootlogger.info('\tRemoved collection %s from version %s', collection.collection_id, version.version)
                # If the version of the collection was new in this version, delete it
                if version.version == collection.rev_idc_version :
                    egest_collection(sess, args, collection)

                    # if this is not a new collection, just a new version of an existing collection,
                    # find the previous version and reset it's final_idc_version to 0 to
                    # restore it to the "current" collection.
                    if collection.init_idc_version != collection.rev_idc_version:
                        prev_collection = sess.query(Collection).filter(
                            Collection.collection_id == collection.collection_id and
                            Collection.final_idc_version == args.previous_version).first()
                        prev_collection.final_idc_version = 0
                        version.collections.append(prev_collection)

                sess.commit()


