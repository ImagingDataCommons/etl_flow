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

# Copy some set of BQ tables from one dataset to another. Used to populate public dataset
# Uses gsutil -m cp. Not continuable or performant.
import argparse
import sys
import os
import logging
from logging import INFO, DEBUG
from subprocess import run

from idc.models import Version, Collection, Patient, Study, Series, Instance, Retired, WSI_metadata, instance_source
from sqlalchemy import select,delete
from sqlalchemy.orm import Session

from python_settings import settings
import settings as etl_settings
settings.configure(etl_settings)

from sqlalchemy import create_engine
from sqlalchemy_utils import register_composites

rootlogger = logging.getLogger('root')
errlogger = logging.getLogger('root.err')


def copy_bucket(args, src_bucket):
    print("Copying {}".format(src_bucket), flush=True)
    try:
        result = run(['gsutil', '-m', 'cp', f'gs://{src_bucket}/*',
                      f'gs://{args.dst_bucket}'])
        print("   {} copied, results: {}".format(src_bucket, result), flush=True)
        if result.returncode:
            errlogger.error('Copy %s failed: %s', src_bucket, result.stderr)
            return {"bucket": src_bucket, "status": -1}
        rootlogger.info('%s',src_bucket)
        return 0
    except:
        errlogger.error("Error copying {}: {},{},{}".format(src_bucket, sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2]), file=sys.stdout, flush=True)
        raise


def copy_buckets(args):
    # rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/copy_bucket_v{}_log.log'.format(os.environ['PWD'], args.version))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    # errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/copy_bucket_v{}_err.log'.format(os.environ['PWD'], args.version))
    errformatter = logging.Formatter('{%(pathname)s:%(lineno)d} %(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    rootlogger.debug('Args: %s', args)

    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{args.db}'
    # sql_engine = create_engine(sql_uri, echo=True)
    sql_engine = create_engine(sql_uri)
    args.sql_engine = sql_engine

    conn = sql_engine.connect()
    register_composites(conn)

    dones = open('{}/logs/copy_bucket_v{}_log.log'.format(os.environ['PWD'], args.version)).read().splitlines()

    # Add a new Version with idc_version_number args.version, if it does not already exist
    with Session(sql_engine) as sess:
        idc_collections = [c.collection_id for c in sess.query(Collection).\
            filter(Collection.rev_idc_version==5 and Collection.done == True ).order_by('collection_id')]
        for c in idc_collections:
            src_bucket = f"{args.src_bucket_prefix}{c.lower().replace('-', '_').replace(' ', '_')}"
            if not c in dones:
                result = copy_bucket(args, src_bucket)




