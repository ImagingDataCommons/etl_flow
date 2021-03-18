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

# This script verifies that the revision of each collection was atomic. I.E.
# it verifies that TCIA did not change the data in a collection while that
# collection was being newly acquired or revised.
#
# This requires that we know the datetime range over which a collection was
# acquired or revised, but we currrently only computed the start of that range.
# We can test whether there have been any changes to a collection since that
# start datatime, and, if so, compute the completion datetime.


import sys
import os
import argparse
import time
from datetime import datetime, timezone, timedelta
import logging
from logging import INFO

import shutil

from idc.models import Version, Collection, Patient, Study, Series, Instance, sql_engine
from sqlalchemy import select
from sqlalchemy.orm import Session
from utilities.tcia_helpers import get_updated_series


def val_collection(sess, args, collection_index, version, collection):
    if True:
        safe_start = collection.collection_timestamp - timedelta(days=1)
        update_start = f'{safe_start.day}/{safe_start.month}/{safe_start.year}'
        print(f'Validating {collection.tcia_api_collection_id} start {update_start}')
        updated_series = get_updated_series(update_start)
        for series in updated_series:
            if series['Collection'] == collection.tcia_api_collection_id:
                print(f'****Collection {collection.tcia_api_collection_id} updated')
                break
        pass


def val_version(sess, args, version):
    if True:
        begin = time.time()
        rootlogger.info("Version %s; %s collections", version.idc_version_number, len(version.collections))
        # repairs = open(args.repairs).read().splitlines()
        for collection in version.collections:
            # if collection.tcia_api_collection_id in repairs:
            if True:
                collection_index = f'{version.collections.index(collection)+1} of {len(version.collections)}'
                val_collection(sess, args, collection_index, version, collection)


def preval(args):
    # Basically add a new Version with idc_version_number args.vnext, if it does not already exist
    with Session(sql_engine) as sess:
        stmt = select(Version).distinct()
        result = sess.execute(stmt)
        version = []
        for row in result:
            if row[0].idc_version_number == args.vnext:
                # We've at least started working on vnext
                version = row[0]
                break

        if not version:
        # If we get here, we have not started work on vnext, so add it to Version
            version = Version(idc_version_number=args.vnext,
                              idc_version_timestamp=datetime.datetime.utcnow(),
                              revised=False,
                              done=False,
                              is_new=True,
                              expanded=False)
            sess.add(version)
            sess.commit()
        val_version(sess, args, version)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--vnext', default=2, help='Next version to generate')
    parser.add_argument('--project', default='idc-dev-etl')
    args = parser.parse_args()

    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/atomlog.log'.format(os.environ['PWD']))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/atomerr.log'.format(os.environ['PWD']))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)


    preval(args)
