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

# Update how hashes.all_hash is generate:
# For series, it is the hash of child instance hashes.
# For higher level objects it is the hash of child
# hashes.all_hashes.

import os
import argparse
import logging
import time
from logging import INFO

from idc.models import Base, Version, Collection, Patient, Study, Series
import settings as etl_settings
from python_settings import settings
settings.configure(etl_settings)
from google.cloud import storage
from ingestion.utilities.utils import get_merkle_hash
from utilities.logging_config import successlogger, progresslogger, errlogger
import settings

from multiprocessing import Process, Queue, Lock, shared_memory
from queue import Empty

from sqlalchemy import create_engine
from sqlalchemy_utils import register_composites
from sqlalchemy.orm import Session


def update_version_hashes(args, sess, parent_class, table, dones):
    parents = sess.query(table).all()
    n = 0
    cnt = len(parents)
    changed = 0
    for parent in parents:
        if True:
            parent_hashes = list(parent.hashes)
            prev_hash = parent_hashes[-1]
            child_hashes = [child.hashes.all_sources for child in parent.collections]
            parent_hashes[-1] = get_merkle_hash(child_hashes)
            if parent.hashes[-1] != parent_hashes[-1]:
                parent.hashes = parent_hashes
                progresslogger.info(f'{args.id}: {n}of{cnt}: {parent_class} {parent.version}: {prev_hash}-->{parent_hashes[-1]} ')

                if prev_hash:
                    errlogger.error(f'{args.id}: {n}of{cnt}: {parent_class} {parent.version}: {prev_hash}-->{parent_hashes[-1]} ')
                changed += 1
                # if not changed%100 :
                #     # sess.commit()
                #     progresslogger.info(f'{like}: {args.id}: Changed {changed}, {time.time() - strt}')
                #     strt = time.time()
            else:
                progresslogger.info(f'{args.id}: {n}of{cnt}: {parent_class} {parent.version}: == ')
        else:
            progresslogger.info(f'{args.id}: {n}of{cnt}: {parent_class} {parent.version}: previously done')
        n += 1
    sess.commit()
    return


def update_collection_hashes(args, sess, parent_class, table, dones):
    parents = sess.query(table).all()
    n = 0
    cnt = len(parents)
    changed = 0
    for parent in parents:
        if True:
            parent_hashes = list(parent.hashes)
            prev_hash = parent_hashes[-1]
            child_hashes = [child.hashes.all_sources for child in parent.patients]
            parent_hashes[-1] = get_merkle_hash(child_hashes)
            if parent.hashes[-1] != parent_hashes[-1]:
                parent.hashes = parent_hashes
                progresslogger.info(f'{args.id}: {n}of{cnt}: {parent_class} {parent.uuid}: {prev_hash}-->{parent_hashes[-1]} ')

                if prev_hash:
                    errlogger.error(f'{args.id}: {n}of{cnt}: {parent_class} {parent.uuid}: {prev_hash}-->{parent_hashes[-1]} ')
                changed += 1
                # if not changed%100 :
                #     # sess.commit()
                #     progresslogger.info(f'{like}: {args.id}: Changed {changed}, {time.time() - strt}')
                #     strt = time.time()
            else:
                progresslogger.info(f'{args.id}: {n}of{cnt}: {parent_class} {parent.uuid}: == ')
        else:
            progresslogger.info(f'{args.id}: {n}of{cnt}: {parent_class} {parent.uuid}: previously done')
        n += 1
    sess.commit()
    return


def update_patient_hashes(args, sess, parent_class, table, like, dones):
    parents = sess.query(table).filter(table.uuid.ilike(like + '%')).all()
    n = 0
    cnt = len(parents)
    changed = 0
    success = []
    for parent in parents:
        if parent.uuid not in dones:
            parent_hashes = list(parent.hashes)
            prev_hash = parent_hashes[-1]
            child_hashes = [child.hashes.all_sources for child in parent.studies]
            parent_hashes[-1] = get_merkle_hash(child_hashes)
            if parent.hashes[-1] != parent_hashes[-1]:
                if prev_hash:
                    errlogger.error(f'{args.id}: {n}of{cnt}: {parent_class} {parent.uuid}: {prev_hash}-->{parent_hashes[-1]} ')
                parent.hashes = parent_hashes
                progresslogger.info(f'{args.id}: {n}of{cnt}: {parent_class} {parent.uuid}: {prev_hash}-->{parent_hashes[-1]} ')

                changed += 1
                # if not changed%100 :
                #     # sess.commit()
                #     progresslogger.info(f'{like}: {args.id}: Changed {changed}, {time.time() - strt}')
                #     strt = time.time()
            else:
                progresslogger.info(f'{args.id}: {n}of{cnt}: {parent_class} {parent.uuid}: == ')

            success.append(parent.uuid)
        else:
            progresslogger.info(f'{args.id}: {n}of{cnt}: {parent_class} {parent.uuid}: previously done')
        n += 1
    sess.commit()
    for row in success:
        successlogger.info(row)
    return


def update_study_hashes(args, sess, parent_class, table, like, dones):
    parents = sess.query(table).filter(table.uuid.ilike(like + '%')).all()
    n = 0
    cnt = len(parents)
    changed = 0
    success=[]
    for parent in parents:
        if parent.uuid not in dones:
            parent_hashes = list(parent.hashes)
            prev_hash = parent_hashes[-1]
            child_hashes = [child.hashes.all_sources for child in parent.seriess]
            parent_hashes[-1] = get_merkle_hash(child_hashes)
            if parent.hashes[-1] != parent_hashes[-1]:
                parent.hashes = parent_hashes
                progresslogger.info(f'{args.id}: {n}of{cnt}: {parent_class} {parent.uuid}: {prev_hash}-->{parent_hashes[-1]} ')

                if prev_hash:
                    errlogger.error(f'{args.id}: {n}of{cnt}: {parent_class} {parent.uuid}: {prev_hash}-->{parent_hashes[-1]} ')
                changed += 1
                # if not changed%100 :
                #     # sess.commit()
                #     progresslogger.info(f'{like}: {args.id}: Changed {changed}, {time.time() - strt}')
                #     strt = time.time()
            else:
                progresslogger.info(f'{args.id}: {n}of{cnt}: {parent_class} {parent.uuid}: == ')
            success.append(parent.uuid)
        else:
            progresslogger.info(f'{args.id}: {n}of{cnt}: {parent_class} {parent.uuid}: previously done')
        n += 1
    sess.commit()
    for row in success:
        successlogger.info(row)
    return


def update_series_hashes(args, sess, parent_class, table, like, dones):
    parents = sess.query(table).filter(table.uuid.ilike(like + '%')).all()
    n = 0
    cnt = len(parents)
    changed = 0
    success = []
    for parent in parents:
        if parent.uuid not in dones:
            parent_hashes = list(parent.hashes)
            prev_hash = parent_hashes[-1]
            child_hashes = [instance.hash for instance in parent.instances]
            parent_hashes[-1] = get_merkle_hash(child_hashes)
            if parent.hashes[-1] != parent_hashes[-1]:
                parent.hashes = parent_hashes
                progresslogger.info(f'{args.id}: {n}of{cnt}: {parent_class} {parent.uuid}: {prev_hash}-->{parent_hashes[-1]} ')

                if prev_hash:
                    errlogger.error(f'{args.id}: {n}of{cnt}: {parent_class} {parent.uuid}: {prev_hash}-->{parent_hashes[-1]} ')
                changed += 1
                # if not changed%100 :
                #     # sess.commit()
                #     progresslogger.info(f'{like}: {args.id}: Changed {changed}, {time.time() - strt}')
                #     strt = time.time()
            else:
                progresslogger.info(f'{args.id}: {n}of{cnt}: {parent_class} {parent.uuid}: == ')
            success.append(parent.uuid)
        else:
            progresslogger.info(f'{args.id}: {n}of{cnt}: {parent_class} {parent.uuid}: previously done')
        n+=1
    sess.commit()
    for row in success:
        successlogger.info(row)
    return

def worker(input, args, func, parent_class, table, dones):
    # rootlogger.debug('p%s: Worker starting: args: %s', args.id, args)
    sql_engine = create_engine(args.sql_uri)
    with Session(sql_engine) as sess:
        for more_args in iter(input.get, 'STOP'):
            like = more_args
            func(args, sess, parent_class, table, like, dones)


def update_some_hashes(args, parent_class, table, func, dones):
    processes = []
    # Create queues
    task_queue = Queue()
    done_queue = Queue()


    num_processes = min(args.processes, 16)

    # Start worker processes
    lock = Lock()
    for process in range(num_processes):
        args.id = process
        processes.append(Process(target=worker, args=(task_queue, args, func, parent_class, table, dones)))
        processes[-1].start()


    # Divide the work into 256 bins
    args.id = 0
    for i in range(256):
        task_queue.put(('{:02x}'.format(i)))

    # Tell child processes to stop
    for process in processes:
        task_queue.put('STOP')

    # Wait for them to stop
    for process in processes:
        process.join()


def update_all_hashes(args):
    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{settings.CLOUD_DATABASE}'
    sql_engine = create_engine(sql_uri)

    # Enable the underlying psycopg2 to deal with composites
    conn = sql_engine.connect()
    register_composites(conn)

    dones = open(successlogger.handlers[0].baseFilename).read().splitlines()

    args.sql_uri = sql_uri
    with Session(sql_engine) as sess:

        update_some_hashes(args, 'Series', Series, update_series_hashes, dones)
        update_some_hashes(args, 'Study', Study, update_study_hashes, dones)
        update_some_hashes(args, 'Patient', Patient, update_patient_hashes, dones)
        update_collection_hashes(args, sess, 'Collection', Collection, dones)
        update_version_hashes(args, sess, 'Version', Version, dones)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='Version to work on')
    parser.add_argument('--client', default=storage.Client())
    args = parser.parse_args()
    parser.add_argument('--db', default=f'idc_vx', help='Database on which to operate')
    parser.add_argument('--processes', default=8, help="Number of concurrent processes")
    args = parser.parse_args()
    args.id = 0 # Default process ID

    update_all_hashes(args)