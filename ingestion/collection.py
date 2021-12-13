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

import time
from datetime import datetime, timedelta
import logging
from uuid import uuid4
from idc.models import Version, Collection, Patient
from ingestion.patient import clone_patient, build_patient, retire_patient

from python_settings import settings

from multiprocessing import Process, Queue, Lock
from queue import Empty

from sqlalchemy.orm import Session
from sqlalchemy_utils import register_composites
from utilities.get_collection_dois import get_data_collection_doi, get_analysis_collection_dois

from ingestion.utils import accum_sources, empty_bucket, create_prestaging_bucket

from sqlalchemy import create_engine

from ingestion.sources import All
from ingestion.sources_mtm import All_mtm

rootlogger = logging.getLogger('root')
errlogger = logging.getLogger('root.err')


def clone_collection(collection,uuid):
    new_collection = Collection(uuid=uuid)
    for key, value in collection.__dict__.items():
        if key not in ['_sa_instance_state', 'uuid', 'versions', 'patients']:
            setattr(new_collection, key, value)
    for patient in collection.patients:
        new_collection.patients.append(patient)
    return new_collection


def retire_collection(args, collection):
    # If this object has children from source, delete them
    for patient in collection.patients:
        retire_patient(args, patient)
    collection.final_idc_version = args.previous_version


PATIENT_TRIES=5
def worker(input, output, args, data_collection_doi, analysis_collection_dois, lock):
    # rootlogger.debug('p%s: Worker starting: args: %s', args.id, args)
    sql_engine = create_engine(args.sql_uri)
    with Session(sql_engine) as sess:

        if args.build_mtm_db:
            # When build the many-to-many DB, we mine some existing one to many DB
            sql_uri_mtm = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/idc_v{args.version}'
            sql_engine_mtm = create_engine(sql_uri_mtm, echo=True)
            conn_mtm = sql_engine_mtm.connect()
            register_composites(conn_mtm)
            # Use this to see the SQL being sent to PSQL
            all_sources = All_mtm(sess, Session(sql_engine_mtm), args.version)
        else:
            all_sources = All(sess, args.version)
        all_sources.lock = lock
        # rootlogger.info('p%s: Worker starting: args: %s', args.id, args)
        # rootlogger.info('p%s: Source: args: %s', args.id, source)
        # rootlogger.info('p%s: conn: %s, cur: %s', args.id, conn, cur)
        # rootlogger.info('p%s: Lock: _rand %s, _sem_lock: %s', args.id, source.lock._rand, source.lock._semlock)
        for more_args in iter(input.get, 'STOP'):
            for attempt in range(PATIENT_TRIES):
                time.sleep((2**attempt)-1)
                index, collection_id, submitter_case_id = more_args
                try:
                    version = sess.query(Version).filter(Version.version==args.version).one()
                    # collection = sess.query(Collection).where(Collection.collection_id==collection_id).one()
                    collection = next(collection for collection in version.collections if collection.collection_id ==collection_id)
                    patient = next(patient for patient in collection.patients if patient.submitter_case_id==submitter_case_id)
                    # rootlogger.debug("p%s: In worker, sess: %s, submitter_case_id: %s", args.id, sess, submitter_case_id)
                    build_patient(sess, args, all_sources, index, data_collection_doi, analysis_collection_dois, version, collection, patient)
                    break
                except Exception as exc:
                    errlogger.error("p%s, exception %s; reattempt %s on patient %s/%s, %s; %s", args.id, exc, attempt, collection.collection_id, patient.submitter_case_id, index, time.asctime())
                    sess.rollback()

            if attempt == PATIENT_TRIES:
                errlogger.error("p%s, Failed to process patient: %s", args.id, patient.submitter_case_id)
                sess.rollback()
            output.put(patient.submitter_case_id)


def expand_collection(sess, args, all_sources, collection):
    if not args.build_mtm_db:
        # Since we are starting, delete everything from the prestaging bucket.
        rootlogger.info("Emptying prestaging bucket")
        begin = time.time()
        create_prestaging_bucket(args)
        empty_bucket(args.prestaging_bucket)
        # Since we are starting, delete everything from the prestaging bucket.
        duration = str(timedelta(seconds=(time.time() - begin)))
        rootlogger.info("Emptying prestaging bucket completed in %s", duration)

    # Get the patients that the sources know about
    patients = all_sources.patients(collection)

    # Check for duplicates
    if len(patients) != len(set(patients)):
        errlogger.error("\tp%s: Duplicate patients in expansion of collection %s", args.id,
                        collection.collection_id)
        raise RuntimeError("p%s: Duplicate patients expansion of collection %s", args.id,
                           collection.collection_id)
    if collection.is_new:
        # ...then all its children are new
        metadata = []
        for patient in sorted(patients):
            rev_patient = Patient()

            rev_patient.submitter_case_id = patient
            if args.build_mtm_db:
                rev_patient.idc_case_id = patients[patient]['idc_case_id']
                rev_patient.min_timestamp = patients[patient]['min_timestamp']
                rev_patient.max_timestamp = patients[patient]['max_timestamp']
                rev_patient.sources = patients[patient]['sources']
                rev_patient.hashes = patients[patient]['hashes']
            else:
                rev_patient.idc_case_idc = str(uuid4())
                rev_patient.min_timestamp = datetime.utcnow()
                rev_patient.sources = (False, False)
                rev_patient.hashes = ("", "", "")
            rev_patient.uuid = str(uuid4())
            rev_patient.max_timestamp = rev_patient.min_timestamp
            rev_patient.init_idc_version=args.version
            rev_patient.rev_idc_version=args.version
            rev_patient.final_idc_version=0
            rev_patient.revised=False
            rev_patient.done=False
            rev_patient.is_new=True
            rev_patient.expanded=False

            collection.patients.append(rev_patient)
    else:
        idc_objects = {object.submitter_case_id: object for object in collection.patients}

        new_objects = sorted([id for id in patients if id not in idc_objects])
        retired_objects = sorted([idc_objects[id] for id in idc_objects if id not in patients], key=lambda patient: patient.submitter_case_id)
        existing_objects = sorted([idc_objects[id] for id in idc_objects if id in patients], key=lambda patient: patient.submitter_case_id)

        for patient in retired_objects:
            breakpoint()
            rootlogger.info('Patient %s retiring', patient.submitter_case_id)
            retire_patient(args, patient)
            collection.patients.remove(patient)


        for patient in existing_objects:
            if all_sources.patient_was_updated(patient):

                rootlogger.info('p%s **Revising patient %s', args.id, patient.submitter_case_id)
                # Mark when we started work on this patient
                assert args.version == patients[patient.submitter_case_id]['rev_idc_version']
                rev_patient = clone_patient(patient, str(uuid4()))
                # rev_patient.collections.append(collection)
                # rev_patient.uuid = str(uuid4())
                rev_patient.revised = True
                rev_patient.done = False
                rev_patient.is_new = False
                rev_patient.expanded = False
                if args.build_mtm_db:
                    rev_patient.min_timestamp = patients[patient.submitter_case_id]['min_timestamp']
                    rev_patient.max_timestamp = patients[patient.submitter_case_id]['max_timestamp']
                    rev_patient.sources = patients[patient.submitter_case_id]['sources']
                    rev_patient.hashes = patients[patient.submitter_case_id]['hashes']
                    rev_patient.rev_idc_version = patients[patient.submitter_case_id]['rev_idc_version']
                else:
                    rev_patient.rev_idc_version = args.version
                collection.patients.append(rev_patient)

                # Mark the now previous version of this object as having been replaced
                # and drop it from the revised collection
                patient.final_idc_version = args.previous_version
                collection.patients.remove(patient)

            else:
                # The patient is unchanged. Just add it to the collection
                if not args.build_mtm_db:
                    # Stamp this series showing when it was checked
                    patient.min_timestamp = datetime.utcnow()
                    patient.max_timestamp = datetime.utcnow()
                    # Make sure the collection is marked as done and expanded
                    # Shouldn't be needed if the previous version is done
                    patient.done = True
                    patient.expanded = True
                rootlogger.info('Patient %s unchanged', patient.submitter_case_id)
                # collection.patients.append(patient)

        for patient in sorted(new_objects):
            rev_patient = Patient()

            rev_patient.submitter_case_id = patient
            if args.build_mtm_db:
                rev_patient.idc_case_id = patients[patient]['idc_case_id']
                rev_patient.min_timestamp = patients[patient]['min_timestamp']
                rev_patient.max_timestamp = patients[patient]['max_timestamp']
                rev_patient.sources = patients[patient]['sources']
                rev_patient.hashes = patients[patient]['hashes']
            else:
                rev_patient.idc_case_idc = str(uuid4())
                rev_patient.min_timestamp = datetime.utcnow()
                rev_patient.sources = (False, False)
                rev_patient.hashes = ("", "", "")
            rev_patient.uuid = str(uuid4())
            rev_patient.max_timestamp = rev_patient.min_timestamp
            rev_patient.init_idc_version=args.version
            rev_patient.rev_idc_version=args.version
            rev_patient.final_idc_version=0
            rev_patient.revised=False
            rev_patient.done=False
            rev_patient.is_new=True
            rev_patient.expanded=False

            collection.patients.append(rev_patient)
    collection.expanded = True
    sess.commit()
    return
    # rootlogger.debug("p%s: Expanded collection %s",args.id, collection.collection_id)


def build_collection(sess, args, all_sources, collection_index, version, collection):
    begin = time.time()
    args.prestaging_bucket = f"{args.prestaging_bucket_prefix}{collection.collection_id.lower().replace(' ','_').replace('-','_')}"
    if not collection.expanded:
        expand_collection(sess, args, all_sources, collection)
    rootlogger.info("Collection %s, %s, %s patients", collection.collection_id, collection_index, len(collection.patients))
    if not args.build_mtm_db:
        # Get the lists of data and analyis series in this patient
        data_collection_doi = get_data_collection_doi(collection.collection_id, server=args.server)
        if data_collection_doi=="":
            if collection.collection_id=='NLST':
                data_collection_doi = '10.7937/TCIA.hmq8-j677'
            elif collection.collection_id=='CMMD':
                data_collection_doi = '10.7937/tcia.eqde-4b16'
            elif collection.collection_id == "Duke-Breast-Cancer-MRI":
                data_collection_doi = '10.7937/TCIA.e3sv-re93'
            elif collection.collection_id == 'QIBA-CT-Liver-Phantom':
                data_collection_doi = '10.7937/TCIA.RMV0-9Y95'
            elif collection.collection_id == 'Training-Pseudo':
                data_collection_doi == 'Training-Pseudo-TBD-DOI'
            elif collection.collection_id == 'B-mode-and-CEUS-Liver':
                data_collection_doi == '10.7937/TCIA.2021.v4z7-tc39'
            else:
                errlogger.error('No DOI for collection %s', collection.collection_id)
                return
        pre_analysis_collection_dois = get_analysis_collection_dois(collection.collection_id, server=args.server)
        analysis_collection_dois = {x['SeriesInstanceUID']: x['SourceDOI'] for x in pre_analysis_collection_dois}
    else:
        data_collection_doi = ""
        analysis_collection_dois = {}

    if args.num_processes==0:
        # for series in sorted_seriess:
        args.id = 0
        for patient in collection.patients:
            patient_index = f'{collection.patients.index(patient) + 1} of {len(collection.patients)}'
            if not patient.done:
                build_patient(sess, args, all_sources, patient_index, data_collection_doi, analysis_collection_dois, version, collection, patient)
            else:
                # if (collection.patients.index(patient) % 100 ) == 0:
                if True:
                    rootlogger.info("  p%s: Patient %s, %s, previously built", args.id, patient.submitter_case_id,
                                patient_index)

    else:
        processes = []
        # Create queues
        task_queue = Queue()
        done_queue = Queue()

        # List of patients enqueued
        enqueued_patients = []

        num_processes = min(args.num_processes, len(collection.patients))

        # Start worker processes
        lock = Lock()
        for process in range(num_processes):
            args.id = process+1
            processes.append(
                Process(target=worker, args=(task_queue, done_queue, args, data_collection_doi, analysis_collection_dois, lock )))
            processes[-1].start()

        # Enqueue each patient in the the task queue
        args.id = 0
        patients = sorted(collection.patients, key=lambda patient: patient.done, reverse=True)
        for patient in patients:
            patient_index = f'{collection.patients.index(patient) + 1} of {len(collection.patients)}'
            if not patient.done:
                # task_queue.put((patient_index, version.idc_version_number, collection.collection_id, patient.submitter_case_id))
                task_queue.put((patient_index, collection.collection_id, patient.submitter_case_id))
                enqueued_patients.append(patient.submitter_case_id)
            else:
                if (collection.patients.index(patient) % 100 ) == 0:
                    rootlogger.info("  p%s: Patient %s, %s, previously built", args.id, patient.submitter_case_id,
                                patient_index)

        # Collect the results for each patient
        try:
            while not enqueued_patients == []:
                # Timeout if waiting too long
                results = done_queue.get(True)
                enqueued_patients.remove(results)

            # Tell child processes to stop
            for process in processes:
                task_queue.put('STOP')

            # Wait for them to stop
            for process in processes:
                process.join()

            sess.commit()

        except Empty as e:
            errlogger.error("Timeout in build_collection %s", collection.collection_id)
            for process in processes:
                process.terminate()
                process.join()
            sess.rollback()
            duration = str(timedelta(seconds=(time.time() - begin)))
            rootlogger.info("Collection %s, %s, NOT completed in %s", collection.collection_id, collection_index,
                            duration)

    if all([patient.done for patient in collection.patients]):
        # collection.min_timestamp = min([patient.min_timestamp for patient in collection.patients if patient.min_timestamp != None])
        collection.max_timestamp = max([patient.max_timestamp for patient in collection.patients if patient.max_timestamp != None])

        if args.build_mtm_db:
            collection.done = True
            sess.commit()
            duration = str(timedelta(seconds=(time.time() - begin)))
            rootlogger.info("Collection %s, %s, completed in %s", collection.collection_id, collection_index,
                        duration)
        else:
        # Get hash of children
            hashes = all_sources.idc_collection_hashes(collection)
            try:
                if all_sources.src_collection_hashes(collection.collection_id) != hashes:
                    errlogger.error('Hash match failed for collection %s', collection.collection_id)
                else:
                    # Test whether anything has changed
                    if hashes != collection.hashes:
                        collection.hashes = hashes
                        collection.sources = accum_sources(collection, collection.patients)
                        collection.rev_idc_version = args.version
                        if collection.is_new:
                            collection.revised = True
                    else:
                        rootlogger.info("Collection %s, %s, unchanged", collection.collection_id, collection_index)

                    collection.done = True
                    sess.commit()
                duration = str(timedelta(seconds=(time.time() - begin)))
                rootlogger.info("Collection %s, %s, completed in %s", collection.collection_id, collection_index,
                                duration)
            except Exception as exc:
                errlogger.error('Could not validate collection hash for %s: %s', collection.collection_id, exc)

    else:
        duration = str(timedelta(seconds=(time.time() - begin)))
        rootlogger.info("Collection %s, %s, not completed in %s", collection.collection_id, collection_index,
                        duration)

