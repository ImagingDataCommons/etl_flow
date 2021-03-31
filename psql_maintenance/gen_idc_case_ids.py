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

# One time use script to generate IDC case uuids for patients already in
# the DB.

import sys
import os
import argparse
from pathlib import Path
import time
from datetime import datetime, timezone, timedelta
import logging
from logging import INFO
import pydicom
import hashlib
from subprocess import run, PIPE
import shutil
from multiprocessing import Process, Queue
from queue import Empty
from base64 import b64decode
from pydicom.errors import InvalidDicomError
from uuid import uuid4
from google.cloud import storage
from idc.models import Version, Collection, Patient, Study, Series, Instance, sql_engine
from sqlalchemy import select
from sqlalchemy.orm import Session
from utilities.tcia_helpers import  get_collections, get_TCIA_patients_per_collection, get_TCIA_studies_per_patient, \
    get_TCIA_series_per_study, get_TCIA_instance_uids_per_series, get_TCIA_instances_per_series, get_collection_values_and_counts
from utilities.get_collection_dois import get_data_collection_doi, get_analysis_collection_dois
from google.api_core.exceptions import Conflict

# from python_settings import settings
# import settings as etl_settings
#
# settings.configure(etl_settings)
# assert settings.configured
# import psycopg2
# from psycopg2.extras import DictCursor


PATIENT_TRIES=3


BUF_SIZE = 65536
def md5_hasher(file_path):
    md5 = hashlib.md5()
    with open(file_path, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            md5.update(data)
    return md5.hexdigest()


# Hash a sorted list of hashes
def get_merkle_hash(hashes):
    md5 = hashlib.md5()
    hashes.sort()
    for hash in hashes:
        md5.update(hash.encode())
    return md5.hexdigest()


def build_series(sess, args, series_index, version, collection, patient, study, series, data_collection_doi, analysis_collection_dois):
    # if not series.done:
    if True:
        begin = time.time()
        # if not series.expanded:
        #     expand_series(sess, series)
        rootlogger.info("      p%s: Series %s; %s; %s instances", args.id, series.series_instance_uid, series_index, len(series.instances))
        # build_instances(sess, args, version, collection, patient, study, series)
        # series.series_instances = len(series.instances)
        # series.series_timestamp = min(instance.instance_timestamp for instance in series.instances)
        series.series_hash = get_merkle_hash([instance.instance_hash for instance in series.instances])
        series.done = True
        sess.commit()
        duration = str(timedelta(seconds=(time.time() - begin)))
        rootlogger.info("      p%s: Series %s, %s, completed in %s", args.id, series.series_instance_uid, series_index, duration)
    else:
        rootlogger.info("      p%s: Series %s, %s, previously built", args.id, series.series_instance_uid, series_index)


def expand_study(sess, collection, patient, study, data_collection_doi, analysis_collection_dois):
    rows = get_TCIA_series_per_study(collection.tcia_api_collection_id, patient.submitter_case_id, study.study_instance_uid)
    # If the study is new, then all the studies are new
    if study.is_new:
        for row in rows:
            study.seriess.append(Series(study_id=study.id,
                                         idc_version_number=study.idc_version_number,
                                         series_timestamp=datetime(1970,1,1,0,0,0),
                                         series_instance_uid=row['SeriesInstanceUID'],
                                         series_uuid=uuid4().hex,
                                         series_instances=0,
                                         source_doi=analysis_collection_dois[row['SeriesInstanceUID']] \
                                             if row['SeriesInstanceUID'] in analysis_collection_dois
                                             else data_collection_doi,
                                         revised=True,
                                         done=False,
                                         is_new=True,
                                         expanded=False))
        study.expanded = True
        sess.commit()
    else:
        # Need to add code for the case that the object is not new
        # Do that when we create version 3
        errlogger.error("p%s: Add code to hanndle not-new case in expand_study, args.id")


def build_study(sess, args, study_index, version, collection, patient, study, data_collection_doi, analysis_collection_dois):
    if True:
        begin = time.time()
        # if not study.expanded:
        #     expand_study(sess, collection, patient, study, data_collection_doi, analysis_collection_dois)
        rootlogger.info("    p%s: Study %s, %s, %s series", args.id, study.study_instance_uid, study_index, len(study.seriess))
        for series in study.seriess:
            series_index = f'{study.seriess.index(series)+1} of {len(study.seriess)}'
            build_series(sess, args, series_index, version, collection, patient, study, series, data_collection_doi, analysis_collection_dois)
        # study.study_instances = sum([series.series_instances for series in study.seriess])
        # study.study_timestamp = min([series.series_timestamp for series in study.seriess])
        study.study_hash = get_merkle_hash([series.series_hash for series in study.seriess])
        study.done = True
        sess.commit()
        duration = str(timedelta(seconds=(time.time() - begin)))
        rootlogger.info("    p%s: Study %s, %s,  completed in %s", args.id, study.study_instance_uid, study_index, duration)
    else:
        rootlogger.info("    p%s: Study %s, %s, previously built", args.id, study.study_instance_uid, study_index)


def expand_patient(sess, collection, patient):
    studies = get_TCIA_studies_per_patient(collection.tcia_api_collection_id, patient.submitter_case_id)
    # If the patient is new, then all the studies are new
    if patient.is_new:
        for study in studies:
            patient.studies.append(Study(patient_id=patient.id,
                                         idc_version_number=patient.idc_version_number,
                                         study_timestamp = datetime(1970,1,1,0,0,0),
                                         study_instance_uid=study['StudyInstanceUID'],
                                         study_uuid=uuid4().hex,
                                         study_instances = 0,
                                         revised=True,
                                         done=False,
                                         is_new=True,
                                         expanded=False))
        patient.expanded = True
        sess.commit()
    else:
        # Need to add code for the case that the object is not new
        # Do that when we create version 3
        errlogger.error("p%s: Add code to hanndle not-new case in expand_patient", args.id)


def build_patient(sess, args, patient_index, data_collection_doi, analysis_collection_dois, version, collection, patient):
    if True:
        begin = time.time()
        # if not patient.expanded:
        #     expand_patient(sess, collection, patient)
        rootlogger.info("  p%s: Patient %s, %s, %s studies", args.id, patient.submitter_case_id, patient_index, len(patient.studies))
        for study in patient.studies:
            study_index = f'{patient.studies.index(study) + 1} of {len(patient.studies)}'
            build_study(sess, args, study_index, version, collection, patient, study, data_collection_doi, analysis_collection_dois)
        # patient.patient_timestamp = min([study.study_timestamp for study in patient.studies])
        patient.patient_hash = get_merkle_hash([study.study_hash for study in patient.studies])

        patient.done = True
        sess.commit()
        # if patient.patient_timestamp == datetime(1970, 1, 1, 0, 0):
        #     pass
        duration = str(timedelta(seconds=(time.time() - begin)))
        rootlogger.info("  p%s: Patient %s, %s, completed in %s", args.id, patient.submitter_case_id, patient_index, duration)
    else:
        rootlogger.info("  p%s: Patient %s, %s, previously built", args.id, patient.submitter_case_id, patient_index)


def expand_collection(sess, args, collection):
    pass



def worker(input, output, args, data_collection_doi, analysis_collection_dois):
    rootlogger.debug('p%s: Worker starting: args: %s', args.id, args)
    for more_args in iter(input.get, 'STOP'):
        with Session(sql_engine) as sess:
            for attempt in range(PATIENT_TRIES):
                try:
                    index, idc_version_number, tcia_api_collection_id, submitter_case_id = more_args
                    version = sess.query(Version).filter_by(idc_version_number=idc_version_number).one()
                    collection = next(collection for collection in version.collections if collection.tcia_api_collection_id==tcia_api_collection_id)
                    patient = next(patient for patient in collection.patients if patient.submitter_case_id==submitter_case_id)
                    rootlogger.debug("p%s: In worker, sess: %s, submitter_case_id: %s", args.id, sess, submitter_case_id)
                    build_patient(sess, args, index, data_collection_doi, analysis_collection_dois, version, collection, patient)
                    break
                except Exception as exc:
                    errlogger.error("Worker p%s, exception %s; reattempt %s on patient %s/%s", args.id, exc, attempt, tcia_api_collection_id, submitter_case_id)
                    sess.rollback()

            if attempt == PATIENT_TRIES:
                errlogger.error("p%s, Failed to process patient: %s", args.id, submitter_case_id)
                sess.rollback()
            output.put(submitter_case_id)


def build_collection(sess, args, collection_index, version, collection):
    # if not collection.done:
    # if collection.tcia_api_collection_id == 'RIDER Breast MRI': # Temporary code for development
    if True:
        begin = time.time()
        # args.prestaging_bucket = f"{args.prestaging_bucket_prefix}{collection.tcia_api_collection_id.lower().replace(' ','_').replace('-','_')}"
        # if not collection.expanded:
        #     expand_collection(sess, args, collection)
        rootlogger.info("Collection %s, %s, %s patients", collection.tcia_api_collection_id, collection_index, len(collection.patients))
        # Get the lists of data and analyis series in this patient
        # data_collection_doi = get_data_collection_doi(collection.tcia_api_collection_id)
        # pre_analysis_collection_dois = get_analysis_collection_dois(collection.tcia_api_collection_id)
        # analysis_collection_dois = {x['SeriesInstanceUID']: x['SourceDOI'] for x in pre_analysis_collection_dois}
        data_collection_doi = ""
        analysis_collection_dois = []
        if args.num_processes==0:
            if collection.tcia_api_collection_id == 'CBIS-DDSM':
                # for series in sorted_seriess:
                patient_ids = {}
                for patient in collection.patients:
                    patient_id = patient.submitter_case_id.split('_')[2]
                    if not patient_id in patient_ids:
                        patient_ids[patient_id] = uuid4().hex
                    patient.crdc_case_id = patient_ids[patient_id]
                sess.commit()
            else:
                for patient in collection.patients:
                    patient.crdc_case_id = uuid4().hex
                sess.commit()
        else:
            processes = []
            # Create queues
            task_queue = Queue()
            done_queue = Queue()

            # List of patients enqueued
            enqueued_patients = []

            # Start worker processes
            for process in range(args.num_processes):
                args.id = process+1
                processes.append(
                    Process(target=worker, args=(task_queue, done_queue, args, data_collection_doi, analysis_collection_dois )))
                processes[-1].start()

            # Enqueue each patient in the the task queue
            for patient in collection.patients:
                patient_index = f'{collection.patients.index(patient)+1} of {len(collection.patients)}'
                task_queue.put((patient_index, version.idc_version_number, collection.tcia_api_collection_id, patient.submitter_case_id))
                enqueued_patients.append(patient.submitter_case_id)

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

                # collection.collection_timestamp = min([patient.patient_timestamp for patient in collection.patients])
                collection.collection_hash = get_merkle_hash([patient.patient_hash for patient in collection.patients])
                # copy_prestaging_to_staging_bucket(args, collection)
                collection.done = True
                # ************ Temporary code during development********************
                # duration = str(timedelta(seconds=(time.time() - begin)))
                # rootlogger.info("Collection %s, %s, completed in %s", collection.tcia_api_collection_id, collection_index, duration)
                # raise
                # ************ End temporary code ********************
                sess.commit()
                duration = str(timedelta(seconds=(time.time() - begin)))
                rootlogger.info("Collection %s, %s, completed in %s", collection.tcia_api_collection_id, collection_index,
                                duration)

            except Empty as e:
                errlogger.error("Timeout in build_collection %s", collection.tcia_api_collection_id)
                for process in processes:
                    process.terminate()
                    process.join()
                sess.rollback()
                duration = str(timedelta(seconds=(time.time() - begin)))
                rootlogger.info("Collection %s, %s, NOT completed in %s", collection.tcia_api_collection_id, collection_index,
                                duration)

    else:
        rootlogger.info("Collection %s, %s, previously built", collection.tcia_api_collection_id, collection_index)


def expand_version(sess, args, version):
    # This code is special because version 2 is special in that it is partially
    # populated with the collections from version 1. Moreover, we know that each collection
    # that we add is a new collection...it was not in version 1.
    # For subsequent versions, we need to determine whether or not a collection is new.

    # If we are here, we are beginning work on this version.
    # GCS data for the collection being built is accumulated in the staging bucket,
    # args.staging bucket.

    ## Since we are starting, delete everything from the staging bucket.
    ## empty_bucket(args.staging_bucket)
    if version.idc_version_number == 2:
        # tcia_collection_ids = [collection['Collection'] for collection in get_TCIA_collections()]
        tcia_collection_ids = get_collection_values_and_counts()
        idc_collection_ids = [collection.tcia_api_collection_id for collection in version.collections]
        new_collections = []
        for tcia_collection_id in tcia_collection_ids:
            if not tcia_collection_id in idc_collection_ids:
                new_collections.append(Collection(version_id = version.id,
                                              idc_version_number = version.idc_version_number,
                                              collection_timestamp = datetime(1970,1,1,0,0,0),
                                              tcia_api_collection_id = tcia_collection_id,
                                              revised = True,
                                              done = False,
                                              is_new = True,
                                              expanded = False))
        sess.add_all(new_collections)
        version.expanded = True
        sess.commit()
        rootlogger.info("Expanded version %s", version.idc_version_number)
    else:
        # Need to add code to handle the normal case
        errlogger.error("extend_version code needs extension")


def build_version1(sess, args, version):
    # Session = sessionmaker(bind= sql_engine)
    # version = version_is_done(sess, args.vnext)
    # if not version.done:
    if True:
        begin = time.time()
        # if not version.expanded:
        #     expand_version(sess, args, version)
        rootlogger.info("Version %s; %s collections", version.idc_version_number, len(version.collections))
        for collection in version.collections:
            if True:
            # if collection.tcia_api_collection_id == 'TCGA-READ':
                collection_index = f'{version.collections.index(collection)+1} of {len(version.collections)}'
                build_collection(sess, args, collection_index, version, collection)
        # version.idc_version_timestamp = min([collection.collection_timestamp for collection in version.collections])
        version.version_hash = get_merkle_hash([collection.collection_hash for collection in version.collections])
        # copy_staging_bucket_to_final_bucket(args,version)
        if all([collection.done for collection in version.collections if not collection.tcia_api_collection_id in skips]):

            version.done = True
            sess.commit()
            duration = str(timedelta(seconds=(time.time() - begin)))
            rootlogger.info("Built version %s in %s", version.idc_version_number, duration)
        else:
            rootlogger.info("Not all collections are done. Rerun.")
    else:
        rootlogger.info("    version %s previously built", version.idc_version_number)


def build_version2(sess, args, version):
    # Session = sessionmaker(bind= sql_engine)
    # version = version_is_done(sess, args.vnext)
    # if not version.done:
    if True:
        begin = time.time()
        # if not version.expanded:
        #     expand_version(sess, args, version)
        rootlogger.info("Version %s; %s collections", version.idc_version_number, len(version.collections))
        for collection in version.collections:
            if True:
            # if collection.tcia_api_collection_id == 'TCGA-READ':
                collection_index = f'{version.collections.index(collection)+1} of {len(version.collections)}'
                build_collection(sess, args, collection_index, version, collection)
        # version.idc_version_timestamp = min([collection.collection_timestamp for collection in version.collections])
        version.version_hash = get_merkle_hash([collection.collection_hash for collection in version.collections])
        # copy_staging_bucket_to_final_bucket(args,version)
        if all([collection.done for collection in version.collections if not collection.tcia_api_collection_id in skips]):

            version.done = True
            sess.commit()
            duration = str(timedelta(seconds=(time.time() - begin)))
            rootlogger.info("Built version %s in %s", version.idc_version_number, duration)
        else:
            rootlogger.info("Not all collections are done. Rerun.")
    else:
        rootlogger.info("    version %s previously built", version.idc_version_number)


def gen_idc_case_ids(args):
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
        if args.version == 1:
            build_version1(sess, args, version)
        else:
            build_version2(sess, args, version)


if __name__ == '__main__':
    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/gen_merkle_log.log'.format(os.environ['PWD']))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/gen_merkle_err.log'.format(os.environ['PWD']))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=2, help='Next version to generate')
    parser.add_argument('--num_processes', default=0, help="Number of concurrent processes")
    args = parser.parse_args()

    # Directory to which to download files from TCIA/NBIA
    args.dicom = 'dicom'
    print("{}".format(args), file=sys.stdout)

    gen_idc_case_ids(args)
