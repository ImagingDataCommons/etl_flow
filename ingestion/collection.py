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
from ingestion.utilities.utils import accum_sources, empty_bucket, create_prestaging_bucket, is_skipped
from ingestion.patient import clone_patient, build_patient, retire_patient
from ingestion.all_sources import All
from ingestion.utilities.get_collection_dois_and_urls import get_data_collection_doi, get_analysis_collection_dois,\
    get_data_collection_url
from utilities.tcia_helpers import get_access_token

from python_settings import settings

from multiprocessing import Process, Queue, Lock, shared_memory
from queue import Empty

from sqlalchemy.orm import Session
from sqlalchemy_utils import register_composites
from sqlalchemy import create_engine

# rootlogger = logging.getLogger('root')
successlogger = logging.getLogger('root.success')
# debuglogger = logging.getLogger('root.prog')
progresslogger = logging.getLogger('root.progress')
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
    progresslogger.debug('p%s: Collection %s retiring', args.pid, collection.collection_id)
    for patient in collection.patients:
        retire_patient(args, patient)
    collection.final_idc_version = settings.PREVIOUS_VERSION


PATIENT_TRIES=5
def worker(input, output, args, data_collection_doi, analysis_collection_dois, access, lock):
    # rootlogger.debug('p%s: Worker starting: args: %s', args.pid, args)
    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{settings.CLOUD_DATABASE}'
    # sql_engine = create_engine(args.sql_uri)
    sql_engine = create_engine(sql_uri)
    with Session(sql_engine) as sess:
        all_sources = All(args.pid, sess, settings.CURRENT_VERSION, args.access,
                          args.skipped_tcia_collections, args.skipped_path_collections, lock)

        for more_args in iter(input.get, 'STOP'):
            for attempt in range(PATIENT_TRIES):
                time.sleep((2**attempt)-1)
                index, collection_id, submitter_case_id = more_args
                try:
                    version = sess.query(Version).filter(Version.version==settings.CURRENT_VERSION).one()
                    collection = next(collection for collection in version.collections if collection.collection_id ==collection_id)
                    patient = next(patient for patient in collection.patients if patient.submitter_case_id==submitter_case_id)
                    # rootlogger.debug("p%s: In worker, sess: %s, submitter_case_id: %s", args.pid, sess, submitter_case_id)
                    build_patient(sess, args, all_sources, index, data_collection_doi, analysis_collection_dois, version, collection, patient)
                    break
                except Exception as exc:
                    errlogger.error("p%s, exception %s; reattempt %s on patient %s/%s, %s; %s", args.pid, exc, attempt, collection.collection_id, patient.submitter_case_id, index, time.asctime())
                    sess.rollback()

            if attempt == PATIENT_TRIES:
                errlogger.error("p%s, Failed to process patient: %s", args.pid, patient.submitter_case_id)
                sess.rollback()
            output.put(patient.submitter_case_id)


def expand_collection(sess, args, all_sources, collection):
    skipped = is_skipped(args.skipped_collections, collection.collection_id)
    not_skipped = [not x for x in skipped]
    # Get the patients that the sources know about
    # Returned data includes a sources vector for each patient
    patients = all_sources.patients(collection, skipped)

    # Since we are starting, delete everything from the prestaging bucket.
    if collection.revised.tcia:
        progresslogger.info("Emptying tcia prestaging buckets")
        create_prestaging_bucket(args, args.prestaging_tcia_bucket)
        empty_bucket(args.prestaging_tcia_bucket)
    if collection.revised.path:
        progresslogger.info("Emptying path prestaging buckets")
        create_prestaging_bucket(args, args.prestaging_path_bucket)
        empty_bucket(args.prestaging_path_bucket)

    # Check for duplicates
    if len(patients) != len(set(patients)):
        errlogger.error("  p%s: Duplicate patients in expansion of collection %s", args.pid,
                        collection.collection_id)
        raise RuntimeError("p%s: Duplicate patients expansion of collection %s", args.pid,
                           collection.collection_id)
    if collection.is_new:
        # All patients are new by definition
        new_objects = patients
        retired_objects = []
        existing_objects = []
    else:
        # Get the IDs of the patients that we have.
        idc_objects = {object.submitter_case_id: object for object in collection.patients}

        # If any (non-skipped) source has an object but IDC does not, it is new. Note that we don't get objects from
        # skipped collections
        new_objects = sorted([id for id in patients \
                              if not id in idc_objects])
        # An object in IDC will continue to exist if any non-skipped source has the object or IDC's object has a
        # skipped source. I.E. if an object has a skipped source then, we can't ask the source about it so assume
        # it exists.
        existing_objects = [obj for id, obj in idc_objects.items() if \
                id in patients or any([a and b for a, b in zip(obj.sources,skipped)])]
        retired_objects = [obj for id, obj in idc_objects.items() \
                      if not obj in existing_objects]
        # new_objects = sorted([id for id in patients if id not in idc_objects])
        # retired_objects = sorted([idc_objects[id] for id in idc_objects if id not in patients], key=lambda patient: patient.submitter_case_id)
        # existing_objects = sorted([idc_objects[id] for id in idc_objects if id in patients], key=lambda patient: patient.submitter_case_id)

    for patient in sorted(new_objects):
        new_patient = Patient()

        new_patient.submitter_case_id = patient
        new_patient.idc_case_id = str(uuid4())
        new_patient.min_timestamp = datetime.utcnow()
        new_patient.revised = patients[patient]
        new_patient.sources = (False, False)
        new_patient.hashes = None
        new_patient.uuid = str(uuid4())
        new_patient.max_timestamp = new_patient.min_timestamp
        new_patient.init_idc_version=settings.CURRENT_VERSION
        new_patient.rev_idc_version=settings.CURRENT_VERSION
        new_patient.final_idc_version=0
        new_patient.done=False
        new_patient.is_new=True
        new_patient.expanded=False

        collection.patients.append(new_patient)
        progresslogger.debug('  p%s: Patient %s is new',  args.pid, new_patient.submitter_case_id)

    for patient in existing_objects:
        idc_hashes = patient.hashes
        # Get the hash from each source that is not skipped
        # The hash of a source is "" if the source is skipped, or the source that does not have
        # the object
        src_hashes = all_sources.src_patient_hashes(collection.collection_id, patient.submitter_case_id, skipped)
        # A source is revised the if idc hashes[source] and the source hash differ and the source is not skipped
        revised = [(x != y) and  not z for x, y, z in \
                zip(idc_hashes[:-1], src_hashes, skipped)]
        # If any source is revised, then the object is revised.
        if any(revised):
            # rootlogger.debug('p%s **Revising patient %s', args.pid, patient.submitter_case_id)
            # Mark when we started work on this patient
            # assert settings.CURRENT_VERSION == patients[patient.submitter_case_id]['rev_idc_version']
            rev_patient = clone_patient(patient, str(uuid4()))
            rev_patient.done = False
            rev_patient.is_new = False
            rev_patient.expanded = False
            rev_patient.revised = revised
            rev_patient.hashes = None
            rev_patient.sources = [False, False]
            rev_patient.rev_idc_version = settings.CURRENT_VERSION
            collection.patients.append(rev_patient)
            progresslogger.debug('  p%s: Patient %s is revised',  args.pid, rev_patient.submitter_case_id)

            # Mark the now previous version of this object as having been replaced
            # and drop it from the revised collection
            patient.final_idc_version = settings.PREVIOUS_VERSION
            collection.patients.remove(patient)

        else:
            # The patient is unchanged. Just add it to the collection
            # Stamp this series showing when it was checked
            patient.min_timestamp = datetime.utcnow()
            patient.max_timestamp = datetime.utcnow()
            # Make sure the collection is marked as done and expanded
            # Shouldn't be needed if the previous version is done

            patient.done = True
            patient.expanded = True
            progresslogger.debug('  p%s: Patient %s unchanged',  args.pid, patient.submitter_case_id)

    for patient in retired_objects:
        breakpoint()
        # rootlogger.debug('  p%s: Patient %s retiring', args.pid, patient.submitter_case_id)
        retire_patient(args, patient)
        collection.patients.remove(patient)


    collection.expanded = True
    sess.commit()
    return
    # rootlogger.debug("p%s: Expanded collection %s",args.pid, collection.collection_id)


def build_collection(sess, args, all_sources, collection_index, version, collection):
    begin = time.time()
    successlogger.debug("p%s: Expand Collection %s, %s", args.pid, collection.collection_id, collection_index)
    args.prestaging_tcia_bucket = f"{args.prestaging_tcia_bucket_prefix}{collection.collection_id.lower().replace(' ','_').replace('-','_')}"
    args.prestaging_path_bucket = f"{args.prestaging_path_bucket_prefix}{collection.collection_id.lower().replace(' ','_').replace('-','_')}"
    if not collection.expanded:
        expand_collection(sess, args, all_sources, collection)
    successlogger.info("p%s: Expanded Collection %s, %s, %s patients", args.pid, collection.collection_id, collection_index, len(collection.patients))
    # Get the lists of data and analyis series for this collection
    breakpoint() # Get URLs
    data_collection_doi = get_data_collection_doi(collection.collection_id, server=args.server)
    data_collection_url = get_data_collection_url(collection.collection_id, sess)
    if data_collection_doi=="" and data_collection_url=="":
        # # Reported as https://help.cancerimagingarchive.net/servicedesk/customer/portal/1/TH-49634
        # if collection.collection_id == 'StageII-Colorectal-CT':
        #         data_collection_doi = 'https://doi.org/10.7937/p5k5-tg43'
        # elif collection.collection_id == 'B-mode-and-CEUS-Liver':
        #     data_collection_doi = '10.7937/TCIA.2021.v4z7-tc39'
        # elif collection.collection_id == 'Pancreatic-CT-CBCT-SEG':
        #     data_collection_doi = '10.7937/TCIA.ESHQ-4D90'
        # elif collection.collection_id == 'CPTAC-LSCC':
        #     data_collection_doi = '10.7937/K9/TCIA.2018.6EMUB5L2'
        # #Reported as https://help.cancerimagingarchive.net/servicedesk/customer/portal/1/TH-49633
        # elif collection.collection_id == 'CPTAC-AML':
        #     data_collection_doi = '10.7937/tcia.2019.b6foe619'
        # elif collection.collection_id == 'CPTAC-BRCA':
        #     data_collection_doi = '10.7937/TCIA.CAEM-YS80'
        # elif collection.collection_id == 'CPTAC-COAD':
        #     data_collection_doi = '10.7937/TCIA.YZWQ-ZZ63'
        # elif collection.collection_id == 'CPTAC-OV':
        #     data_collection_doi = '10.7937/TCIA.ZS4A-JD58'
        #
        # # NBIA does not return DOIs of redacted collections.
        # elif collection.collection_id == 'CPTAC-GBM':
        #     data_collection_doi = '10.7937/K9/TCIA.2018.3RJE41Q1'
        # elif collection.collection_id == 'CPTAC-HNSCC':
        #     data_collection_doi = '10.7937/K9/TCIA.2018.UW45NH81'
        # elif collection.collection_id == 'TCGA-GBM':
        #     data_collection_doi = '10.7937/K9/TCIA.2016.RNYFUYE9'
        # elif collection.collection_id == 'TCGA-HNSC':
        #     data_collection_doi = '10.7937/K9/TCIA.2016.LXKQ47MS'
        # elif collection.collection_id == 'TCGA-LGG':
        #     data_collection_doi = '10.7937/K9/TCIA.2016.L4LTD3TK'
        #
        # # These are non-TCIA TCGA collections. There are no (yet) DOIs for these.
        # # If we ever revise them, we'll come here
        # elif collection.collection_id in [
        #     'TCGA-ACC',
        #     'TCGA-CHOL',
        #     'TCGA-DLBC',
        #     'TCGA-MESO',
        #     'TCGA-PAAD',
        #     'TCGA-PCPG',
        #     'TCGA-SKCM',
        #     'TCGA-TGCT',
        #     'TCGA-THYM',
        #     'TCGA-UCS',
        #     'TCGA-UVM']:
        #
        #     breakpoint()
        #     data_collection_doi = f'{collection.collection_id}-DOI'
        # # Shouldn't ever get here, because we won't update NLST
        # elif collection.collection_id == 'NLST':
        #     breakpoint()
        #     data_collection_doi = '10.7937/TCIA.hmq8-j677'
        # # If we get here, we're broken
        # else:
        errlogger.error('No DOI for collection %s', collection.collection_id)
        breakpoint()
        return
    data_collection_doi_url = {'doi': data_collection_doi, 'url': data_collection_url}

    # Get all the analysis results DOIs.
    pre_analysis_collection_dois = get_analysis_collection_dois(collection.collection_id, server=args.server)
    analysis_collection_dois = {x['SeriesInstanceUID']: x['SourceDOI'] for x in pre_analysis_collection_dois}

    if args.num_processes==0:
        # for series in sorted_seriess:
        patients = sorted(collection.patients, key=lambda patient: patient.done, reverse=True)
        for patient in patients:
            patient_index = f'{patients.index(patient) + 1} of {len(patients)}'
            if not patient.done:
                build_patient(sess, args, all_sources, patient_index, data_collection_doi_url, analysis_collection_dois, version, collection, patient)
            else:
                if True:
                    successlogger.info("  p0: Patient %s, %s, previously built", patient.submitter_case_id,
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
            args.pid = process+1
            processes.append(
                Process(target=worker, args=(task_queue, done_queue, args, data_collection_doi, analysis_collection_dois, args.access, lock )))
            processes[-1].start()

        # Enqueue each patient in the the task queue
        args.pid = 0
        patients = sorted(collection.patients, key=lambda patient: patient.done, reverse=True)
        for patient in patients:
            patient_index = f'{collection.patients.index(patient) + 1} of {len(collection.patients)}'
            if not patient.done:
                # task_queue.put((patient_index, version.idc_version_number, collection.collection_id, patient.submitter_case_id))
                task_queue.put((patient_index, collection.collection_id, patient.submitter_case_id))
                enqueued_patients.append(patient.submitter_case_id)
            else:
                if (collection.patients.index(patient) % 100 ) == 0:
                    successlogger.info("  p%s: Patient %s, %s, previously built", args.pid, patient.submitter_case_id,
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
            successlogger.info("Collection %s, %s, NOT completed in %s", collection.collection_id, collection_index,
                            duration)

    if all([patient.done for patient in collection.patients]):
        collection.max_timestamp = max([patient.max_timestamp for patient in collection.patients if patient.max_timestamp != None])
        try:
            # Get a list of what DB thinks are the collection's hashes
            idc_hashes = all_sources.idc_collection_hashes(collection)
            # # Get a list of what the sources think are the collection's hashes
            # src_hashes = all_sources.src_collection_hashes(collection.collection_id)
            # # They must be the same
            # if src_hashes != idc_hashes[:-1]:
            skipped = is_skipped(args.skipped_collections, collection.collection_id)

            # if collection.collection_id in args.skipped_collections:
            #     skipped = args.skipped_collections[collection.collection_id]
            # else:
            #     skipped = (False, False)
                # if this collection is excluded from a source, then ignore differing source and idc hashes in that source
            src_hashes = all_sources.src_collection_hashes(collection.collection_id, skipped)
            revised = [(x != y) and not z for x, y, z in \
                       zip(idc_hashes[:-1], src_hashes, skipped)]
            if any(revised):
                errlogger.error('Hash match failed for collection %s', collection.collection_id)
            else:
                collection.hashes = idc_hashes
                collection.sources = accum_sources(collection, collection.patients)
                collection.done = True
                sess.commit()
            duration = str(timedelta(seconds=(time.time() - begin)))
            successlogger.info("Completed Collection %s, %s, in %s", collection.collection_id, collection_index,
                            duration)
        except Exception as exc:
            errlogger.error('Could not validate collection hash for %s: %s', collection.collection_id, exc)

    else:
        duration = str(timedelta(seconds=(time.time() - begin)))
        successlogger.info("Collection %s, %s, not completed in %s", collection.collection_id, collection_index,
                        duration)

