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
from utilities.logging_config import successlogger, progresslogger, errlogger
from uuid import uuid4
from idc.models import instance_source, Version, Collection, Patient
from ingestion.utilities.utils import accum_sources, empty_bucket, create_prestaging_bucket, is_skipped
from ingestion.patient import clone_patient, build_patient, retire_patient
from ingestion.all_sources import All_Sources
from utilities.sqlalchemy_helpers import sa_session
from python_settings import settings

from multiprocessing import Process, Queue, Lock, shared_memory
from queue import Empty

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
def worker(input, output, args, access, lock):
    with sa_session() as sess:
        all_sources = All_Sources(args.pid, sess, settings.CURRENT_VERSION, access,
                                  args.skipped_tcia_collections, args.skipped_idc_collections, lock)

        for more_args in iter(input.get, 'STOP'):
            for attempt in range(PATIENT_TRIES):
                time.sleep((2**attempt)-1)
                index, collection_id, submitter_case_id = more_args
                try:
                    version = sess.query(Version).filter(Version.version==settings.CURRENT_VERSION).one()
                    collection = next(collection for collection in version.collections if collection.collection_id ==collection_id)
                    patient = next(patient for patient in collection.patients if patient.submitter_case_id==submitter_case_id)
                    build_patient(sess, args, all_sources, index, version, collection, patient)
                    break
                except Exception as exc:
                    errlogger.error("p%s, exception %s; reattempt %s on patient %s/%s, %s; %s", args.pid, exc, attempt, collection.collection_id, patient.submitter_case_id, index, time.asctime())
                    sess.rollback()

            if attempt == PATIENT_TRIES:
                errlogger.error("p%s, Failed to process patient: %s", args.pid, patient.submitter_case_id)
                sess.rollback()


            output.put(patient.submitter_case_id)


def expand_collection(sess, args, all_sources, collection):
    # skipped is a vector of booleans, one for each source
    skipped = is_skipped(args.skipped_collections, collection.collection_id)

    # Get the patients that the sources know about
    # For each patient, returns a vector of booleans, one for each source.
    # A boolean is True if the hash of the corresponding source differs
    # from the hash of the current version of the patient
    # If the source is skipped, then the corresponding boolean will be False.

    # If the source hash is "", and the corresponding source hash in idc_objects
    # is not "", the collection has no longer in the source. Probably should
    # retire it from the source.
    patients = all_sources.patients(collection, skipped)

    # Since we are starting, delete everything from the prestaging bucket.
    if collection.revised.tcia:
        progresslogger.info("Emptying tcia prestaging buckets")
        create_prestaging_bucket(args, args.prestaging_tcia_bucket)
        empty_bucket(args.prestaging_tcia_bucket)
        # breakpoint() # Verify that the bucket is empty
    if collection.revised.idc:
        progresslogger.info("Emptying idc prestaging buckets")
        create_prestaging_bucket(args, args.prestaging_idc_bucket)
        empty_bucket(args.prestaging_idc_bucket)
        # breakpoint() # Verify that the bucket is empty

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

    for patient in sorted(new_objects):
        new_patient = Patient()

        new_patient.submitter_case_id = patient
        new_patient.idc_case_id = str(uuid4())
        new_patient.min_timestamp = datetime.utcnow()
        new_patient.revised = patients[patient]
        # The following line can probably be deleted because
        # a object's sources are computed hierarchically after
        # building all the children.
        new_patient.sources = patients[patient]
        new_patient.hashes = ("","","")
        new_patient.uuid = str(uuid4())
        new_patient.max_timestamp = new_patient.min_timestamp
        new_patient.init_idc_version=settings.CURRENT_VERSION
        new_patient.rev_idc_version=settings.CURRENT_VERSION
        new_patient.final_idc_version=0
        new_patient.done=False
        new_patient.is_new=True
        new_patient.expanded=False

        collection.patients.append(new_patient)
        progresslogger.info('  p%s: Patient %s is new',  args.pid, new_patient.submitter_case_id)

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
            rev_patient.hashes = ("","","")
            # The following line can probably be deleted because
            # a object's sources are computed hierarchically after
            # building all the children.
            rev_patient.sources = patients[patient.submitter_case_id]
            rev_patient.rev_idc_version = settings.CURRENT_VERSION
            collection.patients.append(rev_patient)
            progresslogger.info('  p%s: Patient %s is revised',  args.pid, rev_patient.submitter_case_id)

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
            progresslogger.info('  p%s: Patient %s unchanged',  args.pid, patient.submitter_case_id)

    for patient in retired_objects:
        # breakpoint()
        retire_patient(args, patient)
        collection.patients.remove(patient)
        progresslogger.info('  p%s: Patient %s is retired', args.pid, patient.submitter_case_id)

    new_patients = []
    revised_patients = []
    for patient in collection.patients:
        if not patient.done:
            if patient.init_idc_version == patient.rev_idc_version:
                new_patients.append(patient.submitter_case_id)
            else:
                revised_patients.append(patient.submitter_case_id)
    progresslogger.info(f'{len(new_patients)} new patients:')
    for patient_id in sorted(new_patients):
        progresslogger.info(patient_id)
    progresslogger.info(f'{len(revised_patients)} revised patients:')
    for patient_id in sorted(revised_patients):
        progresslogger.info(patient_id)
    progresslogger.info(f'{len(retired_objects)} retired patients:')
    for patient in retired_objects:
        progresslogger.info(patient.submitter_case_id)

    collection.expanded = True
    sess.commit()
    return


def build_collection(sess, args, all_sources, collection_index, version, collection):
    begin = time.time()
    successlogger.info("p%s: Expand Collection %s, %s", args.pid, collection.collection_id, collection_index)
    args.prestaging_tcia_bucket = f"{args.prestaging_tcia_bucket_prefix}{collection.collection_id.lower().replace(' ','_').replace('-','_')}"
    args.prestaging_idc_bucket = f"{args.prestaging_idc_bucket_prefix}{collection.collection_id.lower().replace(' ','_').replace('-','_')}"
    if not collection.expanded:
        successlogger.info("p%s: Expanding Collection %s, %s, %s patients", args.pid, collection.collection_id,
                           collection_index, len(collection.patients))
        expand_collection(sess, args, all_sources, collection)
        successlogger.info("p%s: Expanded Collection %s, %s, %s patients", args.pid, collection.collection_id, collection_index, len(collection.patients))

    if args.num_processes==0:
        patients = sorted(collection.patients, key=lambda patient: patient.done, reverse=True)
        for patient in patients:
            patient_index = f'{patients.index(patient) + 1} of {len(patients)}'
            if not patient.done:
                build_patient(sess, args, all_sources, patient_index, version, collection, patient)
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
        if collection.revised.tcia:
            num_processes = min(num_processes, 8)

        # Enqueue each patient in the the task queue
        # patients = sorted(collection.patients, key=lambda patient: patient.done, reverse=True)
        all_patients = [patient for patient in collection.patients]
        all_patients = sorted(all_patients, key=lambda patient: patient.submitter_case_id)
        patients = [patient for patient in collection.patients if patient.done==False]
        patients = sorted(patients, key=lambda patient: patient.submitter_case_id)
        # Start worker processes
        lock = Lock()
        for process in range(min(num_processes, len(patients))):
            args.pid = process+1
            processes.append(
                Process(target=worker, args=(task_queue, done_queue, args, args.access, lock )))
            processes[-1].start()

        args.pid = 0
        for patient in patients:
            patient_index = f'{all_patients.index(patient) + 1} of {len(all_patients)}'
            if not patient.done:
                task_queue.put((patient_index, collection.collection_id, patient.submitter_case_id))
                enqueued_patients.append(patient.submitter_case_id)
                successlogger.info("  p%s: Patient %s %s enqueued", args.pid, patient.submitter_case_id,
                            patient_index)
            else:
                successlogger.info("  p%s: Patient %s, %s, previously built", args.pid, patient.submitter_case_id,
                            patient_index)

        # Collect the results for each patient
        try:
            while not enqueued_patients == []:
                # Timeout if waiting too long
                results = done_queue.get(True)
                enqueued_patients.remove(results)
                successlogger.info("  p%s: Patient %s dequeued", args.pid, results)
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
            # Get IDCs vector of collection hashes from the DB
            idc_hashes = all_sources.idc_collection_hashes(collection)
            # Record the collection hashes
            collection.hashes = idc_hashes
            # The collection's sources vector is the OR of the sources vector of all its patients
            collection.sources = accum_sources(collection, collection.patients)

            skipped = is_skipped(args.skipped_collections, collection.collection_id)
            # Get the sources' vector of collection hashes
            src_hashes = all_sources.src_collection_hashes(collection.collection_id, skipped)
            # Compare hashes of unskipped sources
            revised = [(x != y) and not z for x, y, z in \
                       zip(idc_hashes[:-1], src_hashes, skipped)]
            if any(revised):
                # raise Exception('Hash match failed for collection %s', collection.collection_id)
                errlogger.error('Hash match failed for collection %s', collection.collection_id)
            else:
                # Record the collection hashes
                collection.hashes = idc_hashes
                # The collection's sources vector is the OR of the sources vector of all its patients
                collection.sources = accum_sources(collection, collection.patients)
                collection.done = True
                duration = str(timedelta(seconds=(time.time() - begin)))
                successlogger.info("Built Collection %s, %s, in %s", collection.collection_id, collection_index, duration)
                sess.commit()

        except Exception as exc:
            errlogger.error(f'Could not validate collection hash for { collection.collection_id}: {exc}')
            # Record our hashes but don't mark as done
            # Record the collection hashes
            collection.hashes = idc_hashes
            # The collection's sources vector is the OR of the sources vector of all its patients
            collection.sources = accum_sources(collection, collection.patients)

    else:
        duration = str(timedelta(seconds=(time.time() - begin)))
        successlogger.info("Collection %s, %s, not completed in %s", collection.collection_id, collection_index,
                        duration)

