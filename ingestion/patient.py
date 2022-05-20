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
from idc.models import Patient, Study
from ingestion.utilities.utils import accum_sources, get_merkle_hash, is_skipped
from ingestion.study import clone_study, build_study, retire_study
from python_settings import settings

successlogger = logging.getLogger('root.success')
progresslogger = logging.getLogger('root.progress')
errlogger = logging.getLogger('root.err')


def clone_patient(patient, uuid):
    new_patient = Patient(uuid=uuid)
    for key, value in patient.__dict__.items():
        if key not in ['_sa_instance_state', 'uuid', 'collections', 'studies']:
            setattr(new_patient, key, value)
    for study in patient.studies:
        new_patient.studies.append(study)
    return new_patient


def retire_patient(args, patient):
    # If this object has children from source, delete them
    progresslogger.debug  ('  p%s: Patient %s retiring', args.pid, patient.submitter_case_id)
    for study in patient.studies:
        retire_study(args, study)
    patient.final_idc_version = settings.PREVIOUS_VERSION


def expand_patient(sess, args, all_sources, version, collection, patient):
    skipped = is_skipped(args.skipped_collections, collection.collection_id)
    # Get the studies that the sources know about
    studies = all_sources.studies(patient, skipped)    # patient_ids = [patient['PatientId'] for patient in patients]

    if len(studies) != len(set(studies)):
        errlogger.error("\tp%s: Duplicate studies in expansion of patient %s", args.pid,
                        patient.submitter_case_id)
        raise RuntimeError("p%s: Duplicate studies expansion of collection %s", args.pid,
                           patient.submitter_case_i)

    if patient.is_new:
        # All patients are new by definition
        new_objects = studies
        retired_objects = []
        existing_objects = []
    else:
        # Get the IDs of the studies that we have.
        idc_objects = {object.study_instance_uid: object for object in patient.studies}
        # If any (non-skipped) source has an object but IDC does not, it is new. Note that we don't get objects from
        # skipped collections
        new_objects = sorted([id for id in studies \
                              if not id in idc_objects])
        # An object in IDC will continue to exist if any non-skipped source has the object or IDC's object has a
        # skipped source. I.E. if an object has a skipped source then, we can't ask the source about it so assume
        # it exists.
        existing_objects = [obj for id, obj in idc_objects.items() if \
                id in studies or any([a and b for a, b in zip(obj.sources,skipped)])]
        # An object in IDC is retired if it no longer exists in IDC
        retired_objects = [obj for id, obj in idc_objects.items() \
                      if not obj in existing_objects]

    for study in sorted(new_objects):
        new_study = Study()
        new_study.study_instance_uid=study
        new_study.uuid = str(uuid4())
        new_study.min_timestamp = datetime.utcnow()
        new_study.study_instances = 0
        new_study.revised = studies[study]
        new_study.hashes = None
        new_study.max_timestamp = new_study.min_timestamp
        new_study.init_idc_version=settings.CURRENT_VERSION
        new_study.rev_idc_version=settings.CURRENT_VERSION
        new_study.final_idc_version=0
        new_study.done = False
        new_study.is_new=True
        new_study.expanded=False
        patient.studies.append(new_study)
        progresslogger.debug  ('    p%s: Study %s is new',  args.pid, new_study.study_instance_uid)

    for study in existing_objects:
        idc_hashes = study.hashes

        # Get the hash from each source that is not skipped
        # The hash of a source is "" if the source is skipped, or the source that does not have
        # the object
        src_hashes = all_sources.src_study_hashes(collection.collection_id, study.study_instance_uid, skipped)
        # A source is revised the idc hashes[source] and the source hash differ and the source is not skipped
        revised = [(x != y) and not z for x, y, z in \
                   zip(idc_hashes[:-1], src_hashes, skipped)]
        # If any source is revised, then the object is revised.
        if any(revised):
            rev_study = clone_study(study, str(uuid4()))
            rev_study.revised = True
            rev_study.done = False
            rev_study.is_new = False
            rev_study.expanded = False
            rev_study.revised = revised
            rev_study.hashes = None
            rev_study.rev_idc_version = settings.CURRENT_VERSION
            patient.studies.append(rev_study)
            progresslogger.debug  ('    p%s: Study %s is revised',  args.pid, rev_study.study_instance_uid)

            # Mark the now previous version of this object as having been replaced
            # and drop it from the revised patient
            study.final_idc_version = settings.PREVIOUS_VERSION
            patient.studies.remove(study)
        else:
            # The study is unchanged. Just add it to the patient
            # Stamp this study showing when it was checked
            study.min_timestamp = datetime.utcnow()
            study.max_timestamp = datetime.utcnow()
            # Make sure the collection is marked as done and expanded
            # Shouldn't be needed if the previous version is done
            study.done = True
            study.expanded = True
            progresslogger.debug  ('    p%s: Study %s unchanged',  args.pid, study.study_instance_uid)

    for study in retired_objects:
        breakpoint()
        retire_study(args, study)
        patient.studies.remove(study)

    patient.expanded = True
    sess.commit()
    return

def build_patient(sess, args, all_sources, patient_index, data_collection_doi_url, analysis_collection_dois, version, collection, patient):
    try:
        begin = time.time()
        successlogger.debug("  p%s: Expand Patient %s, %s", args.pid, patient.submitter_case_id, patient_index)
        if not patient.expanded:
            expand_patient(sess, args, all_sources, version, collection, patient)
        successlogger.info("  p%s: Expanded Patient %s, %s, %s studies, expand_time: %s, %s", args.pid, patient.submitter_case_id, patient_index, len(patient.studies), time.time()-begin, time.asctime())
        for study in patient.studies:
            study_index = f'{patient.studies.index(study) + 1} of {len(patient.studies)}'
            if not study.done:
                build_study(sess, args, all_sources, study_index, version, collection, patient, study, data_collection_doi_url, analysis_collection_dois)
            else:
                successlogger.info("    p%s: Study %s, %s, previously built", args.pid, study.study_instance_uid, study_index)
        if all([study.done for study in patient.studies]):
            patient.max_timestamp = max([study.max_timestamp for study in patient.studies if study.max_timestamp != None])

             # Get a list of what DB thinks are the patient's hashes
            idc_hashes = all_sources.idc_patient_hashes(patient)
            skipped = is_skipped(args.skipped_collections, collection.collection_id)
            src_hashes = all_sources.src_patient_hashes(collection.collection_id, patient.submitter_case_id, skipped)
            revised = [(x != y) and  not z for x, y, z in \
                    zip(idc_hashes[:-1], src_hashes, skipped)]
            if any(revised):
                raise Exception('Hash match failed for patient %s', patient.submitter_case_id)
            else:
                patient.hashes = idc_hashes
                patient.sources = accum_sources(patient, patient.studies)

                patient.done = True
                sess.commit()
                duration = str(timedelta(seconds=(time.time() - begin)))
                successlogger.info("  p%s: Completed Patient %s, %s, in %s, %s", args.pid, patient.submitter_case_id, patient_index, duration, time.asctime())
    except Exception as exc:
        errlogger.info('  p%s build_patient failed: %s', args.pid, exc)
        raise exc
