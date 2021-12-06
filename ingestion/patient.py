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
from ingestion.utils import accum_sources
from ingestion.study import clone_study, build_study

rootlogger = logging.getLogger('root')
errlogger = logging.getLogger('root.err')


def clone_patient(patient, uuid):
    new_patient = Patient(uuid=uuid)
    for key, value in patient.__dict__.items():
        if key not in ['_sa_instance_state', 'uuid', 'collections', 'studies']:
            setattr(new_patient, key, value)
    for study in patient.studies:
        new_patient.studies.append(study)
    return new_patient


def expand_patient(sess, args, all_sources, patient):
    # Get the studies that the sources know about
    studies = all_sources.studies(patient)    # patient_ids = [patient['PatientId'] for patient in patients]

    if len(studies) != len(set(studies)):
        errlogger.error("\tp%s: Duplicate studies in expansion of patient %s", args.id,
                        patient.submitter_case_id)
        raise RuntimeError("p%s: Duplicate studies expansion of collection %s", args.id,
                           patient.submitter_case_i)

    if patient.is_new:
        for study in sorted(studies):
            rev_study = Study()
            rev_study.study_instance_uid=study
            if args.build_mtm_db:
                rev_study.uuid = studies[study]['uuid']
                rev_study.min_timestamp = studies[study]['min_timestamp']
                rev_study.max_timestamp = studies[study]['max_timestamp']
                rev_study.study_instances = studies[study]['study_instances']
                rev_study.sources = studies[study]['sources']
                rev_study.hashes = studies[study]['hashes']
            else:
                rev_study.uuid = str(uuid4())
                rev_study.min_timestamp = datetime.utcnow()
                rev_study.study_instances = 0
                rev_study.sources = (False, False)
                rev_study.hashes = ("", "", "")
            rev_study.study_instances = 0
            rev_study.max_timestamp = rev_study.min_timestamp
            rev_study.init_idc_version=args.version
            rev_study.rev_idc_version=args.version
            rev_study.final_idc_version=0
            rev_study.revised=False
            rev_study.done = False
            rev_study.is_new=True
            rev_study.expanded=False
            patient.studies.append(rev_study)

    else:
        # Studies in the previous version of this patient
        idc_objects = {object.study_instance_uid: object for object in patient.studies}

        new_objects = sorted([id for id in studies if id not in idc_objects])
        retired_objects = sorted([idc_objects[id] for id in idc_objects if id not in studies], key=lambda study: study.study_instance_uid)
        existing_objects = sorted([idc_objects[id] for id in studies if id in idc_objects], key=lambda study: study.study_instance_uid)

        for study in retired_objects:
            rootlogger.info('Study %s:%s retiring', study.study_instance_uid, study.uuid)
            study.final_idc_version = args.previous_version
            # retire_study(sess, args, study, source)
            # if not any(study.sources):
            #     sess.delete(study)

        for study in existing_objects:
            if all_sources.study_was_updated(study):
                rootlogger.info('**Patient %s needs revision', patient.submitter_case_id)
                rev_study = clone_study(study, studies[study.study_instance_uid]['uuid'] if args.build_mtm_db else str(uuid4()))
                assert args.version == study[study.study_instance_uid]['rev_idc_version']
                rev_study.revised = True
                rev_study.done = False
                rev_study.is_new = False
                rev_study.expanded = False
                if args.build_mtm_db:
                    rev_study.min_timestamp = studies[study.study_instance_uid]['min_timestamp']
                    rev_study.max_timestamp = studies[study.study_instance_uid]['max_timestamp']
                    rev_study.sources = studies[study.study_instance_uid]['sources']
                    rev_study.hashes = studies[study.study_instance_uid]['hashes']
                    # rev_study.uuid = studies[study.study_instance_uid]['uuid']
                    rev_study.rev_idc_version = studies[study.study_instance_uid]['rev_idc_version']
                else:
                    # rev_study.uuid = str(uuid4())
                    rev_study.rev_idc_version = args.version
                patient.studies.append(rev_study)

                # Mark the now previous version of this object as having been replaced
                # and drop it from the revised patient
                study.final_idc_version = args.previous_version
                patient.studies.remove(study)
            else:
                # The study is unchanged. Just add it to the patient
                if not args.build_mtm_db:
                    # Stamp this study showing when it was checked
                    study.min_timestamp = datetime.utcnow()
                    study.max_timestamp = datetime.utcnow()
                    # Make sure the collection is marked as done and expanded
                    # Shouldn't be needed if the previous version is done
                    study.done = True
                    study.expanded = True
                rootlogger.debug('**Study %s unchanged', study.study_instance_uid)
                # patient.studies.append(study)

        for study in sorted(new_objects):
            rev_study = Study()
            rev_study.study_instance_uid=study
            if args.build_mtm_db:
                rev_study.uuid = studies[study]['uuid']
                rev_study.min_timestamp = studies[study]['min_timestamp']
                rev_study.max_timestamp = studies[study]['max_timestamp']
                rev_study.study_instances = studies[study]['study_instances']
                rev_study.sources = studies[study]['sources']
                rev_study.hashes = studies[study]['hashes']
            else:
                rev_study.uuid = str(uuid4())
                rev_study.min_timestamp = datetime.utcnow()
                rev_study.study_instances = 0
                rev_study.sources = (False, False)
                rev_study.hashes = ("", "", "")
                rev_study.max_timestamp = rev_study.min_timestamp
            rev_study.init_idc_version=args.version
            rev_study.rev_idc_version=args.version
            rev_study.final_idc_version=0
            rev_study.revised=False
            rev_study.done = False
            rev_study.is_new=True
            rev_study.expanded=False
            patient.studies.append(rev_study)

    patient.expanded = True
    sess.commit()
    # rootlogger.debug("  p%s: Expanded patient %s",args.id, patient.submitter_case_id)
    return

def build_patient(sess, args, all_sources, patient_index, data_collection_doi, analysis_collection_dois, version, collection, patient):
    begin = time.time()
    if not patient.expanded:
        expand_patient(sess, args, all_sources, patient)
    rootlogger.info("  p%s: Patient %s, %s, %s studies, expand_time: %s, %s", args.id, patient.submitter_case_id, patient_index, len(patient.studies), time.time()-begin, time.asctime())
    for study in patient.studies:
        study_index = f'{patient.studies.index(study) + 1} of {len(patient.studies)}'
        if not study.done:
            build_study(sess, args, all_sources, study_index, version, collection, patient, study, data_collection_doi, analysis_collection_dois)
        else:
            rootlogger.info("    p%s: Study %s, %s, previously built", args.id, study.study_instance_uid, study_index)
    if all([study.done for study in patient.studies]):
        # patient.min_timestamp = min([study.min_timestamp for study in patient.studies if study.min_timestamp != None])
        patient.max_timestamp = max([study.max_timestamp for study in patient.studies if study.max_timestamp != None])

        if args.build_mtm_db:
            patient.done = True
            sess.commit()
            duration = str(timedelta(seconds=(time.time() - begin)))
            rootlogger.info("  p%s: Patient %s, %s, completed in %s, %s", args.id, patient.submitter_case_id,
                            patient_index, duration, time.asctime())
        else:
            # Get hash of children
            hashes = all_sources.idc_patient_hash(patient)
            if all_sources.src_patient_hash(collection.collection_id, patient.submitter_case_id) != hashes:
                # errlogger.error('Hash match failed for patient %s', patient.submitter_case_id)
                raise Exception('Hash match failed for patient %s', patient.submitter_case_id)
            else:
                # Test whether anything has changed
                if hashes != patient.hashes:
                    patient.hashes = hashes
                    patient.sources = accum_sources(patient, patient.studies)
                    patient.rev_idc_version = args.version
                    if not patient.is_new:
                        patient.revised = True
                else:
                    rootlogger.info("  p%s: Patient %s, %s, unchanged", args.id, patient.submitter_case_id, patient_index)

                patient.done = True
                sess.commit()
                duration = str(timedelta(seconds=(time.time() - begin)))
                rootlogger.info("  p%s: Patient %s, %s, completed in %s, %s", args.id, patient.submitter_case_id, patient_index, duration, time.asctime())
