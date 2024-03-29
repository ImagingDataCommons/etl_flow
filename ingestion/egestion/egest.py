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

# Remove a version, collection, etc. from the DB
# This script has never been used. It needs to be tested.
#
# The intent is that when called at some level, it will
# restore the object to its state to what it was when
# added to the corresponding table when expanding its
# parent.
# All descendants added when building the object are deleted.
# All descendants retired when building the object are unretired.
#
# To delete a version, after calling egest_version():
#   sess.delete(version)

import os
from idc.models import Collection, Patient, Study, Series, Instance
import logging
from python_settings import settings

successlogger = logging.getLogger('root.success')
errlogger = logging.getLogger('root.err')

def egest_series(sess, series):
    successlogger.info('        Deleting series %s', series.series_instance_uid)
    # instances = {instance.sop_instance_uid:instance for instance in series.instances }
    # for instance in series.instances:
    while series.instances:
        instance = series.instances
        # We first remove the instance from the series
        series.instances.remove(instance)
        # If the version of the instance was new in this version, delete it
        if series.rev_idc_version == instance.rev_idc_version :
            sess.delete(instance)
            successlogger.info('          Deleted instance %s', instance.sop_instance_uid)

            # if this is not a new instance, just a new version of an existing instance,
            # find the previous version and reset it's final_idc_version to 0 to
            # restore it to the "current" instance.
            if instance.init_idc_version != instance.rev_idc_version:
                prev_instance = sess.query(Instance).filter(
                    Instance.sop_instance_uid == instance.sop_idc_version and
                    Instance.final_idc_version == settings.PREVIOUS_VERSION).first()
                prev_instance.final_idc_version = 0
                series.instances.append(prev_instance)
        successlogger.info('        Removed instance %s from series %s', instance.sop_instance_uid,
                       series.series_instance_uid)
    series.expanded = False
    series.done = False
    series.sources = [False,False]
    series.hashes = None
    series.final_idc_version = 0


def egest_study(sess, study):
    successlogger.info('      Deleting study %s', study.study_instance_uid)
    # seriess = {series.series_instance_uid:series for series in study.studies }
    # for series in study.seriess:
    while study.seriess:
        series = study.seriess[0]
        study.seriess.remove(series)
        # If the version of the series was new in this version, delete it
        if study.rev_idc_version == series.rev_idc_version :
            egest_series(sess, series)
            sess.delete(series)
            successlogger.info('        Deleted series %s', series.series_instance_uid)

            # if this is not a new series, just a new version of an existing series,
            # find the previous version and reset it's final_idc_version to 0 to
            # restore it to the "current" series.
            if series.init_idc_version != series.rev_idc_version:
                prev_series = sess.query(Series).filter(
                    Series.series_instance_uid == series.series_instance_uid and
                    Series.final_idc_version == settings.PREVIOUS_VERSION).first()
                prev_series.final_idc_version = 0
                study.seriess.append(prev_series)
        successlogger.info('      Removed series %s from study %s', series.series_instance_uid, study.study_instance_uid)
    study.expanded = False
    study.done = False
    study.sources = [False,False]
    study.hashes = None
    study.final_idc_version = 0


def egest_patient(sess, patient):
    successlogger.info('    Deleting patient %s', patient.submitter_case_id)
    # studys = {study.study_instance_uid:study for study in patient.studies }
    # for study in patient.studies:
    while patient.studies:
        study = patient.studies[0]
        patient.studies.remove(study)
        # If the version of the study was new in this version, delete it
        if patient.rev_idc_version == study.rev_idc_version :
            egest_study(sess, study)
            sess.delete(study)
            successlogger.info('      Deleted study %s', study.study_instance_uid)

            # if this is not a new study, just a new version of an existing study,
            # find the previous version and reset it's final_idc_version to 0 to
            # restore it to the "current" study.
            if study.init_idc_version != study.rev_idc_version:
                prev_study = sess.query(Study).filter(
                    Study.study_instance_uid == study.study_instance_uid and
                    Study.final_idc_version == settings.PREVIOUS_VERSION).first()
                prev_study.final_idc_version = 0
                patient.studies.append(prev_study)
        successlogger.info('    Removed study %s from patient %s', study.study_instance_uid, patient.submitter_case_id)
    patient.expanded = False
    patient.done = False
    patient.sources = [False,False]
    patient.hashes = None
    patient.final_idc_version = 0


def egest_collection(sess, collection):
    successlogger.info('  Deleting collection %s', collection.collection_id)
    # patients = {patient.submitter_case_id:patient for patient in collection.patients }
    # for patient in collection.patients:
    while collection.patients:
        patient = collection.patients[0]
        collection.patients.remove(patient)
        # If the version of the patient was new in this version, delete it
        if collection.rev_idc_version == patient.rev_idc_version :
            egest_patient(sess, patient)
            sess.delete(patient)
            successlogger.info('    Deleted patient %s', patient.submitter_case_id)

            # if this is not a new patient, just a new version of an existing patient,
            # find the previous version and reset it's final_idc_version to 0 to
            # restore it to the "current" patient.
            if patient.init_idc_version != patient.rev_idc_version:
                prev_patient = sess.query(Patient).filter(
                    Patient.submitter_case_id == patient.submitter_case_id and
                    Patient.final_idc_version == settings.PREVIOUS_VERSION).first()
                prev_patient.final_idc_version = 0
                breakpoint()
                ### Why are we doing the following? Isn't prev_patient a child of some previous version of collection?
                collection.patients.append(prev_patient)
        successlogger.info('  Removed patient %s from collection %s', patient.submitter_case_id, collection.collection_id)
    collection.expanded = False
    collection.done = False
    collection.sources = [False,False]
    collection.hashes = None
    collection.final_idc_version = 0


def egest_version(sess, version):
    successlogger.info('Deleting version %s', version.version)
    # collections = {collection.collection_id:collection for collection in version.collections }
    # for collection in version.collections:
    while version.collections:
        collection = version.collections[0]
        version.collections.remove(collection)
        # If the version of the collection was new in this version, delete it
        if version.version == collection.rev_idc_version :
            egest_collection(sess, collection)
            sess.delete(collection)
            successlogger.info('  Deleted collection %s', collection.collection_id)

            # if this is not a new collection, just a new version of an existing collection,
            # find the previous version and reset it's final_idc_version to 0 to
            # restore it to the "current" collection.
            if collection.init_idc_version != collection.rev_idc_version:
                prev_collection = sess.query(Collection).filter(
                    Collection.collection_id == collection.collection_id and
                    Collection.final_idc_version == settings.PREVIOUS_VERSION).first()
                prev_collection.final_idc_version = 0
                breakpoint()
                ### Why are we doing the following? Isn't prev_collection a child of some previous version of version?
                ### Seems doing this will prevent deleting the version unless it can somehow cascade.
                version.collections.append(prev_collection)
        successlogger.info('  Removed collection %s from version %s', collection.collection_id, version.version)

    version.expanded = False
    version.done = False
    version.sources = [False,False]
    version.hashes = None
    version.final_idc_version = 0



