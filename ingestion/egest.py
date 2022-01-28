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
import os
from idc.models import Collection, Patient, Study, Series, Instance
import logging

rootlogger = logging.getLogger('root')
errlogger = logging.getLogger('root.err')

def egest_instance(sess, args, instance):
    rootlogger.info('\t\t\t\t\tDeleting instance %s', instance.sop_instance_uid)
    sess.delete(instance)
    rootlogger.info('\t\t\t\t\tDeleted instance %s', instance.sop_instance_uid)

def egest_series(sess, args, series):
    rootlogger.info('\t\t\t\tDeleting series %s', series.series_instance_uid)
    # instances = {instance.sop_instance_uid:instance for instance in series.instances }
    for instance in series.instances:
        # We first remove the instance from the series
        series.instances.remove(instance)
        rootlogger.info('\t\t\t\tRemoved instance %s from series %s', instance.sop_instance_uid, series.series_instance_uid)
        # If the version of the instance was new in this version, delete it
        if series.rev_idc_version == instance.rev_idc_version :
            egest_instance(sess, args, instance)
            # if this is not a new instance, just a new version of an existing instance,
            # find the previous version and reset it's final_idc_version to 0 to
            # restore it to the "current" instance.
            if instance.init_idc_version != instance.rev_idc_version:
                prev_instance = sess.query(Instance).filter(
                    Instance.sop_instance_uid == instance.sop_idc_version and
                    Instance.final_idc_version == args.previous_version).first()
                prev_instance.final_idc_version = 0
                series.instances.append(prev_instance)

    sess.delete(series)
    rootlogger.info('\t\t\t\tDeleted series %s', series.series_instance_uid)


def egest_study(sess, args, study):
    rootlogger.info('\t\t\tDeleting study %s', study.study_instance_uid)
    # seriess = {series.series_instance_uid:series for series in study.studies }
    for series in study.seriess:
        study.seriess.remove(series)
        rootlogger.info('\t\t\tRemoved series %s from study %s', series.series_instance_uid, study.study_instance_uid)
        # If the version of the series was new in this version, delete it
        if study.rev_idc_version == series.rev_idc_version :
            egest_series(sess, args, series)
            # if this is not a new series, just a new version of an existing series,
            # find the previous version and reset it's final_idc_version to 0 to
            # restore it to the "current" series.
            if series.init_idc_version != series.rev_idc_version:
                prev_series = sess.query(Series).filter(
                    Series.series_instance_uid == series.series_instance_uid and
                    Series.final_idc_version == args.previous_version).first()
                prev_series.final_idc_version = 0
                study.seriess.append(prev_series)

    sess.delete(study)
    rootlogger.info('\t\t\tDeleted study %s', study.study_instance_uid)


def egest_patient(sess, args, patient):
    rootlogger.info('\t\tDeleting patient %s', patient.submitter_case_id)
    # studys = {study.study_instance_uid:study for study in patient.studies }
    for study in patient.studies:
        patient.studies.remove(study)
        rootlogger.info('\t\tRemoved study %s from patient %s', study.study_instance_uid, patient.submitter_case_id)
        # If the version of the study was new in this version, delete it
        if patient.rev_idc_version == study.rev_idc_version :
            egest_study(sess, args, study)
            # if this is not a new study, just a new version of an existing study,
            # find the previous version and reset it's final_idc_version to 0 to
            # restore it to the "current" study.
            if study.init_idc_version != study.rev_idc_version:
                prev_study = sess.query(Study).filter(
                    Study.study_instance_uid == study.study_instance_uid and
                    Study.final_idc_version == args.previous_version).first()
                prev_study.final_idc_version = 0
                patient.studies.append(prev_study)

    sess.delete(patient)
    rootlogger.info('\t\tDeleted patient %s', patient.submitter_case_id)


def egest_collection(sess, args, collection):
    rootlogger.info('\tDeleting collection %s', collection.collection_id)
    # patients = {patient.submitter_case_id:patient for patient in collection.patients }
    for patient in collection.patients:
        collection.patients.remove(patient)
        rootlogger.info('\tRemoved patient %s from collection %s', patient.submitter_case_id, collection.collection_id)
        # If the version of the patient was new in this version, delete it
        if collection.rev_idc_version == patient.rev_idc_version :
            egest_patient(sess, args, patient)

            # if this is not a new patient, just a new version of an existing patient,
            # find the previous version and reset it's final_idc_version to 0 to
            # restore it to the "current" patient.
            if patient.init_idc_version != patient.rev_idc_version:
                prev_patient = sess.query(Patient).filter(
                    Patient.submitter_case_id == patient.submitter_case_id and
                    Patient.final_idc_version == args.previous_version).first()
                prev_patient.final_idc_version = 0
                collection.patients.append(prev_patient)

    sess.delete(collection)
    rootlogger.info('\tDeleted collection %s', collection.collection_id)


def egest_version(sess, args, version):
    rootlogger.info('Deleting version %s', version.version)
    # collections = {collection.collection_id:collection for collection in version.collections }
    for collection in version.collections:
        version.collections.remove(collection)
        rootlogger.info('\tRemoved collection %s from version %s', collection.collection_id, version.version)
        # If the version of the collection was new in this version, delete it
        if version.version == collection.rev_idc_version :
            egest_collection(sess, args, collection)

            # if this is not a new collection, just a new version of an existing collection,
            # find the previous version and reset it's final_idc_version to 0 to
            # restore it to the "current" collection.
            if collection.init_idc_version != collection.rev_idc_version:
                prev_collection = sess.query(Collection).filter(
                    Collection.collection_id == collection.collection_id and
                    Collection.final_idc_version == args.previous_version).first()
                prev_collection.final_idc_version = 0
                version.collections.append(prev_collection)

    sess.delete(version)
    rootlogger.info('Deleted version %s', version.version)


