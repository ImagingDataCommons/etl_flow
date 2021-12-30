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
import logging

rootlogger = logging.getLogger('root')
errlogger = logging.getLogger('root.err')

def egest_series(sess, series):
    rootlogger.info('Deleting series %s', series.series_instance_uid)
    instances = {instance.sop_instance_uid:instance for instance in series.instances }
    for instance_id in instances:
        series.instances.remove(instances[instance_id])
        rootlogger.info('\tRemoved instance %s from series %s', instance_id, series.series_instance_uid)
        # If the version of the instance was new in this version, delete it
        if series.rev_idc_version == instances[instance_id].rev_idc_version :
            sess.delete(instances[instance_id])
    sess.delete(series)
    rootlogger.info('Deleted series %s', series.series_instance_uid)


def egest_study(sess, study):
    rootlogger.info('Deleting study %s', study.study_instance_uid)
    seriess = {series.series_instance_uid:series for series in study.studies }
    for series_id in seriess:
        study.studies.remove(seriess[series_id])
        rootlogger.info('\tRemoved series %s from study %s', series_id, study.study_instance_uid)
        # If the version of the series was new in this version, delete it
        if study.rev_idc_version == seriess[series_id].rev_idc_version :
            egest_series(sess, seriess[series_id])
    sess.delete(study)
    rootlogger.info('Deleted study %s', study.study_instance_uid)


def egest_patient(sess, patient):
    rootlogger.info('Deleting patient %s', patient.submitter_case_id)
    studys = {study.study_instance_uid:study for study in patient.studies }
    for study_id in studys:
        patient.studies.remove(studys[study_id])
        rootlogger.info('\tRemoved study %s from patient %s', study_id, patient.submitter_case_id)
        # If the version of the study was new in this version, delete it
        if patient.rev_idc_version == studys[study_id].rev_idc_version :
            egest_study(sess, studys[study_id])
    sess.delete(patient)
    rootlogger.info('Deleted patient %s', patient.submitter_case_id)


def egest_collection(sess, collection):
    rootlogger.info('Deleting collection %s', collection.collection_id)
    patients = {patient.submitter_case_id:patient for patient in collection.patients }
    for patient_id in patients:
        collection.patients.remove(patients[patient_id])
        rootlogger.info('\tRemoved patient %s from collection %s', patient_id, collection.collection_id)
        # If the version of the patient was new in this version, delete it
        if collection.rev_idc_version == patients[patient_id].rev_idc_version :
            egest_patient(sess, patients[patient_id])
    sess.delete(collection)
    rootlogger.info('Deleted collection %s', collection.collection_id)


def egest_version(sess, version):
    rootlogger.info('Deleting version %s', version.version)
    collections = {collection.collection_id:collection for collection in version.collections }
    for collection_id in collections:
        version.collections.remove(collections[collection_id])
        rootlogger.info('\tRemoved collection %s from version %s', collection_id, version.version)
        # If the version of the collection was new in this version, delete it
        if version.version == collections[collection_id].rev_idc_version :
            egest_collection(sess, collections[collection_id])
    sess.delete(version)
    rootlogger.info('Deleted version %s', version.version)


