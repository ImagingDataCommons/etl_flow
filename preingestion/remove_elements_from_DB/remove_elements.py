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

# Conditionally and hierarchically removes an instance, series, study, patient or collection from
# the idc_xxx hierarchy.
# A series is only removed if its source_url matches a specified URL. This enables removing data
# that, for example is from a particular analysis result.
# An element at a level is only removed if all its children have been removed.

import os
import io
import sys
import argparse
import csv
from idc.models import Base, IDC_Collection, IDC_Patient, IDC_Study, IDC_Series
from ingestion.utilities.utils import get_merkle_hash, list_skips
from utilities.logging_config import successlogger, errlogger, progresslogger
from python_settings import settings
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, update
from google.cloud import storage


def remove_instance(client, args, sess, series, instance):
    try:
        series.instances.remove(instance)
        sess.delete(instance)
        progresslogger.info('\t\t\t\tInstance %s deleted', instance.sop_instance_uid)
        return
    except StopIteration:
        # Instance no longer in series
        return


def remove_series(client, args, sess, study, series):
    try:
        if series.source_url == args.source_url:
            index = 0
            while index < len(series.instances):
                # for instance in series.instances:
                instance = series.instances[index]
                remove_instance(client, args, sess, series, instance)
                if instance in series.instances:
                    # We didn't remove the instance
                    index += 1
                # if index > 0 and len(series.instances) >= index and instance == series.instances[index]:
                #     index += 1
            # If the series is empty, remove it from study and delete it
            if len(series.instances) == 0:
                study.seriess.remove(series)
                sess.delete(series)
                progresslogger.info('\t\t\tSeries %s deleted', series.series_instance_uid)
            else:
                progresslogger.info('\t\t\tSeries %s retained', series.series_instance_uid)
        else:
            progresslogger.info('\t\t\tSeries %s skipped', series.series_instance_uid)
        return
    except StopIteration:
        # Series no longer in study
        return


# def remove_study(client, args, sess, patient, study):
#     try:
#         while study.seriess:
#             if study.seriess
#             remove_series(client, args, sess, study, study.seriess[0])
#             # Study is empty now. Remove it from patient and delete it
#         if not study.seriess:
#             patient.studies.remove(study)
#             sess.delete(study)
#         progresslogger.info('\t\tStudy %s', study.study_instance_uid)
#         return
#     except StopIteration:
#         # Study no longer in patient
#         return


def remove_study(client, args, sess, patient, study):
    try:
        index = 0
        while index < len(study.seriess):
            series = study.seriess[index]
            remove_series(client, args, sess, study, series)
            # if index > 0 and len(study.seriess) >= index and series == study.seriess[index]:
            if series in study.seriess:
                # We didn't remove the series
                index += 1
        # If the study is empty now, remove it from patient and delete it
        if len(study.seriess) == 0:
            patient.studies.remove(study)
            sess.delete(study)
            progresslogger.info('\t\tStudy %s deleted', study.study_instance_uid)
        else:
            progresslogger.info('\t\tStudy %s retained', study.study_instance_uid)
        return
    except StopIteration:
        # Study no longer in patient
        return


# def remove_patient(client, args, sess, collection, patient):
#     try:
#         while patient.studies:
#             remove_study(client,args, sess, patient, patient.studies[0])
#             # The patient is empty. Remove it from collection and delete it
#         if not patient.studies:
#            collection.patients.remove(patient)
#            sess.delete(patient)
#         progresslogger.info('\tPatient %s', patient.submitter_case_id)
#         return
#     except StopIteration:
#         # Patient no longer in collection
#         return


def remove_patient(client, args, sess, collection, patient):
    try:
        index = 0
        while index < len(patient.studies):
            study = patient.studies[index]
            remove_study(client,args, sess, patient, study)
            if study in patient.studies:
            # if index > 0 and len(patient.studies) >= index and study == patient.studies[index]:
                # We didn't remove the study
                index += 1
        # If the patient is empty, remove it from collection and delete it
        if len(patient.studies) == 0:
            collection.patients.remove(patient)
            sess.delete(patient)
            progresslogger.info('\tPatient %s deleted', patient.submitter_case_id)
        else:
            progresslogger.info('\tPatient %s retained', patient.submitter_case_id)

        return
    except StopIteration:
        # Patient no longer in collection
        return


def remove_collection(client, args, sess, collection):
    try:
        # Try to remove all patients in the collection
        # index = 0
        # while index < len(collection.patients):
        #     patient = collection.patients[index]
        #     remove_patient(client, args, sess, collection, patient)
        #     # If the patient was not removed, advance the index
        #     # if index > 0 and len(collection.patients) >= index and patient == collection.patients[index]:
        #     if patient in collection.patients:
        #         # We didn't remove the patient
        #         index += 1
        # If the collection is empty. Delete it
        patients = sess.query(IDC_Patient).distinct().join(IDC_Collection.patients).join(
            IDC_Patient.studies).join(IDC_Study.seriess).filter(IDC_Patient.collection_id == collection.collection_id). \
            filter(IDC_Series.source_url == args.source_url).all()
        for patient in patients:
            remove_patient(client, args, sess, collection, patient)

        if len(collection.patients) == 0:
            sess.delete(collection)
            progresslogger.info('Collection %s deleted', collection.collection_id)
        else:
            progresslogger.info('Collection %s retained', collection.collection_id)
        return
    except StopIteration:
        # Collection no longer in DB
        return
