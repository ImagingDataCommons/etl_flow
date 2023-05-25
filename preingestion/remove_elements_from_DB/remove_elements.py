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

# Removes an instance, series, study, patient or collection from
# the idc_xxx hierarchy

import os
import io
import sys
import argparse
import csv
from idc.models import Base, IDC_Collection
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
        progresslogger.info('\t\t\t\tInstance %s', instance.sop_instance_uid)
        return
    except StopIteration:
        # Instance no longer in series
        return

def remove_series(client, args, sess, study, series):
    try:
        while series.instances:
            remove_instance(client, args, sess, series, series.instances[0])
            # Series is empty now. Remove it from study and delete it
        study.seriess.remove(series)
        sess.delete(series)
        progresslogger.info('\t\t\tSeries %s', series.series_instance_uid)
        return
    except StopIteration:
        # Series no longer in study
        return


def remove_study(client, args, sess, patient, study):
    try:
        while study.seriess:
            remove_series(client, args, sess, study, study.seriess[0])
            # Study is empty now. Remove it from patient and delete it
        patient.studies.remove(study)
        sess.delete(study)
        progresslogger.info('\t\tStudy %s', study.study_instance_uid)
        return
    except StopIteration:
        # Study no longer in patient
        return


def remove_patient(client, args, sess, collection, patient):
    try:
        while patient.studies:
            remove_study(client,args, sess, patient, patient.studies[0])
            # The patient is empty. Remove it from collection and delete it
        collection.patients.remove(patient)
        sess.delete(patient)
        progresslogger.info('\tPatient %s', patient.submitter_case_id)
        return
    except StopIteration:
        # Patient no longer in collection
        return


def remove_collection(client, args, sess, collection):
    try:
        while collection.patients:
            remove_patient(client, args, sess, collection, collection.patients[0])
            # Collection is empty. Delete it
        sess.delete(collection)
        progresslogger.info('Collection %s', collection.collection_id)
        return
    except StopIteration:
        # Collection no longer in DB
        return
