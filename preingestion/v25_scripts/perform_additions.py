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
# Adds data to the idc_collection/_patient/_study/_series/_instance DB tables
# from a specified bucket.
#
# One or more (manifest_url, manifest_type) pairs are specified in args,
#  the manifest_url is relative to the GCS folder specified by args.subdir.
#  if a pair is like ("", manifest_type), then a manifest is generated from the bucket contents and applied
#  according to the manifest_type.
# Note: In the event that a ("", 'partial_deletion') pair is specified, the script will remove all instances
# found in the args.subdir folder.
##
# In the last case, how do we know whether the revision is 'complete' or 'partial'?
#
# The script walks the directory hierarchy from a specified subdirectory of the
# gcsfuse mount point

import settings
import json5
from idc.models import IDC_Collection, IDC_Patient, IDC_Study, IDC_Series, IDC_Instance
from utilities.logging_config import successlogger, errlogger, progresslogger
import pandas as pd

import time

from ingestion.utilities.utils import get_merkle_hash, streaming_md5_hasher

from idc.models import IDC_Instance
from sqlalchemy import select
from utilities.sqlalchemy_helpers import sa_session
from google.cloud import storage

from multiprocessing import Queue, Process
from queue import Empty
from sqlalchemy import select, text, Table, MetaData, Column, String


# def validate_additions(sess, additions):
#     ids = additions['SOPInstanceUID'].unique()
#     sop_instance_uids = sess.scalars(select(IDC_Instance).where(IDC_Instance.sop_instance_uid.in_(ids))).all()
#
#     if len(sop_instance_uids) != len(additions):
#         errlogger.error(f'Failed to add {len(additions) - len(sop_instance_uids)} instances')
#         return False
#     else:
#         return True

def validate_additions(sess, additions):
    sess.execute(text("DROP TABLE IF EXISTS temp_revision;"))
    metadata = MetaData()
    temp_table = Table(
        "temp_revision",
        metadata,
        Column("SOPInstanceUID", String),  # Match the data types of your DataFrame columns
        Column("instance_hash", String)
    )
    conn = sess.connection()
    metadata.create_all(bind=conn)
    sess.execute(temp_table.insert(), additions.to_dict(orient="records"))
    d = select(temp_table.c.SOPInstanceUID, temp_table.c.instance_hash).cte("d")
    stmt = (
        select(IDC_Instance)
        .join(d, d.c.SOPInstanceUID == IDC_Instance.sop_instance_uid)
        .where(d.c.instance_hash == IDC_Instance.hash)
    )
    results = sess.execute(stmt).all()
    if len(results) != len(additions):
        errlogger.error(f'Failed to revise {len(additions) - len(results)} instances')
        return False
    else:
        return True

def build_instance(args, series, instance_data):
    sop_instance_uid = instance_data["SOPInstanceUID"]
    ingestion_url = instance_data["full_gcs_url"]
    blob_name = ingestion_url.split('/',3)[-1]

    try:
        # Get the record of this instance if it exists
        instance = next(instance for instance in series.instances if instance.sop_instance_uid == sop_instance_uid)
        errlogger.error(f'{args.pid}\t\t\t\tInstance {blob_name} exists')
        exit(1)
    except StopIteration:
        try:
            instance = IDC_Instance()
            instance.sop_instance_uid = sop_instance_uid
            instance.excluded = False
            instance.redacted = False
            instance.mitigation = ""
            series.instances.append(instance)
            progresslogger.info(f'{args.pid}\t\t\t\tInstance {blob_name} added')
        except Exception as exc1:
            errlogger.error(f'Error creating new instance: {exc1}')
            raise
    except Exception as exc:
        raise

    instance.hash = instance_data['instance_hash']
    instance.size = instance_data["size"]
    instance.idc_version = args.version
    instance.ingestion_url = ingestion_url
    instance.source_file_url = instance_data['source_file_url']
    instance.source_file_hash = instance_data['source_file_hash']
    successlogger.info(sop_instance_uid)
    return

def build_series(args, study, series_data):
    # study_id is the  for all rows`
    series_id = series_data.iloc[0]['SeriesInstanceUID']
    try:
        series = next(series for series in study.seriess if series.series_instance_uid == series_id)
        progresslogger.info(f'{args.pid}\t\t\tSeries {series_id} exists')
    except StopIteration:
        try:
            series = IDC_Series()
            series.series_instance_uid = series_id
            series.excluded = False
            series.redacted = False
            series.source_doi = series_data.iloc[0]['source_doi']
            series.source_url = f"https://doi.org/{series_data.iloc[0]['source_doi'].lower()}"
            series.versioned_source_doi = series_data.iloc[0]['versioned_source_doi'].lower()
            series.analysis_result = args.analysis_result
            study.seriess.append(series)
            progresslogger.info(f'{args.pid}\t\t\tSeries {series_id} added')
        except Exception as exc1:
            errlogger.error(f'Error creating new series: {exc1}')
            raise
    except Exception as exc:
        raise

    series.ingestion_script = settings.BASE_NAME
    # At this point, each row in series data corresponds to an instance of the series
    for _,instance_data in series_data.iterrows():
        try:
            build_instance(args, series, instance_data)
        except Exception as esc:
            raise
    hashes = [instance.hash for instance in series.instances]
    series.hash = get_merkle_hash(hashes)
    return


def build_study(args, patient, study_data):
    # study_id is the second column and same for all rows`
    study_id = study_data.iloc[0]["StudyInstanceUID"]
    try:
        study = next(study for study in patient.studies if study.study_instance_uid == study_id)
        progresslogger.info(f'{args.pid}\t\tStudy {study_id} exists')
    except StopIteration:
        try:
            study = IDC_Study()
            study.study_instance_uid = study_id
            study.redacted = False
            patient.studies.append(study)
            progresslogger.info(f'{args.pid}\t\tStudy {study_id} added')
        except Exception as exc1:
            errlogger.error(f'Error creating new study: {exc1}')
            raise
    except Exception as exc:
        raise

    series_ids = sorted(study_data['SeriesInstanceUID'].unique())
    for series_id in series_ids:
        series_data = study_data[study_data["SeriesInstanceUID"] == series_id]
        try:
            build_series(args, study, series_data)
        except Exception as exc:
            raise
    hashes = [series.hash for series in study.seriess ]
    study.hash = get_merkle_hash(hashes)
    return


def build_patient(args, collection, patient_data):
    # patient_id is the first column and same for all rows`
    patient_id = patient_data.iloc[0]['PatientID']
    try:
        patient = next(patient for patient in collection.patients if patient.submitter_case_id == patient_id)
        progresslogger.info(f'{args.pid}\tPatient {patient_id} exists')
    except StopIteration:
        try:
            patient = IDC_Patient()
            patient.submitter_case_id = patient_id
            patient.redacted = False
            collection.patients.append(patient)
            progresslogger.info(f'{args.pid}\tPatient {patient_id} added')
        except Exception as exc1:
            errlogger.error(f'Error creating new patient: {exc1}')
            raise
    except Exception as exc:
        raise
    study_ids = sorted(patient_data["StudyInstanceUID"].unique())
    for study_id in study_ids:
        study_data = patient_data[patient_data["StudyInstanceUID"] == study_id]
        try:
            build_study(args, patient, study_data)
        except Exception as exc:
            raise
    hashes = [study.hash for study in patient.studies ]
    patient.hash = get_merkle_hash(hashes)
    return


PATIENT_TRIES=5
def worker(input, output, args, collection_id, source_doi, versioned_source_doi):
    with sa_session() as sess:
        client = storage.Client()
        bucket = client.bucket(args.src_bucket)
        # with sa_session() as sess:
        collection = sess.query(IDC_Collection).filter(IDC_Collection.collection_id == collection_id).first()
        for more_args in iter(input.get, 'STOP'):
            index, patient_data = more_args
            for attempt in range(PATIENT_TRIES):
                try:
                    progresslogger.info(f'Building patient {index}')
                    try:
                        build_patient(args, bucket, collection, patient_data, source_doi, versioned_source_doi)
                    except Exception as exc:
                        raise
                    output.put(patient_data.iloc[0]["PatientID"])
                    break
                except Exception as exc:
                    errlogger.error("p%s, exception %s; reattempt %s on patient %s/%s, %s; %s", args.pid, exc, attempt, collection.collection_id, patient_data.iloc[0]["PatientID"], index, time.asctime())
                    sess.rollback()
                time.sleep((2**attempt)-1)

            else:
                errlogger.error("p%s, Failed to process patient: %s", args.pid, patient_data.iloc[0]["PatientID"])
                sess.rollback()

def perform_additions(args, sess, additions):
    client = storage.Client()

    dones = open(successlogger.handlers[0].baseFilename).read().splitlines()

    done_data = pd.DataFrame(dones, columns=['SOPInstanceUID'])

    all_collection_names = sorted(additions['collection_name'].unique())
    undone_data = pd.merge(additions, done_data, how="left", on=['SOPInstanceUID'], indicator=True)
    undone_data = undone_data[undone_data['_merge'] == 'left_only']

    for collection_name in all_collection_names:
        # Create the collection if it is not yet in the DB
        collection = sess.query(IDC_Collection).filter(IDC_Collection.collection_id == collection_name).first()
        if not collection:
            # The collection is not currently in the DB, so add it
            collection = IDC_Collection()
            collection.collection_id = collection_name
            collection.redacted = False
            sess.add(collection)
            progresslogger.info(f'Collection {collection_name} added')
        else:
            progresslogger.info(f'Collection {collection_name} exists')


        collection_data = undone_data[undone_data['collection_name'] == collection_name]
        all_patient_ids = sorted(additions["PatientID"].unique())
        # All patients in the collection
        patient_in_collection_ids = sorted(collection_data['PatientID'].unique())

        args.pid = 0
        if args.processes == 0:
            # bucket = client.bucket(args.src_bucket)
            for patient_id in patient_in_collection_ids:
                # Data for this patient
                patient_data = collection_data[collection_data['PatientID'] == patient_id]
                patient_index = f'{all_patient_ids.index(patient_id) + 1} of {len(all_patient_ids)}'
                # build_patient(args, bucket, collection, patient_data, args.source_doi, args.versioned_source_doi)
                build_patient(args, collection, patient_data)
        else:
            processes = []
            # Create queues
            task_queue = Queue()
            done_queue = Queue()
            # List of patients enqueued
            enqueued_patients = []
            # Start worker processes
            for process in range(min(args.processes, len(patient_in_collection_ids))):
                args.pid = process+1
                processes.append(
                     Process(target=worker, args=(task_queue, done_queue, args, collection_name,
                                                 args.source_doi,
                                                 args.versioned_source_doi)))
                processes[-1].start()

            for patient_id in patient_in_collection_ids:
                # Data for this patient
                patient_data = collection_data[collection_data['PatientID'] == patient_id]
                patient_index = f'{all_patient_ids.index(patient_id) + 1} of {len(all_patient_ids)}'

                task_queue.put((patient_index, patient_data))
                enqueued_patients.append(patient_id)

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


            except Empty as e:
                errlogger.error("Timeout in build_collection %s", collection.collection_id)
                for process in processes:
                    process.terminate()
                    process.join()
                sess.rollback()
                successlogger.info("Collection %s, %s, NOT completed in %s", collection.collection_id)

        hashes = [patient.hash for patient in collection.patients]
        collection.hash= get_merkle_hash(hashes)

    return all_collection_names

