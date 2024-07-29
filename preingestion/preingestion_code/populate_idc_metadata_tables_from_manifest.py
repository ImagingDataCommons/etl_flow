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

# Adds/replaces data to the idc_collection/_patient/_study/_series/_instance DB tables
# from a specified bucket.
#
# For this purpose, the bucket containing the instance blobs is gcsfuse mounted, and
# pydicom is then used to extract needed metadata.
#
# The script walks the directory hierarchy from a specified subdirectory of the
# gcsfuse mount point

from idc.models import IDC_Collection, IDC_Patient, IDC_Study, IDC_Series, IDC_Instance
from utilities.logging_config import successlogger, errlogger, progresslogger
from base64 import b64decode
from python_settings import settings
from preingestion.validation_code.validate_analysis_result import validate_analysis_result
from preingestion.validation_code.validate_original_collection import validate_original_collection
from ingestion.utilities.utils import md5_hasher

import time

from ingestion.utilities.utils import get_merkle_hash

from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from utilities.sqlalchemy_helpers import sa_session
from google.cloud import storage

from multiprocessing import Queue, Process
from queue import Empty

from subprocess import run

PATIENT_ID = 0
STUDY_INSTANCE_UID = 1
SERIES_INSTANCE_UID = 2
SOP_INSTANCE_UID = 3
GCS_URL = 4


def build_instance(args, bucket, series, instance_data):
    instance_id = instance_data[SOP_INSTANCE_UID]
    blob_name = instance_data[GCS_URL].split('/',3)[-1]
    gcs_url = instance_data[GCS_URL]
    try:
        # Get the record of this instance if it exists
        instance = next(instance for instance in series.instances if instance.sop_instance_uid == instance_id)
        progresslogger.info(f'\t\t\t\tInstance {blob_name} exists')
    except StopIteration:
        instance = IDC_Instance()
        instance.sop_instance_uid = instance_id
        series.instances.append(instance)
        progresslogger.info(f'\t\t\t\tInstance {blob_name} added')

    blob = bucket.blob(blob_name)
    blob.reload()
    try:
        hash = b64decode(blob.md5_hash).hex()
    except TypeError:
        # Can't get md5 hash for some blobs (maybe multipart copied/)
        # So try to compute it
        try:
            # Copy the blob to disk
            src = gcs_url
            dst = f'{args.tmp_directory}/{blob_name}'
            result = run(["gsutil", "-m", "-q", "cp", "-r", src, dst], check=True)

            hash = md5_hasher(f"{args.tmp_directory}/{blob_name}")
            result = run(['rm', dst])
            progresslogger.info(f'Computed md5 hash of {blob_name}')

        except Exception as exc:
            errlogger.error(f'Failed to get hash/sizeof {blob_name}')
            exit

    instance.size = blob.size
    instance.idc_version = args.version
    instance.gcs_url = f'{gcs_url}'
    instance.hash = hash
    instance.excluded = False
    successlogger.info(gcs_url)


def build_series(args, bucket, study, series_data):
    # study_id is the  for all rows`
    series_id = series_data[0][SERIES_INSTANCE_UID]
    try:
        series = next(series for series in study.seriess if series.series_instance_uid == series_id)
        progresslogger.info(f'\t\t\tSeries {series_id} exists')
    except StopIteration:
        series = IDC_Series()
        series.series_instance_uid = series_id
        series.third_party = args.third_party
        series.license_url =args.license['license_url']
        series.license_long_name =args.license['license_long_name']
        series.license_short_name =args.license['license_short_name']
        series.third_party = args.third_party
        study.seriess.append(series)
        progresslogger.info(f'\t\t\tSeries {series_id} added')
    # Always set/update the source_doi in case it has changed
    series.source_doi = args.source_doi.lower()
    series.source_url = args.source_url.lower()
    series.versioned_source_doi = args.verioned_source_doi.lower()
    series.excluded = False
    # At this point, each row in series data corresponds to an instance on the series
    for instance_data in series_data:
        build_instance(args, bucket, series, instance_data)
    hashes = [instance.hash for instance in series.instances]
    series.hash = get_merkle_hash(hashes)
    return


def build_study(args, bucket, patient, study_data):
    # study_id is the second column and same for all rows`
    study_id = study_data[0][STUDY_INSTANCE_UID]
    try:
        study = next(study for study in patient.studies if study.study_instance_uid == study_id)
        progresslogger.info(f'\t\tStudy {study_id} exists')
    except StopIteration:
        study = IDC_Study()
        study.study_instance_uid = study_id
        patient.studies.append(study)
        progresslogger.info(f'\t\tStudy {study_id} added')
    series_ids = set(row[2] for row in study_data)
    series_ids = list(series_ids)
    series_ids.sort()
    for series_id in series_ids:
        series_data = [row for row in study_data if series_id == row[SERIES_INSTANCE_UID]]
        build_series(args, bucket, study, series_data)
    hashes = [series.hash for series in study.seriess ]
    study.hash = get_merkle_hash(hashes)
    return


def build_patient(args, bucket, collection, patient_data):
    # patient_id is the first column and same for all rows`
    patient_id = patient_data[0][PATIENT_ID]
    try:
        patient = next(patient for patient in collection.patients if patient.submitter_case_id == patient_id)
        progresslogger.info(f'\tPatient {patient_id} exists')
    except StopIteration:
        patient = IDC_Patient()
        patient.submitter_case_id = patient_id
        collection.patients.append(patient)
        progresslogger.info(f'\tPatient {patient_id} added')
    study_ids = set(row[1] for row in patient_data)
    study_ids = list(study_ids)
    study_ids.sort()
    for study_id in study_ids:
        study_data = [row for row in patient_data if study_id == row[STUDY_INSTANCE_UID]]
        build_study(args, bucket, patient, study_data)
    hashes = [study.hash for study in patient.studies ]
    patient.hash = get_merkle_hash(hashes)
    return


PATIENT_TRIES=5
def worker(input, output, args, collection_id):
    client = storage.Client()
    bucket = client.bucket(args.src_bucket)
    with sa_session() as sess:
        collection = sess.query(IDC_Collection).filter(IDC_Collection.collection_id == collection_id).first()
        for more_args in iter(input.get, 'STOP'):
            index, patient_data = more_args
            for attempt in range(PATIENT_TRIES):
                try:
                    progresslogger.info(f'Building patient {index}')
                    build_patient(args, bucket, collection, patient_data)
                    sess.commit()
                    output.put(patient_data[0][PATIENT_ID])
                    break
                except Exception as exc:
                    errlogger.error("p%s, exception %s; reattempt %s on patient %s/%s, %s; %s", args.pid, exc, attempt, collection.collection_id, patient.submitter_case_id, index, time.asctime())
                    sess.rollback()
                time.sleep((2**attempt)-1)

            else:
                errlogger.error("p%s, Failed to process patient: %s", args.pid, patient.submitter_case_id)
                sess.rollback()

def build_collection(args, sess, collection_id):
    client = storage.Client()

    # Create the collection if it is not yet in the DB
    collection = sess.query(IDC_Collection).filter(IDC_Collection.collection_id == collection_id).first()
    if not collection:
        # The collection is not currently in the DB, so add it
        collection = IDC_Collection()
        collection.collection_id = collection_id
        sess.add(collection)
        progresslogger.info(f'Collection {collection_id} added')
    else:
        progresslogger.info(f'Collection {collection_id} exists')

    # Get a list of all the 'done' instance. These are presumed to be instances having the corresponding
    # source_doi and which have the current version
    dones = sess.query(IDC_Series, IDC_Instance.gcs_url).join(IDC_Instance.seriess). \
        filter(IDC_Series.source_doi == args.source_doi).filter(IDC_Instance.idc_version == args.version).all()
    dones = set([row['gcs_url'] for row in dones])

    # Read in the manifest and make a list of the (distinct) patient_ids
    data = [row.split(',') for row in open(args.metadata_table).read().splitlines()]
    all_patient_ids = set(str(row[0]) for row in data[1:])
    all_patient_ids = list(all_patient_ids)
    all_patient_ids.sort()
    undone_data = [row for row in data[1:] if row[GCS_URL] not in dones]
    patient_ids = set(str(row[0]) for row in undone_data[1:])
    patient_ids = list(patient_ids)
    patient_ids.sort()

    processes = []
    # Create queues
    task_queue = Queue()
    done_queue = Queue()
    # List of patients enqueued
    enqueued_patients = []
    # Start worker processes
    for process in range(min(args.processes, len(patient_ids))):
        args.pid = process+1
        processes.append(
            Process(target=worker, args=(task_queue, done_queue, args, collection_id)))
        processes[-1].start()

    args.pid = 0
    for patient_id in patient_ids:
        # Make a list of the metadata of patient_id
        # patient_data = [row for row in data if patient_id == row[PATIENT_ID] and not row[GCS_URL] in dones]
        patient_data = [row for row in undone_data if patient_id == row[PATIENT_ID]]
        patient_index = f'{all_patient_ids.index(patient_id) + 1} of {len(all_patient_ids)}'
        # Enqueue the patient data
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

        sess.commit()

    except Empty as e:
        errlogger.error("Timeout in build_collection %s", collection.collection_id)
        for process in processes:
            process.terminate()
            process.join()
        sess.rollback()
        successlogger.info("Collection %s, %s, NOT completed in %s", collection.collection_id)

    hashes = [patient.hash for patient in collection.patients]
    collection.hash= get_merkle_hash(hashes)
    return


def prebuild_from_manifest(args):
    client = storage.Client()
    src_bucket = storage.Bucket(client, args.src_bucket)

    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{settings.CLOUD_DATABASE}'
    # sql_engine = create_engine(sql_uri, echo=True,)
    # sql_engine = create_engine(sql_uri)

    sql_engine = create_engine('bigquery://idc-dev-etl/idc_v18_dev' '?'  'dry_run=true', echo=True)

    with Session(sql_engine) as sess:

        build_collection(args, sess, args.collection_id)
        sess.commit()

    if args.validate:
        if args.third_party:
            if validate_analysis_result(args) == -1:
                exit -1
        else:
            if validate_original_collection(args) == -1:
                exit -1


# if __name__ == '__main__':
#
#     parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
#     parser.add_argument('--version', default=settings.CURRENT_VERSION)
#     parser.add_argument('--src_bucket', default='dac-vhm-dst', help='Bucket containing WSI instances')
#     parser.add_argument('--metadata_table', default='./manifest.csv', help='csv table of study, series, SOPInstanceUID, filepath')
#     parser.add_argument('--collection_id', default='NLM_visible_human_project', help='idc_webapp_collection id of the collection or ID of analysis result to which instances belong.')
#     parser.add_argument('--source_doi', default='', help='Collection DOI')
#     parser.add_argument('--source_url', default='https://www.nlm.nih.gov/research/visible/visible_human.html',\
#                         help='Info page URL')
#     parser.add_argument('--license', default = {"license_url": 'https://www.nlm.nih.gov/databases/download/terms_and_conditions.html',\
#             "license_long_name": "National Library of Medicine Terms and Conditions; May 21, 2019", \
#             "license_short_name": "National Library of Medicine Terms and Conditions; May 21, 2019"})
#     parser.add_argument('--third_party', type=bool, default=False, help='True if from a third party analysis result')
#     parser.add_argument('--validate', type=bool, default=True, help='True if validation is to be performed')
#     parser.add_argument('--gen_hashes', type=bool, default=True, help='True if hashes are to be generated')
#
#     args = parser.parse_args()
#     print("{}".format(args), file=sys.stdout)
#     args.client=storage.Client()
#
#     prebuild(args)

