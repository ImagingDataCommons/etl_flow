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
import settings
# Adds/replaces data to the idc_collection/_patient/_study/_series/_instance DB tables
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

from idc.models import IDC_Collection, IDC_Patient, IDC_Study, IDC_Series, IDC_Instance
from utilities.logging_config import successlogger, errlogger, progresslogger
from base64 import b64decode
import pandas as pd
from preingestion.validation_code.validate_analysis_result import validate_analysis_result
from preingestion.validation_code.validate_original_collection import validate_original_collection
from preingestion.preingestion_code.gen_hashes_sql import gen_hashes
from preingestion.preingestion_code.gen_manifest_from_dicom_metadata import build_manifest
from preingestion.preingestion_code.remove_source_doi_elements import remove_collections

import time

from ingestion.utilities.utils import get_merkle_hash

from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from utilities.sqlalchemy_helpers import sa_session
from google.cloud import storage

from multiprocessing import Queue, Process
from queue import Empty

from subprocess import run

def build_instance(args, bucket, series, instance_data):
    instance_id = instance_data["SOPInstanceUID"]
    url = instance_data["ingestion_url"]
    if url.startswith('gs://'):
        # A full GCS URL
        blob_name = url.split('/',3)[-1]
    else:
        # url is relative to the bucket.
        if url.startswith('./'):
            blob_name = url.split('/',1)[-1]
        # if args.subdir:
        #     # If relative to a subdirectory of the bucket, add it
        #     blob_name = f'{args.subdir}/{url}'
        else:
            blob_name = f'{url}'
        if args.subdir:
            blob_name = f'{args.subdir}/{blob_name}'

    ingestion_url = f'gs://{bucket.name}/{blob_name}'
    try:
        # Get the record of this instance if it exists
        instance = next(instance for instance in series.instances if instance.sop_instance_uid == instance_id)
        progresslogger.info(f'{args.pid}\t\t\t\tInstance {blob_name} exists')
    except StopIteration:
        try:
            instance = IDC_Instance()
            instance.sop_instance_uid = instance_id
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

    blob = bucket.blob(blob_name)
    blob.reload()
    try:
        instance.hash = b64decode(blob.md5_hash).hex()
    except TypeError:
        # Can't get md5 hash for some blobs (maybe multipart copied/)
        # So try to compute it
        try:
            # Copy the blob to disk
            # if args.subdir:
            #     ingestion_url = f"gs://{bucket.name}/{args.subdir}/{blob_name}"
            # else:
            #     ingestion_url = f"gs://{bucket.name}/{blob_name}"

            src = ingestion_url

            dst = f'{args.tmp_directory}/{blob_name}'
            result = run(["gsutil", "-m", "-q", "cp", "-r", src, dst], check=True)

            instance.hash = md5_hasher(f"{args.tmp_directory}/{blob_name}")
            result = run(['rm', dst])
            progresslogger.info(f'Computed md5 hash of {blob_name}')

        except Exception as exc:
            errlogger.error(f'Failed to get hash/sizeof {blob_name}')
            exit

    instance.size = blob.size
    instance.idc_version = args.version
    instance.ingestion_url = ingestion_url
    successlogger.info(instance_id)


def build_series(args, bucket, study, series_data, source_doi, versioned_source_doi):
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
            study.seriess.append(series)
            progresslogger.info(f'{args.pid}\t\t\tSeries {series_id} added')
        except Exception as exc1:
            errlogger.error(f'Error creating new series: {exc1}')
            raise
    except Exception as exc:
        raise
    # Always set/update the source_doi in case it has changed
    series.license_url = args.license['license_url']
    series.license_long_name = args.license['license_long_name']
    series.license_short_name = args.license['license_short_name']
    series.analysis_result = args.analysis_result
    series.source_doi = source_doi.lower()
    series.source_url = f'https://doi.org/{source_doi.lower()}'
    series.versioned_source_doi = versioned_source_doi.lower()
    series.ingestion_script = settings.BASE_NAME
    # At this point, each row in series data corresponds to an instance of the series
    for _,instance_data in series_data.iterrows():
        try:
            build_instance(args, bucket, series, instance_data)
        except Exception as esc:
            raise
    hashes = [instance.hash for instance in series.instances]
    series.hash = get_merkle_hash(hashes)
    return


def build_study(args, bucket, patient, study_data, source_doi, versioned_source_doi):
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
            build_series(args, bucket, study, series_data, source_doi, versioned_source_doi)
        except Exception as exc:
            raise
    hashes = [series.hash for series in study.seriess ]
    study.hash = get_merkle_hash(hashes)
    return


def build_patient(args, bucket, collection, patient_data, source_doi, versioned_source_doi):
    # patient_id is the first column and same for all rows`
    patient_id = patient_data.iloc[0]['patientID']
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
            build_study(args, bucket, patient, study_data, source_doi, versioned_source_doi)
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
                    sess.commit()
                    output.put(patient_data.iloc[0]["patientID"])
                    break
                except Exception as exc:
                    errlogger.error("p%s, exception %s; reattempt %s on patient %s/%s, %s; %s", args.pid, exc, attempt, collection.collection_id, patient_data.iloc[0]["patientID"], index, time.asctime())
                    sess.rollback()
                time.sleep((2**attempt)-1)

            else:
                errlogger.error("p%s, Failed to process patient: %s", args.pid, patient_data.iloc[0]["patientID"])
                sess.rollback()

def build_collections(args, sess, manifest_data, sep=','):
    client = storage.Client()

    dones = open(successlogger.handlers[0].baseFilename).read().splitlines()

    # Rename columns in case they are misnamed:
    manifest_data = manifest_data.rename( columns = {
        "Filename": "ingestion_url",
        "Patient ID": "patientID",
        "Study Instance UID": "StudyInstanceUID",
        "Series Instance UID": "SeriesInstanceUID",
        "SOP Instance UID": "SOPInstanceUID"
        }
    )
    # Remove whitespace
    manifest_data = manifest_data.applymap(lambda x: x.strip())

    done_data = pd.DataFrame(dones, columns=['SOPInstanceUID'])
    if 'collection_id' in args and args.collection_id:
        all_collection_ids = [args.collection_id]
    else:
        all_collection_ids = sorted(manifest_data['collection_id'].unique())
    undone_data = pd.merge(manifest_data, done_data, how="left", on=['SOPInstanceUID'], indicator=True)
    undone_data = undone_data[undone_data['_merge'] == 'left_only']

    if 'collection_id' in args and args.collection_id:
        collection_ids = [args.collection_id]
        # Add/replace the collection_id column
        undone_data['collection_id'] = args.collection_id
    else:
        collection_ids = sorted(undone_data['collection_id'].unique())
    # collection_ids = sorted(undone_data['collection_id'].unique())
    for collection_id in collection_ids:
        # Create the collection if it is not yet in the DB
        collection = sess.query(IDC_Collection).filter(IDC_Collection.collection_id == collection_id).first()
        if not collection:
            # The collection is not currently in the DB, so add it
            collection = IDC_Collection()
            collection.collection_id = collection_id
            collection.redacted = False
            sess.add(collection)
            sess.commit()
            progresslogger.info(f'Collection {collection_id} added')
        else:
            progresslogger.info(f'Collection {collection_id} exists')


        # if 'collection_id' in args and args.collection_id:
        #     # If there is a single collection then all patients are in that collection
        #     # A df of data for just this collection
        #     collection_data = undone_data
        # else:
        #     # A df of data for just this collection
        #     collection_data = undone_data[undone_data['collection_id'] == collection_id]
        collection_data = undone_data[undone_data['collection_id'] == collection_id]
        all_patient_ids = sorted(manifest_data["patientID"].unique())
        # All patients in the collection
        patient_in_collection_ids = sorted(collection_data['patientID'].unique())

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
                Process(target=worker, args=(task_queue, done_queue, args, collection_id,
                        args.source_doi, args.versioned_source_doi)))
                        # source_dois[collection_id] if source_dois is not None else args.source_doi,
                        # versioned_source_dois[collection_id] if versioned_source_dois is not None else args.versioned_source_doi)))
            processes[-1].start()

        args.pid = 0
        for patient_id in patient_in_collection_ids:
            # Data for this patient
            patient_data = collection_data[collection_data['patientID'] == patient_id]
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

    return all_collection_ids

def perform_partial_deletion(sess, args, manifest_url, sep):
    pass

def perform_partial_revision(sess, args, manifest_url, sep):
    # Read the manifest into a data frame
    if manifest_url:
        try:
            if args.subdir:
                manifest_data = pd.read_csv(f"gs://{args.src_bucket}/{args.subdir}/{manifest_url}", sep=sep, header=0)
            else:
                manifest_data = pd.read_csv(f"gs://{args.src_bucket}/{manifest_url}", sep=sep, header=0)
        except Exception as exc:
            errlogger.error(f'Failed to read manifest: {exc}')
            exit(-1)
    else:
        try:
            # If no manifest is provided, first see if we've already genetated one
            if args.subdir:
                manifest_data = pd.read_csv(f"gs://{args.src_bucket}/{args.subdir}/generated_partial_revision.csv", sep=sep,
                                            header=0)
            else:
                manifest_data = pd.read_csv(f"gs://{args.src_bucket}/generated_partial_revision.csv", sep=sep, header=0)

        except Exception as exc:
            manifest_data = build_manifest(args)
            # Save the manifest to the bucket in case we need to rerun
            if args.subdir:
                manifest_data.to_csv(f"gs://{args.src_bucket}/{args.subdir}/generated_partial_revision.csv", sep=sep, index=False)
            else:
                manifest_data.to_csv(f"gs://{args.src_bucket}/generated_partial_revision.csv", sep=sep, index=False)
    all_collection_ids = build_collections(args, sess, manifest_data, sep)

    return all_collection_ids

def perform_full_revision(sess, args, manifest_url, sep):
    client = storage.Client()
    remove_collections(client, args, sess)
    all_collection_ids = perform_partial_revision(sess, args, manifest_url, sep)
    return all_collection_ids


def prebuild_from_manifests(args, sep=','):
    with sa_session(echo=False) as sess:
        for manifest_url, manifest_type in args.manifests:
            if manifest_type == 'partial_deletion':
                all_collection_ids = perform_partial_deletion(sess, args, manifest_url, sep)
                pass
            elif manifest_type == 'partial_revision':
                all_collection_ids = perform_partial_revision(sess, args, manifest_url, sep)
            elif manifest_type == 'full_revision':
                all_collection_ids = perform_full_revision(sess, args, manifest_url, sep)
            else:
                errlogger.error(f'Unknown manifest type {manifest_type}')
                exit(-1)
        sess.commit()

    if args.validate:
        if args.analysis_result:
            if validate_analysis_result(args) == -1:
                exit -1
        else:
            if validate_original_collection(args, all_collection_ids) == -1:
                exit -1

    if args.gen_hashes:
        gen_hashes()
    return

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

