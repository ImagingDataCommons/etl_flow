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
import os
import sys
import settings
import json5
from google.cloud import bigquery
from idc.models import IDC_Collection, IDC_Patient, IDC_Study, IDC_Series, IDC_Instance
from utilities.logging_config import successlogger, errlogger, progresslogger
from base64 import b64decode
import pandas as pd
from preingestion.validation_code.validate_analysis_result import validate_analysis_result
from preingestion.validation_code.validate_original_collection import validate_original_collection
from preingestion.preingestion_code.gen_hashes_sql import gen_hashes
from preingestion.preingestion_code.gen_manifest_from_dicom_metadata import build_manifest

import time

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
import hashlib
from ingestion.utilities.utils import get_merkle_hash #, streaming_md5_hasher
import threading
import multiprocessing as mp

from utilities.sqlalchemy_helpers import sa_session
from google.cloud import storage

from multiprocessing import Queue, Process
from queue import Empty

from pydicom import dcmread


_thread_local = threading.local()


def get_client():
    if not hasattr(_thread_local, "client"):
        _thread_local.client = storage.Client()
    return _thread_local.client


# def get_client():
#     global _client
#     if _client is None:
#         _client = storage.Client()
#     return _client

def streaming_md5_hasher(bucket_name, blob_name, chunk_size=pow(2, 20)):
    client = get_client()
    blob = client.bucket(bucket_name).blob(blob_name)
    md5_hasher = hashlib.md5()
    with blob.open(mode="rb", chunk_size=chunk_size) as f:
        i = 0
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            md5_hasher.update(chunk)

    return md5_hasher.hexdigest()


# Compute hashes of all composite blobs
def get_computed_hashes(blob_names, bucket_name, max_workers=2 * 2 * os.cpu_count()):
    results = {}
    errors = {}
    max_workers = min(len(blob_names), max_workers)
    ctx = mp.get_context("spawn")  # fresh interpreter per child, no inherited thread/lock state
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        #    with ProcessPoolExecutor(max_workers=max_workers, mp_context=ctx) as executor:
        future_to_name = {
            executor.submit(streaming_md5_hasher, bucket_name, name): name
            for name in blob_names
        }
        for future in as_completed(future_to_name):
            name = future_to_name[future]
            try:
                results[name] = future.result()
            except Exception as e:
                errors[name] = str(e)

    return results, errors


def get_metadata_from_a_file(bucket_name, blob_name, chunk_size=pow(2, 30)):
    client = get_client()
    blob = client.bucket(bucket_name).blob(blob_name)
    with blob.open('rb') as f:
        try:
            r = dcmread(f, specific_tags=['PatientID', 'StudyInstanceUID', 'SeriesInstanceUID',
                                          'SOPInstanceUID'], stop_before_pixels=True)
            patient_id = r.PatientID
            study_id = r.StudyInstanceUID
            series_id = r.SeriesInstanceUID
            instance_id = r.SOPInstanceUID
        except Exception as exc:
            errlogger.error(f'pydicom failed for {blob.name}: {exc}')
            exit(1)
        try:
            hash = b64decode(blob.md5_hash).hex()
        except TypeError:
            hash = ""

        # progresslogger.info(f'Got {blob.name} metadata')
        # blob_subname = blob.name.removeprefix(f'{args.subdir}/') if args.subdir else blob.name

        return {"blob_name": blob_name, "PatientID": patient_id, "StudyInstanceUID": study_id,
                "SeriesInstanceUID": series_id,
                "SOPInstanceUID": instance_id, "instance_hash": hash, "size": blob.size}


def get_dicom_metadata_from_files(manifest, src_bucket, src_subdir, max_workers=2 * os.cpu_count()):
    blob_names = {}
    for row in manifest.itertuples():
        if row.relative_gcs_url.startswith('/'):
            blob_names[row.relative_gcs_url[1:]] = row.relative_gcs_url
        elif row.relative_gcs_url.startswith('./'):
            if src_subdir:
                # Remove leading "."
                blob_names[f'{src_subdir}{row.relative_gcs_url[1:]}'] = row.relative_gcs_url
            else:
                # Remove leading "./"
                blob_names[f'{row.relative_gcs_url[2:]}'] = row.relative_gcs_url

        else:
            errlogger.error(f'Invalid relative_gcs_url: {row.relative_gcs_url}')
            exit(1)

    results = {}
    errors = {}
    ctx = mp.get_context("spawn")
    max_workers = min(len(blob_names), max_workers)
    # with ProcessPoolExecutor(max_workers=max_workers, mp_context=ctx) as executor:
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_name = {
            executor.submit(get_metadata_from_a_file, src_bucket, blob_name): relative_gcs_url
            for blob_name, relative_gcs_url in blob_names.items()
        }
        for future in as_completed(future_to_name):
            name = future_to_name[future]
            try:
                results[name] = future.result()
            except Exception as e:
                errors[name] = str(e)
    if errors != {}:
        errlogger.error(f'Errors while getting blob metadata: {errors}')
        exit(1)

    progresslogger.info(f'Got metadata for {len(results)} blobs')

    missing_hashes = []

    for relative_gcs_url, metadata in results.items():
        for id, datum in metadata.items():
            mask = manifest["relative_gcs_url"] == relative_gcs_url
            if id in ["SOPInstanceUID"]:
                if datum != manifest.loc[mask, id].item():
                    errlogger.error(
                        f'{id} mismatch for {relative_gcs_url}: Manifest: {manifest.loc[mask, id].item()}, DICOM: {datum}')
                    exit(1)
            elif id == "instance_hash":
                if datum == "":
                    # Couldn't get the hash from the blob metadata. Will need to compute it
                    missing_hashes.append(relative_gcs_url)
                else:
                    if datum != manifest.loc[mask, id].item():
                        errlogger.error(
                            f'instance_hash mismatch for {relative_gcs_url}: Manifest: {manifest.loc[mask, id].item()}, DICOM: {datum}')
                        exit(1)
            else:
                manifest.loc[mask, id] = datum

    stream_hash_blob_names = {metadata['blob_name']: metadata['size'] for relative_gcs_url, metadata in results.items()
                              if metadata['instance_hash'] == ""}
    progresslogger.info(
        f'Computing {len(stream_hash_blob_names)} hashes')
    start = time.time()
    results, errors = get_computed_hashes(stream_hash_blob_names, src_bucket)
    elapsed = time.time() - start
    bytes = sum(size for blob_name, size in stream_hash_blob_names.items())
    rate = bytes / elapsed
    progresslogger.info(
        f'Computed {len(stream_hash_blob_names)} hashes: Elapsed: {elapsed} sec, Total size: {bytes / pow(10, 9)} GB, Ingestion BW: {rate / pow(10, 9)}  GB/s')

    for blob_name, hash in results.items():
        if manifest.loc[manifest['blob_name'] == blob_name, 'instance_hash'].item() != hash:
            errlogger.error(f'Hash validation error for {blob_name}')
            exit(1)

    return manifest


# Validate and cleanup a manifest, and add DICOM ids extracted from the DICOM blobs
def cleanup_and_validate_manifest(src_bucket_id, src_subdir, manifest_id):
    try:
        if src_subdir:
            manifest = pd.read_csv(f"gs://{src_bucket_id}/{src_subdir}/{manifest_id}", sep=',', header=0)
        else:
            manifest = pd.read_csv(f"gs://{src_bucket_id}/{manifest_id}", sep=',', header=0)
    except Exception as exc:
        errlogger.error(f'Failed to read manifest: {exc}')
        exit(-1)

    # Convert NaNs to ""
    manifest = manifest.fillna('')

    # Remove whitespace
    manifest = manifest.map(lambda x: x.strip())

    if "blob_name" not in manifest.columns:
        manifest["blob_name"] = ""
    if "StudyInstanceUID" not in manifest.columns:
        manifest["StudyInstanceUID"] = ""
    if "SeriesInstanceUID" not in manifest.columns:
        manifest["SeriesInstanceUID"] = ""
    if "size" not in manifest.columns:
        manifest['size'] = 0

    missing_hashes = []

    # Add DICOM IDs to the manifest
    manifest = get_dicom_metadata_from_files(manifest, src_bucket_id, src_subdir)


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
        instance.hash = instance_data["md5_hash"]
    except:
        try:
            instance.hash = b64decode(blob.md5_hash).hex()
        except TypeError:
            # Can't get md5 hash for some blobs (maybe multipart copied/)
            # So try to compute it
            breakpoint()
            instance.hash = streaming_md5_hasher(blob)

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
    # series.license_url = args.license['license_url']
    # series.license_long_name = args.license['license_long_name']
    # series.license_short_name = args.license['license_short_name']
    # series.analysis_result = args.analysis_result
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
def worker(input, output, args, collection_id, src_bucket_id, source_doi, versioned_source_doi):
    with sa_session() as sess:
        client = storage.Client()
        bucket = client.bucket(src_bucket_id)
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





def process_additions_and_replacements(args, sess, manifest_data, source_doi, versioned_source_doi):
    client = storage.Client()

    dones = open(successlogger.handlers[0].baseFilename).read().splitlines()
    done_data = pd.DataFrame(dones, columns=['SOPInstanceUID'])

    all_collection_ids = sorted(manifest_data['collection_id'].unique())
    undone_data = pd.merge(manifest_data, done_data, how="left", on=['SOPInstanceUID'], indicator=True)
    undone_data = undone_data[undone_data['_merge'] == 'left_only']

    for collection_id in all_collection_ids:
        # Create the collection if it is not yet in the DB
        collection = sess.query(IDC_Collection).filter(IDC_Collection.collection_id == collection_id).first()
        if not collection:
            # The collection is not currently in the DB, so add it
            collection = IDC_Collection()
            collection.collection_id = collection_id
            collection.redacted = False
            sess.add(collection)
            # sess.commit()
            progresslogger.info(f'Collection {collection_id} added')
        else:
            progresslogger.info(f'Collection {collection_id} exists')


        collection_data = undone_data[undone_data['collection_id'] == collection_id]
        all_patient_ids = sorted(manifest_data["patientID"].unique())
        # All patients in the collection
        patient_in_collection_ids = sorted(collection_data['patientID'].unique())

        args.pid = 0
        if args.processes == 0:
            bucket = client.bucket(args.src_bucket_id)
            for patient_id in patient_in_collection_ids:
                # Data for this patient
                patient_data = collection_data[collection_data['patientID'] == patient_id]
                patient_index = f'{all_patient_ids.index(patient_id) + 1} of {len(all_patient_ids)}'
                build_patient(args, bucket, collection, patient_data, source_doi, versioned_source_doi)
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
                     Process(target=worker, args=(task_queue, done_queue, args, collection_id,
                                                 args.source_doi,
                                                 args.versioned_source_doi)))
                processes[-1].start()

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

                # sess.commit()

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



def process_deletions(args, sess, manifest):
    return manifest

def get_current_dicom_ids(collection_ids):
    client = bigquery.Client()
    query = f"""
SELECT collection_id, patientID, StudyInstanceUID, SeriesInstanceUID, SOPInstanceUID, source_doi
FROM `{settings.DEV_PROJECT}.idc_v{settings.PREVIOUS_VERSION}_pub.dicom_all`
WHERE collection_id in {collection_ids}
ORDER BY collection_id, patientID, StudyInstanceUID, SeriesInstanceUID, SOPInstanceUID
    """

    current_ids = client.query(query).to_dataframe()
    return current_ids

def prebuild_from_manifest(args, collection_ids, src_bucket_id, src_subdir, manifest_id, source_doi, versioned_source_doi, manifest_hash):
    with sa_session(echo=False) as sess:
        current_dicom_ids = get_current_dicom_ids(collection_ids)
        manifest = cleanup_and_validate_manifest(src_bucket_id, src_subdir, manifest_id)
        validated_manifest_path = f'gs://{src_bucket_id}/{src_subdir}/etl_validated_{manifest_hash}-{manifest_id}' if src_subdir else \
            f'gs://{src_bucket_id}/etl_validated_{manifest_hash}-{manifest_id}'
        manifest.to_csv(validated_manifest_path, index=False)

        process_deletions(args, sess, manifest)
        process_additions_and_replacements(args, sess, manifest)

        # manifest_data = process_deletions(args, sess, manifest_data)
        # manifest_data = process_additions_and_revisions(args, sess, src_bucket_id, src_subdir, manifest_data, source_doi, versioned_source_doi)

        # sess.commit()

    all_collection_ids = []
    if args.validate:
        if "analysis_result" in args and args.analysis_result:
            if validate_analysis_result(args) == -1:
                exit(1)
        else:
            if validate_original_collection(args, all_collection_ids) == -1:
                exit(1)
    if args.gen_hashes:
        gen_hashes()


    return

# if __name__ == '__main__':
#
#     parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
#     parser.add_argument('--src_bucket_id', default='j2kfixup', help='Bucket containing WSI instances')
#     parser.add_argument('--src_subdir', default='images', help='Bucket containing WSI instances')
#     parser.add_argument('--manifest_id', default='identifiers_catch.txt', help='Bucket containing WSI instances')
#
#     parser.add_argument('--source_doi', default="")
#     parser.add_argument('--versioned_source_doi', default="")
#
#     parser.add_argument('--validate', type=bool, default=True, help='True if validation is to be performed')
#     parser.add_argument('--gen_hashes', type=bool, default=True, help='True if hashes are to be generated')
#
#     args = parser.parse_args()
#     print("{}".format(args), file=sys.stdout)
#     args.client=storage.Client()
#
#
#
#
#     prebuild_from_manifest(args, args.src_bucket_id, args.src_subdir, args.manifest_id, args.source_doi, args.versioned_source_doi, sep='\t')
#
