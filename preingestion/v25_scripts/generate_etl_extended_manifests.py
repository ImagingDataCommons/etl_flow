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


import os
import sys
import argparse
import settings

from python_settings import settings
from preingestion.preingestion_code.populate_idc_metadata_tables_from_manifest import prebuild_from_manifest
from google.cloud import storage, bigquery
from bq.bq_utilities import dataframe_to_bq, get_github_directory_contents_from_comet, \
    get_data_from_comet
import logging
from utilities.logging_config import successlogger, progresslogger, errlogger
import contextlib

import pandas as pd

import time

from concurrent.futures import ProcessPoolExecutor, as_completed
from base64 import b64decode
import hashlib
import threading
import multiprocessing as mp
from google.cloud import storage
from pydicom import dcmread

from utilities.sqlalchemy_helpers import sa_session



_thread_local = threading.local()


@contextlib.contextmanager
def temp_logger(source_doi):
    for hdlr in successlogger.handlers[:]:
        successlogger.removeHandler(hdlr)
    os.makedirs(f'{settings.LOG_DIR}/{source_doi}', exist_ok=True)

    success_fh = logging.FileHandler(f'{settings.LOG_DIR}/{source_doi}/success.log')
    successlogger.addHandler(success_fh)
    successformatter = logging.Formatter('%(message)s')
    success_fh.setFormatter(successformatter)

    for hdlr in progresslogger.handlers[:]:
        progresslogger.removeHandler(hdlr)
    progress_fh = logging.FileHandler(f'{settings.LOG_DIR}/{source_doi}/progress.log')
    progresslogger.addHandler(progress_fh)
    successformatter = logging.Formatter('%(message)s')
    progress_fh.setFormatter(successformatter)

    # Always empty the error file
    with open(f'{settings.LOG_DIR}/{source_doi}/error.log', 'w') as f:
        pass
    for hdlr in errlogger.handlers[:]:
        errlogger.removeHandler(hdlr)
    err_fh = logging.FileHandler(f'{settings.LOG_DIR}/{source_doi}/error.log')
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    try:
        yield
    finally:
        successlogger.removeHandler(success_fh)
        progress_fh = logging.FileHandler(f'{settings.LOG_DIR}/{"success.log"}')
        progresslogger.addHandler(progress_fh)
        successformatter = logging.Formatter('%(message)s')
        progress_fh.setFormatter(successformatter)

        progresslogger.removeHandler(progress_fh)
        progress_fh = logging.FileHandler('{}/progress.log'.format(settings.LOG_DIR))
        progresslogger.addHandler(progress_fh)
        successformatter = logging.Formatter('%(message)s')
        progress_fh.setFormatter(successformatter)

        errlogger.removeHandler(err_fh)
        for hdlr in errlogger.handlers[:]:
            errlogger.removeHandler(hdlr)
        err_fh = logging.FileHandler('{}/error.log'.format(settings.LOG_DIR))
        errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
        errlogger.addHandler(err_fh)
        err_fh.setFormatter(errformatter)
    return


def get_previous_manifest_hashes():
    client = bigquery.Client()
    query = f"""
SELECT *
FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.manifest_hash_map`
"""
    df = client.query(query).to_dataframe()
    return df


def add_manifest_to_manifest_hash_map(source_doi, manifest_url, md5_hash, idc_version):
    client = bigquery.Client()
    query = f"""
INSERT INTO `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.manifest_hash_map` VALUES
({source_doi}, {manifest_url}, {md5_hash}, {idc_version})
    """

    result= client.query(query)
    return


def get_client():
    if not hasattr(_thread_local, "client"):
        _thread_local.client = storage.Client()
    return _thread_local.client


def streaming_md5_hasher(bucket_name, blob_name, chunk_size=pow(2, 30)):
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
def get_computed_hashes(blob_names, bucket_name, max_workers= 2 * os.cpu_count()):
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
    # Get various metadata from the DICOM files. This includes UIDs, size, hash when available
    # In the case of a deletion, there is no corresponding DICOM file
    blob_names = {}
    for row in manifest.itertuples():
        if row.operation != "deletion":
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

    if not blob_names:
        progresslogger.info(f'No additions or replacements in manifest')
        return manifest

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
    if args.validate_hashes:
        stream_hash_blob_names = {metadata['blob_name']: metadata['size'] for relative_gcs_url, metadata in results.items()
                                  if metadata['instance_hash'] == ""}
        if stream_hash_blob_names:
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
        else:
            progresslogger.info(f'No missing hashes')
    else:
        progresslogger.info(f'Skipped validation of missing hashes')

    return manifest


# Validate and cleanup a manifest, and add DICOM ids extracted from the DICOM blobs
def cleanup_and_validate_manifest(src_bucket_id, current_dicom_ids, src_subdir, manifest_id, versioned_source_doi, collection_name_id_pairs):
    try:
        if src_subdir:
            manifest = pd.read_csv(f"gs://{src_bucket_id}/{src_subdir}/{manifest_id}", sep=',', header=0)
        else:
            manifest = pd.read_csv(f"gs://{src_bucket_id}/{manifest_id}", sep=',', header=0)
    except Exception as exc:
        errlogger.error(f'Failed to read manifest: {exc}')
        exit(-1)

    # Remove NaNs
    all_nan_cols = manifest.columns[manifest.isna().all()]
    manifest[all_nan_cols] = manifest[all_nan_cols].astype(object).fillna("")


    manifest["blob_name"] = ""
    manifest["PatientID"] = ""
    manifest["StudyInstanceUID"] = ""
    manifest["SeriesInstanceUID"] = ""
    manifest['size'] = 0
    manifest['full_gcs_url'] = ""
    manifest['versioned_source_doi'] = versioned_source_doi
    manifest['full_gcs_url'] = manifest['relative_gcs_url'].apply(
        lambda x: f'gs://{src_bucket_id}/{src_subdir}/{x[2:]}' if src_subdir else f'gs://{src_bucket_id}/{x[2:]}')
    manifest = manifest.merge(
        collection_name_id_pairs,
        on='collection_id',
        how='left'  # Left join ensures you don't lose rows from manifest
    )
    if "crdc_instance_uuid" not in manifest.columns:
        manifest["crdc_instance_uuid"] = ""
    if "source_file_url" not in manifest.columns:
        manifest["source_file_url"] = ""
    if "source_file_hash" not in manifest.columns:
        manifest["source_file_hash"] = ""
    if "relative_gcs_url" not in manifest.columns:
        manifest["relative_gcs_url"] = ""
    if "instance_hash" not in manifest.columns:
        manifest["instance_hash"] = ""
    if "source_file_url" not in manifest.columns:
        manifest["source_file_url"] = ""


    # Add DICOM IDs to the manifest, validate hashes,
    manifest = get_dicom_metadata_from_files(manifest, src_bucket_id, src_subdir)
    # Convert NaNs to ""
    # manifest.fillna("", inplace=True)
    # current_dicom_ids.fillna("", inplace=True)

    # Convert NaNs to ""
    all_nan_cols = current_dicom_ids.columns[current_dicom_ids.isna().all()]
    current_dicom_ids[all_nan_cols] = current_dicom_ids[all_nan_cols].astype(object).fillna("")

    # Merge in the IDs of the current DICOM version
    manifest = pd.merge(manifest, current_dicom_ids, on='crdc_instance_uuid', how='left' )

    # Remove NaNs one more time
    all_nan_cols = manifest.columns[manifest.isna().all()]
    manifest[all_nan_cols] = manifest[all_nan_cols].astype(object).fillna("")

    # Do some more clean up
    # Strip leading and trailing spaces from string columns
    string_cols = manifest.select_dtypes(include=['object']).columns
    manifest[string_cols] = manifest[string_cols].apply(lambda x: x.str.strip())


    return manifest

# Get the DICOM UIDs and other values from of instances in a source for the current (last released) IDC version
def get_current_dicom_ids(collection_ids, source_doi):
    client = bigquery.Client()
    query = f"""
SELECT crdc_instance_uuid, collection_id current_collection_id, patientID current_PatientID, StudyInstanceUID current_StudyInstanceUID, 
    SeriesInstanceUID current_SeriesInstanceUID, SOPInstanceUID current_SOPInstanceUID, source_doi current_source_doi, instance_hash current_hash 
FROM `{settings.DEV_PROJECT}.idc_v{settings.PREVIOUS_VERSION}_pub.dicom_all`
WHERE collection_id in {collection_ids} AND source_doi = '{source_doi}'
ORDER BY collection_id, patientID, StudyInstanceUID, SeriesInstanceUID, SOPInstanceUID
    """

    current_ids = client.query(query).to_dataframe()
    return current_ids


def generate_manifest(args, collection_ids, src_bucket_id, src_subdir, manifest_id, source_doi, versioned_source_doi, manifest_hash, collection_name_id_pairs):
    with sa_session(echo=False) as sess:
        current_dicom_ids = get_current_dicom_ids(collection_ids, source_doi)
        manifest = cleanup_and_validate_manifest(src_bucket_id, current_dicom_ids, src_subdir, manifest_id, versioned_source_doi, collection_name_id_pairs)
        validated_manifest_path = f'gs://{src_bucket_id}/{src_subdir}/etl_validated_{manifest_hash}-{manifest_id}' if src_subdir else \
            f'gs://{src_bucket_id}/etl_validated_{manifest_hash}-{manifest_id}'
        manifest.to_csv(validated_manifest_path, index=False)
    return


def generate_original_sources_manifests(args, existing_hashes, collection_name_id_pairs):
    collection_files = get_github_directory_contents_from_comet("collections/original", args.comet_branch)
    for collection_file in collection_files:
            data = get_data_from_comet(f"collections/original/{collection_file}", branch=args.comet_branch)
            try:
               for source in data['sources']:
                   if 'submitter_manifests' in source:
                        collection_name = data['collection_name']
                        collection_id = data['collection_id']
                        source_doi = source['source_doi']
                        versioned_source_doi = source['versioned_source_doi'] if 'versioned_source_doi' in source else ""
                        manifests = source['submitter_manifests']
                        for manifest in manifests:
                            # If we've not previously preingested from this manifest:
                            if existing_hashes[existing_hashes['md5_hash'] == manifest['hash']].empty:
                                manifest_id = manifest['url'].rsplit('/',1)[1]
                                src_bucket_id = manifest['url'].split('gs://')[1].split('/',1)[0]
                                src_subdir = manifest['url'].removeprefix(f'gs://{src_bucket_id}/')
                                src_subdir = src_subdir.removesuffix(f'{manifest_id}')
                                src_subdir = src_subdir.removesuffix('/')
                                folder_id = f'{collection_id}-{source_doi}'
                                # Check whether we've already validated the manifest
                                bucket = storage.Client().bucket(src_bucket_id)
                                blob_name = f'{src_subdir}/etl_validated_{manifest["hash"]}-{manifest_id}' if src_subdir else \
                                    f'etl_validated_{manifest["hash"]}-{manifest_id}'
                                if args.regen or not bucket.blob(blob_name).exists():
                                     progresslogger.info(f'Generating manifest {blob_name} for collection {collection_id}, source {source_doi}')
                                     with temp_logger(folder_id):
                                        collection_ids = f"('{collection_id}')"
                                        generate_manifest(args, collection_ids, src_bucket_id, src_subdir, manifest_id, source_doi, \
                                                  versioned_source_doi, manifest['hash'], collection_name_id_pairs)
                                else:
                                     progresslogger.info(f'Previously generated manifest {blob_name} for collection {collection_id}, source {source_doi}')
                            else:
                                progresslogger.info((f'Previously ingested manifest {manifest["url"]}'))
            except Exception as exc:
                errlogger.error(exc)
                exit(1)
    return


def generate_analysis_results_manifests(args, existing_hashes, collection_name_id_pairs):

    analysis_results_files = get_github_directory_contents_from_comet("collections/analysis", args.comet_branch)
    for analysis_result in analysis_results_files:
        data = get_data_from_comet(f"collections/analysis/{analysis_result}", branch=args.comet_branch)
        try:
            if 'submitter_manifests' in data:
                analysis_result_name = data['analysis_result_name']
                analysis_result_id = data['analysis_result_id']
                source_doi = data['source_doi']
                versioned_source_doi = data['versioned_source_doi'] if 'versioned_source_doi' in data else ""
                manifests = data['submitter_manifests']
                for manifest in manifests:
                    # If we've not previously preingested from this manifest:
                    if existing_hashes[existing_hashes['md5_hash'] == manifest['hash']].empty:
                        manifest_id = manifest['url'].rsplit('/', 1)[1]
                        src_bucket_id = manifest['url'].split('gs://')[1].split('/', 1)[0]
                        src_subdir = manifest['url'].removeprefix(f'gs://{src_bucket_id}/')
                        src_subdir = src_subdir.removesuffix(f'{manifest_id}')
                        src_subdir = src_subdir.removesuffix('/')
                        folder_id = f'{analysis_result_id}-{source_doi}'
                        # Check whether we've already validated the manifest
                        bucket = storage.Client().bucket(src_bucket_id)
                        blob_name = f'{src_subdir}/etl_validated_{manifest["hash"]}-{manifest_id}' if src_subdir else \
                            f'etl_validated_{manifest["hash"]}-{manifest_id}'
                        if args.regen or not bucket.blob(blob_name).exists():
                            progresslogger.info(
                                f'Generating manifest {blob_name} for analysis_result {analysis_result_id}, source {source_doi}')
                            with temp_logger(folder_id):
                                if src_subdir:
                                    manifest_data = pd.read_csv(f"gs://{src_bucket_id}/{src_subdir}/{manifest_id}", sep=',',
                                                           header=0)
                                else:
                                    manifest_data = pd.read_csv(f"gs://{src_bucket_id}/{manifest_id}", sep=',', header=0)
                                collection_ids = manifest_data["collection_id"].unique()
                                collection_ids = ",".join(f"\'{id}\'" for id in collection_ids)
                                collection_ids = f'({collection_ids})'
                                generate_manifest(args, collection_ids, src_bucket_id, src_subdir, manifest_id, source_doi, \
                                                  versioned_source_doi, manifest['hash'], collection_name_id_pairs)
                                continue
                        else:
                            progresslogger.info(
                                f'Previously generated manifest {blob_name} for analysis_result {analysis_result_id}, source {source_doi}')
                    else:
                        progresslogger.info((f'Previously ingested manifest {manifest["url"]}'))
        except Exception as exc:
            errlogger.error(exc)
            exit(1)
    return

def get_collection_name_id_pairs():
    client = bigquery.Client()
    query = f"""
SELECT DISTINCT collection_name, collection_id
FROM `{settings.DEV_PROJECT}.idc_v{settings.PREVIOUS_VERSION}_dev.all_sources`
    """

    pairs = client.query(query).to_dataframe()
    return pairs


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--processes', default=0)
    parser.add_argument('--version', default=settings.CURRENT_VERSION)
    parser.add_argument("--comet_branch", default='release/v25')
    parser.add_argument('--regen', default=True, help='If True, regenerate a manifest even is previously generated')
    parser.add_argument('--validate_hashes', default=False, help='If True, validate manifest instance_hash when GCS does not have md5' )

    parser.add_argument('--gen_hashes', default=False, help=' Generate hierarchical hashes of collection if True.')
    parser.add_argument('--validate', type=bool, default=True, help='True if validation is to be performed')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    args.client=storage.Client()

    # Get a dataframe of previously processed manifest hashes
    existing_hashes = get_previous_manifest_hashes()
    collection_name_id_pairs = get_collection_name_id_pairs()
    generate_original_sources_manifests(args,existing_hashes, collection_name_id_pairs)
    generate_analysis_results_manifests(args,existing_hashes, collection_name_id_pairs)