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
from google.cloud import storage, bigquery
from bq.bq_utilities import get_github_directory_contents_from_comet, \
    get_data_from_comet
import logging
from utilities.logging_config import successlogger, progresslogger, errlogger
import contextlib
from perform_additions import perform_additions, validate_additions
from perform_revisions import perform_revisions, validate_revisions
from perform_deletions import perform_deletions, validate_deletions
import pandas as pd

from google.cloud import storage
from sqlalchemy import select
from utilities.sqlalchemy_helpers import sa_session



def add_manifest_to_manifest_hash_map(manifest_data):
    client = bigquery.Client()
    query = f"""
INSERT INTO `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.manifest_hash_map` VALUES
("{manifest_data['source_doi']}","{manifest_data['manifest_url']}", "{manifest_data['md5_hash']}", {manifest_data['idc_version']})

"""
    result = client.query(query)
    return result


def get_previous_manifest_hashes():
    client = bigquery.Client()
    query = f"""
SELECT *
FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.manifest_hash_map`
"""
    df = client.query(query).to_dataframe()
    return df


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






def preingest_source(args, manifest, manifest_data):
    # Remove NaNs one more time
    all_nan_cols = manifest.columns[manifest.isna().all()]
    manifest[all_nan_cols] = manifest[all_nan_cols].astype(object).fillna("")
    # Make sure that PatientID columns are strings not ints
    manifest['PatientID'] = manifest['PatientID'].astype(str)
    if 'current_PatientID' in manifest:
        manifest['current_PatientID'] = manifest['current_PatientID'].astype(str)

    deletions = manifest[manifest["operation"] == "deletion"]
    possible_replacements = manifest[manifest["operation"] == "replacement"]
    if len(possible_replacements):
        revisions = possible_replacements[ \
            (possible_replacements["collection_id"] == possible_replacements["current_collection_id"]) & \
            (possible_replacements["PatientID"] == possible_replacements["current_PatientID"]) & \
            (possible_replacements["StudyInstanceUID"] == possible_replacements["current_StudyInstanceUID"]) & \
            (possible_replacements["SeriesInstanceUID"] == possible_replacements["current_SeriesInstanceUID"]) & \
            (possible_replacements["SOPInstanceUID"] == possible_replacements["current_SOPInstanceUID"]) & \
            (possible_replacements["source_doi"] == possible_replacements["current_source_doi"])
            ]

        replacements = possible_replacements[ \
            (possible_replacements["collection_id"] != possible_replacements["current_collection_id"]) | \
            (possible_replacements["PatientID"] != possible_replacements["current_PatientID"]) | \
            (possible_replacements["StudyInstanceUID"] != possible_replacements["current_StudyInstanceUID"]) | \
            (possible_replacements["SeriesInstanceUID"] != possible_replacements["current_SeriesInstanceUID"]) | \
            (possible_replacements["SOPInstanceUID"] != possible_replacements["current_SOPInstanceUID"]) | \
            (possible_replacements["source_doi"] != possible_replacements["current_source_doi"])
            ]
    else:
        replacements = None
        revisions = None

    additions = manifest[manifest["operation"] == "addition"]

    with sa_session(echo=False) as sess:
        if len(deletions):
            progresslogger.info(f'Performing {len(deletions)} deletions')
            perform_deletions(sess, deletions)
            if not validate_deletions(sess, deletions):
                errlogger.error(f'Deletion validation failed')
                sess.rollback()
                return False
        else:
            progresslogger.info(f'No deletions')

        if isinstance(replacements, pd.DataFrame) and len(replacements):
            progresslogger.info(f'Performing replacements')

            progresslogger.info(f'Performing {len(replacements)} replacement deletions')
            perform_deletions(sess, replacements)
            if not validate_deletions(sess, replacements):
                errlogger.error(f'Deletion validation failed')
                sess.rollback()
                return False

            progresslogger.info(f'Performing {len(replacements)} replacement additions')
            perform_additions(args, sess, replacements)
            if not validate_additions(sess, replacements):
                errlogger.error(f'Addition validation failed')
                sess.rollback()
                return False
        else:
            progresslogger.info(f'No replacement deletions')

        if len(additions):
            progresslogger.info(f'Performing {len(additions)} additions')
            perform_additions(args, sess, additions)
            if not validate_additions(sess, additions):
                errlogger.error(f'Addition validation failed')
                sess.rollback()
                return False
        else:
            progresslogger.info('No additions')

        if isinstance(revisions, pd.DataFrame) and len(revisions):
            progresslogger.info(f'Performing {len(revisions)} revisions')
            perform_revisions(args, sess, revisions)
            if not validate_revisions(sess, revisions):
                errlogger.error(f'Revision validation failed')
                sess.rollback()
                return False
        else:
            progresslogger.info('No revisions')


        add_manifest_to_manifest_hash_map(manifest_data)
        sess.commit()
        return True

def preingest_original_data_sources(args, existing_hashes):
    if args.extended_manifest_url:
        progresslogger.info(f'Ingesting from manifest {args.extended_manifest_url}')
        manifest = pd.read_csv(args.extended_manifest_url,
                               sep=',', header=0)

        preingest_source(args, manifest)
    else:
        collection_files = get_github_directory_contents_from_comet("collections/original", args.comet_branch)
        for collection_file in collection_files:
                # print(collection_file)
                data = get_data_from_comet(f"collections/original/{collection_file}", branch=args.comet_branch)
                try:
                    for source in data['sources']:
                        if 'submitter_manifests' in source:
                            manifests = source['submitter_manifests']
                            for manifest in manifests:
                                # If we've not previously preingested from this manifest:
                                if existing_hashes[existing_hashes['md5_hash'] == manifest['hash']].empty:
                                    manifest_data = {
                                        "source_doi": source["source_doi"],
                                        "manifest_url": manifest["url"],
                                        "md5_hash": manifest["hash"],
                                        "idc_version": args.version
                                    }
                                    manifest_id = manifest['url'].rsplit('/',1)[1]
                                    src_bucket_id = manifest['url'].split('gs://')[1].split('/',1)[0]
                                    src_subdir = manifest['url'].removeprefix(f'gs://{src_bucket_id}/')
                                    src_subdir = src_subdir.removesuffix(f'{manifest_id}')
                                    src_subdir = src_subdir.removesuffix('/')

                                    # Check whether we've already validated the manifest
                                    bucket = storage.Client().bucket(src_bucket_id)
                                    blob_name = f'{src_subdir}/etl_validated_{manifest["hash"]}-{manifest_id}' if src_subdir else \
                                        f'etl_validated_{manifest["hash"]}-{manifest_id}'
                                    if bucket.blob(blob_name).exists():
                                        progresslogger.info(f'Pre-ingesting from manifest {f"gs://{src_bucket_id}/{blob_name}"}')
                                        try:
                                            if f"gs://{src_bucket_id}/{blob_name}" not in args.skipped_extended_manifests:
                                                manifest = pd.read_csv(f"gs://{src_bucket_id}/{blob_name}",
                                                                           sep=',', header=0)
                                            else:
                                                progresslogger.info(f'Skipping {f"gs://{src_bucket_id}/{blob_name}"}')
                                                continue
                                        except Exception as exc:
                                            errlogger.error(f'Failed to read manifest: {exc}')
                                            exit(-1)
                                        with temp_logger(blob_name.replace("/", "\u2044")):
                                            result = preingest_source(args, manifest, manifest_data)
                                        if result:
                                            progresslogger.info(f'Validated')
                                        else:
                                            errlogger.error(f'Validation failure on {blob_name}')
                                            progresslogger.info(f'Validation failure on {blob_name}')

                                    else:
                                        errlogger.error(f'ETL generated manifest gs://{src_bucket_id}/{blob_name} not found')
                                        exit(1)
                                else:
                                    progresslogger.info((f'Previously preingested manifest {manifest["url"]}'))
                except Exception as exc:
                    errlogger.error(exc)
                    exit(1)
    return


def preingest_analysis_results(args, existing_hashes):
    if args.extended_manifest_url:
        progresslogger.info(f'Ingesting from manifest {args.extended_manifest_url}')

        manifest = pd.read_csv(args.extended_manifest_url,
                               sep=',', header=0)
        manifest_data = {
            "source_doi": args.source_doi,
            "manifest_url": manifest["url"],
            "md5_hash": manifest["hash"],
            "idc_version": args.version
        }
        preingest_source(args, manifest, manifest_data)
    else:
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
                            manifest_data = {
                                "source_doi": data["source_doi"],
                                "manifest_url": manifest["url"],
                                "md5_hash": manifest["hash"],
                                "idc_version": args.version
                            }
                            manifest_id = manifest['url'].rsplit('/', 1)[1]
                            src_bucket_id = manifest['url'].split('gs://')[1].split('/', 1)[0]
                            src_subdir = manifest['url'].removeprefix(f'gs://{src_bucket_id}/')
                            src_subdir = src_subdir.removesuffix(f'{manifest_id}')
                            src_subdir = src_subdir.removesuffix('/')

                            # Check whether we've already validated the manifest
                            bucket = storage.Client().bucket(src_bucket_id)
                            blob_name = f'{src_subdir}/etl_validated_{manifest["hash"]}-{manifest_id}' if src_subdir else \
                                f'etl_validated_{manifest["hash"]}-{manifest_id}'
                            if bucket.blob(blob_name).exists():
                                progresslogger.info(f'Pre-ingesting from manifest {f"gs://{src_bucket_id}/{blob_name}"}')
                                try:
                                    manifest = pd.read_csv(f"gs://{src_bucket_id}/{blob_name}",
                                                           sep=',', header=0)
                                except Exception as exc:
                                    errlogger.error(f'Failed to read manifest: {exc}')
                                    exit(-1)
                                with temp_logger(blob_name.replace("/", "\u2044")):
                                    result = preingest_source(args, manifest, manifest_data)
                                if result:
                                    progresslogger.info(f'Validated')
                                else:
                                    errlogger.error(f'Validation failure on {blob_name}')
                                    progresslogger.info(f'Validation failure on {blob_name}')
                            else:
                                errlogger.error(f'ETL generated manifest gs://{src_bucket_id}/{blob_name} not found')
                                exit(1)
                        else:
                            progresslogger.info((f'Previously preingested manifest {manifest["url"]}'))
            except Exception as exc:
                errlogger.error(exc)
                exit(1)
    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--processes', default=0)
    parser.add_argument('--version', default=settings.CURRENT_VERSION)
    parser.add_argument("--comet_branch", default=f'release/v{settings.CURRENT_VERSION}')
    parser.add_argument("--extended_manifest_url",
            default="", \
                        help='Process this manifest if not null')
    parser.add_argument("--source_doi", default='', help="source_doi of the source of the --extended_manifest_url")
    parser.add_argument("--skipped_extended_manifests", default=[], \
                        help='Skip processing these manifests')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    args.client=storage.Client()

    # Get a dataframe of previously processed manifest hashes
    existing_hashes = get_previous_manifest_hashes()
    args.analysis_result = False
    preingest_original_data_sources(args,existing_hashes)
    args.analysis_result = True
    preingest_analysis_results(args,existing_hashes)