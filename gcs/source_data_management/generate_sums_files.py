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

import os
import argparse
import json
from google.cloud  import storage
import settings
from get_tcia_pathology_metadata import bucket_collection_id, get_blob_metadata_from_package, get_aspera_package_urls
from utilities.logging_config import successlogger, progresslogger, errlogger
from time import strftime, gmtime
from base64 import b64decode
import logging
import contextlib

from ingestion.utilities.utils import md5_hasher

ASPERA_DOWNLOAD_FOLDER = '/mnt/disks/idc-etl/aspera'

@contextlib.contextmanager
def temp_logger(Download_slug):
    for hdlr in successlogger.handlers[:]:
        successlogger.removeHandler(hdlr)
    os.makedirs(f'{settings.LOG_DIR}/{Download_slug}', exist_ok=True)

    success_fh = logging.FileHandler(f'{settings.LOG_DIR}/{Download_slug}/success.log')
    successlogger.addHandler(success_fh)
    successformatter = logging.Formatter('%(message)s')
    success_fh.setFormatter(successformatter)

    for hdlr in progresslogger.handlers[:]:
        progresslogger.removeHandler(hdlr)
    progress_fh = logging.FileHandler(f'{settings.LOG_DIR}/{Download_slug}/progress.log')
    progresslogger.addHandler(progress_fh)
    successformatter = logging.Formatter('%(message)s')
    progress_fh.setFormatter(successformatter)

    # Always empty the error file
    with open(f'{settings.LOG_DIR}/{Download_slug}/error.log', 'w') as f:
        pass
    for hdlr in errlogger.handlers[:]:
        errlogger.removeHandler(hdlr)
    err_fh = logging.FileHandler(f'{settings.LOG_DIR}/{Download_slug}/error.log')
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


def get_conversion_source_hashs(aspera_files, dst_bucket, bucket_tag, tag, slug):
    storage_client = storage.Client(project='idc-source-data')
    buckets = storage_client.list_buckets()

    sources = []
    dst_bucket_name = dst_bucket.name.lower().replace('-', '_')
    for bucket in buckets:
        if dst_bucket_name in bucket.name.lower().replace('-', '_') or \
                bucket_tag and bucket_tag in bucket.name.lower().replace('-', '_'):
            progresslogger.debug(f'Adding bucket {bucket.name}')
            blobs = bucket.list_blobs()
            # prefix = f'{tag if not tag else (tag+"/" if bucket_tag != "cptac" else "CPTAC-"+tag+"/")}'
            prefix = f'{tag if not tag else tag+"/"}'
            for blob in blobs:
                if not tag or tag in blob.name:
                    try:
                        if bucket.name == 'cptac_pathology_source_data':
                            sources.append(f'{b64decode(blob.md5_hash).hex()} {blob.name.split("/",1)[-1]}')
                        elif bucket.name == 'idc-source-data-cmb':
                            sources.append(f'{b64decode(blob.md5_hash).hex()} {blob.name}')
                        else:
                            sources.append(f'{b64decode(blob.md5_hash).hex()} {prefix}{blob.name.split(slug+"/")[-1]}')

                    except Exception as exc:
                        pass

    return sources

def main(args, download_slugs=[]):
    aspera_package_urls = get_aspera_package_urls().sort_values(by="Download_slug")
    for _, package in aspera_package_urls.iterrows():
        slug = package['Download_slug']
        if args.download_slugs == [] or slug in args.download_slugs:
            with temp_logger(package['Download_slug']):
                dones = open(successlogger.handlers[0].baseFilename).read().splitlines()
                if package["Download_slug"] in dones and False:
                    progresslogger.info(f'Package {slug} previously processed')
                else:
                    TCIA_collection_version = package['TCIA_collection_version']
                    IDC_collection_name = package['IDC_collection_name']
                    idc_collection_id = package['IDC_collection_id']
                    client = storage.Client(args.dst_project)
                    bucket_tag = bucket_collection_id(
                        idc_collection_id if idc_collection_id else package["TCIA_collection_id"])
                    # Bucket name length must be 63 or less, so we truncate the collection part
                    dst_bucket = client.bucket(f"{args.dst_bucket_prefix}{bucket_tag}"[:63-len(args.dst_bucket_suffix)] +
                                               f"{args.dst_bucket_suffix}")
                    if IDC_collection_name in ['NLST', 'ICDC-Glioma']:
                        tag = ''
                    elif IDC_collection_name == 'CPTAC-CCRCC':
                        tag = 'CCRCC'
                    elif IDC_collection_name.startswith('CPTAC'):
                        tag = IDC_collection_name.split('-')[1]
                    elif IDC_collection_name.startswith('CMB'):
                        tag = IDC_collection_name
                    else:
                        tag = ""
                    aspera_url = package['Aspera_URL'].split('&')[0] if '&' in package['Aspera_URL'] \
                        else package['Aspera_URL']
                    aspera_id = aspera_url.rsplit('context=', 1)[1]
                    if tag:
                        prefix = f'{tag}/v{TCIA_collection_version}/{slug}'
                    else:
                        prefix = f'v{TCIA_collection_version}/{slug}'
                    blobs = dst_bucket.list_blobs(prefix=prefix+'/', delimiter='/')
                    found_sums_file = False
                    for blob in blobs:
                        if blob.name.endswith(".sums") and (not blob.name.endswith("generated.sums") or not args.regen_generated_sums_file):
                            progresslogger.info(f"Found sums file {dst_bucket.name}/{blob.name}")
                            found_sums_file = True
                            break

                    if found_sums_file:
                        continue
                    else:
                        progresslogger.info(f"Did not find sums file for {dst_bucket.name}/{prefix}")
                        if args.load_aspera_files and os.path.exists(
                                f"{settings.LOG_DIR}/{package['Download_slug']}/{slug}_files.json"):
                            with open(f"{settings.LOG_DIR}/{package['Download_slug']}/{slug}_files.json") as f:
                                aspera_files = json.load(f)
                        else:
                            manifest_params = get_blob_metadata_from_package(args, package, args.version)
                            aspera_files = manifest_params["aspera_files"]
                            with open(f"{settings.LOG_DIR}/{package['Download_slug']}/{slug}_files.json", 'w') as f:
                                json.dump(aspera_files, f)
                            progresslogger.info(manifest_params["logger_string"])
                        target_bucket = client.bucket(args.alternate_dst_bucket) if 'alternate_dst_bucket' in args and args.alternate_dst_bucket else dst_bucket
                        target_bucket_tag = "" if 'use_null_tag' in args and args.use_null_tag else bucket_tag
                        all_hashes = get_conversion_source_hashs(aspera_files, target_bucket, target_bucket_tag, tag, slug)
                        package_hashes = []
                        all_hashes_dict = {hash.split(" ", 1)[1]: hash for hash in all_hashes}
                        for aspera_file in aspera_files:
                            try:
                                hash = all_hashes_dict[aspera_file["path"][1:]]
                                package_hashes.append(hash)
                                # package_hashes.append(next(hash for hash in all_hashes if aspera_file["path"][1:] == hash.split(" ")[1]))
                            except:
                                errlogger.error(f'No hash found for {aspera_file["path"][1:]}')
                        sum_files_blob = dst_bucket.blob(f'{prefix}/generated.sums')
                        sum_files_blob.upload_from_string("\n".join(package_hashes))
                        progresslogger.info(f"Generated sums file for {dst_bucket.name}/{prefix}")
                        successlogger.info(slug)

    return

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--version", default=settings.CURRENT_VERSION-1)
    parser.add_argument('--processes', default=1)
    parser.add_argument('--mode', default='download')
    parser.add_argument("--dst_bucket_prefix", default="", help="dst_bucket ID prefix")
    parser.add_argument("--dst_bucket_suffix", default="_pathology_data", help="dst_bucket ID suffix")
    parser.add_argument("--dst_project", default='idc-source-data', help="Project in which to create bucket")
    parser.add_argument("--google_drive_folder", default="", help="Google Drive folder ID")
    parser.add_argument("--save_result", default=True, help="Save result to a Drive file if True")
    parser.add_argument("--download_slugs", default = ['upenn-gbm-da-path'], help="Slugs to process; all if empty")
    parser.add_argument("--manifest_file_name", default= f'manifest_{strftime("%Y%m%d_%H%M%S", gmtime())}.txt')
    parser.add_argument("--only_idc_collections", default=False, help="Only include a collection if IDC already has it")
    parser.add_argument("--skip", default=[], help="TCIA_collection_ids to be skipped")
    parser.add_argument("--load_aspera_files", default=True)
    parser.add_argument("--regen_generated_sums_file", default=True, help="If true, regen existing generated.sums")
    parser.add_argument('--alternate_dst_bucket', default='', help="Alternate bucket to access for md5 hashes. Intended for when instances in the the actually bucket do not have md5 hashes.")
    parser.add_argument('--use_null_tag', default='False', help="Use '' as the bucket tag. Intended for when instances in the the actually bucket do not have md5 hashes.")
    args = parser.parse_args()
    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    # Default process ID
    args.id = 0


    main(args)

