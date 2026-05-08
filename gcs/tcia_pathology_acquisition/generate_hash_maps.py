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

# For each TCIA Aspera pathology package, generate a table, mapping the URL in idc-source-data of
# each downloaded blob in the package, to its corresponding md5_hash
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

    # successlogger = logging.getLogger('root.success')
    # successlogger.setLevel(INFO)
    for hdlr in successlogger.handlers[:]:
        successlogger.removeHandler(hdlr)
    os.makedirs(f'{settings.LOG_DIR}/{Download_slug}', exist_ok=True)

    success_fh = logging.FileHandler(f'{settings.LOG_DIR}/{Download_slug}/success.log')
    successlogger.addHandler(success_fh)
    successformatter = logging.Formatter('%(message)s')
    success_fh.setFormatter(successformatter)

    # progresslogger = logging.getLogger('root.progress')
    # progresslogger.setLevel(INFO)
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


def get_conversion_source_hashes(aspera_files, sources_bucket, bucket_tag, tag, aspera_package_id, source_hashes):
    source_hashes_dict = {row.split(' ',1)[1]: row.split(' ',1)[0] for row in source_hashes if row}
    storage_client = storage.Client(project='idc-source-data')
    buckets = storage_client.list_buckets()
    aspera_files_basenames = [file['basename'] for file in aspera_files]

    sources = {}
    dst_bucket_name = sources_bucket.name.lower().replace('-', '_')
    for bucket in buckets:
        if dst_bucket_name  in bucket.name.lower().replace('-', '_') or \
                bucket_tag and bucket_tag in bucket.name.lower().replace('-', '_'):
            progresslogger.info(f'Processing bucket {bucket.name}')
            if bucket.name == 'idc-source-data-cmb':
                prefix = f'{tag}/'
                filter = ""
            elif bucket.name == 'cptac_pathology_source_data':
                prefix = f'v1/{tag}/'
                filter = ""
            elif bucket.name == 'idc-source-data-cmb':
                prefix = f'pathology-NLST_1225files/pathology-NLST_1225files/'
                filter = ""
            elif bucket.name == 'idc-source-icdc-glioma':
                prefix = ""
                filter = ""
            elif bucket.name == 'nlst_pathology_source_data':
                prefix = ""
                filter = ""
            elif bucket.name == 'idc_glioma_pathology_data':
                prefix = ""
                filter = ""
            else:
                prefix = f'{tag}/' if tag else ""
                filter = f'{aspera_package_id}/'

            blobs = bucket.list_blobs(prefix=prefix)
            # prefix = f'{tag if not tag else (tag+"/" if bucket_tag != "cptac" else "CPTAC-"+tag+"/")}'
            for blob in blobs:
                if filter in blob.name:
                    if bucket.name == 'cptac_pathology_source_data':
                        id = f'{tag}/{blob.name.split("/")[-1]}'
                    elif bucket.name == 'idc-source-data-cmb':
                        id = f'{tag}/{blob.name.split("/")[-1]}'
                    elif bucket.name == 'nlst_pathology_source_data':
                        id = f'{blob.name.split("pathology-NLST_1225files/", 1)[-1]}'
                    elif bucket.name in ('cmb_pathology_data', 'cptac_pathology_data'):
                        if aspera_package_id == 'cptac-ccrcc-da-path-nonccrcc':
                            id = f'data/{blob.name.split(aspera_package_id + "/")[-1]}'
                        else:
                            id = f'{tag}/{blob.name.split(aspera_package_id + "/")[-1]}'

                    else:
                        id = f'{blob.name.split(aspera_package_id + "/")[-1]}'
                    try:
                        sources[id] = \
                            {
                                "source_url": f'{bucket.name}/{blob.name}',
                                "hash": f'{b64decode(blob.md5_hash).hex()}'
                            }
                    except Exception as exc:
                        # THe blob doesn't have an md5 hash. Get it from the sums file.

                        try:
                            hash = next(hash for key, hash in source_hashes_dict.items() if key in blob.name)
                            sources[id] = \
                                {
                                    "source_url": f'{bucket.name}/{blob.name}',
                                    "hash": hash
                                }
                        except Exception as exc:
                            errlogger.error(f'No hash found for {blob.name}')
                            return []
    return sources

def main(args, download_slugs=[]):
    aspera_package_urls = get_aspera_package_urls().sort_values(by="Download_slug")
    for _, package in aspera_package_urls.iterrows():
        aspera_package_id = package['Download_slug']
        if aspera_package_id in args.skips:
            progresslogger.info(f'Skipping aspera package {aspera_package_id}')
        else:
            if args.download_slugs == [] or aspera_package_id in args.download_slugs:
                with temp_logger(package['Download_slug']):
                    dones = open(successlogger.handlers[0].baseFilename).read().splitlines()
                    if package["Download_slug"] in dones:
                        progresslogger.info(f'Package {aspera_package_id} previously processed')
                    else:
                        TCIA_collection_version = package['TCIA_collection_version']
                        IDC_collection_name = package['IDC_collection_name']
                        idc_collection_id = package['IDC_collection_id']
                        client = storage.Client(args.dst_project)
                        bucket_tag = bucket_collection_id(
                            idc_collection_id if idc_collection_id else package["TCIA_collection_id"])
                        # Bucket name length must be 63 or less, so we truncate the collection part
                        sources_bucket = client.bucket(f"{args.dst_bucket_prefix}{bucket_tag}"[:63-len(args.dst_bucket_suffix)] +
                                                   f"{args.dst_bucket_suffix}")
                        if IDC_collection_name in ['NLST', 'ICDC-Glioma']:
                            tag = ''
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
                            prefix = f'{tag}/v{TCIA_collection_version}/{aspera_package_id}'
                        else:
                            prefix = f'v{TCIA_collection_version}/{aspera_package_id}'
                        blobs = sources_bucket.list_blobs(prefix=prefix+'/', delimiter='/')
                        found_hash_map = False
                        for blob in blobs:
                            if blob.name.endswith("hash.map"):
                                progresslogger.info(f"Found hash map for {sources_bucket.name}/{prefix}")
                                found_hash_map = True
                                break
                        # for blob in blobs:
                        #     if blob.name.endswith(".sums") and (not blob.name.endswith("generated.sums") or not args.regen_generated_sums_file):
                        #         progresslogger.info(f"Found sums file {sources_bucket.name}/{blob.name}")
                        #         found_sums_file = True
                        #         break

                        if found_hash_map and not args.regen_hash_maps:
                            continue
                        else:
                            found_sums_file = False
                            blobs = sources_bucket.list_blobs(prefix=prefix + '/', delimiter='/')
                            # Find a sums file to use to as a source of hashes in case the actual source blobs in do not have md5 hashes
                            for blob in blobs:
                                if blob.name.endswith(".sums"):
                                    progresslogger.info(f"Found sums file {sources_bucket.name}/{blob.name}")
                                    found_sums_file = True
                                    break

                            if found_sums_file == False:
                                errlogger.error(f'No sums file found for {aspera_package_id}')
                                continue

                            source_hashes = blob.download_as_text().split('\n')

                            progresslogger.info(f"Did not find hash_map for {sources_bucket.name}/{prefix}")
                            if args.load_aspera_files and os.path.exists(
                                    f"{settings.LOG_DIR}/{package['Download_slug']}/{aspera_package_id}_files.json"):
                                with open(f"{settings.LOG_DIR}/{package['Download_slug']}/{aspera_package_id}_files.json") as f:
                                    aspera_files = json.load(f)
                            else:
                                manifest_params = get_blob_metadata_from_package(args, package, args.version)
                                aspera_files = manifest_params["aspera_files"]
                                with open(f"{settings.LOG_DIR}/{package['Download_slug']}/{aspera_package_id}_files.json", 'w') as f:
                                    json.dump(aspera_files, f)
                                progresslogger.info(manifest_params["logger_string"])

                            all_hashes = get_conversion_source_hashes(aspera_files, sources_bucket, bucket_tag, tag, aspera_package_id, source_hashes)
                            if all_hashes:
                                package_hashes = []
                                for aspera_file in aspera_files:
                                    if not aspera_file["path"].endswith(".sums"):
                                        try:
                                            # hash = all_hashes[aspera_file["path"][1:]]
                                            hash = all_hashes[aspera_file["path"].split('/',1)[-1]]
                                            package_hashes.append(hash)
                                            # package_hashes.append(next(hash for hash in all_hashes if aspera_file["path"][1:] == hash.split(" ")[1]))
                                        except:
                                            errlogger.error(f'No hash found for {aspera_file["path"].split("/",1)[-1]}')
                                sum_files_blob = sources_bucket.blob(f'{prefix}/hash.map')
                                sum_files_blob.upload_from_string(json.dumps(package_hashes))
                                progresslogger.info(f"Generated sums file for {sources_bucket.name}/{prefix}")
                                successlogger.info(aspera_package_id)
                            else:
                                progresslogger.info(f'Did not generate a hash map for {aspera_package_id}')
                                continue

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
    parser.add_argument("--download_slugs", default = ['cptac-brca-da-path'], help="Slugs to process; all if empty")
    parser.add_argument("--manifest_file_name", default= f'manifest_{strftime("%Y%m%d_%H%M%S", gmtime())}.txt')
    parser.add_argument("--only_idc_collections", default=False, help="Only include a collection if IDC already has it")
    parser.add_argument("--skips", default=['dlbcl-morphology-da-path'], help="TCIA_collection_ids to be skipped")
    parser.add_argument("--load_aspera_files", default=True)
    parser.add_argument("--regen_hash_maps", default=True, help="If true, regen existing generated.sums")
    args = parser.parse_args()
    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    # Default process ID
    args.id = 0


    main(args)
