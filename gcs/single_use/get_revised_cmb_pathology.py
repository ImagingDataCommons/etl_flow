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

import json
from io import StringIO
from subprocess import run
from utilities.logging_config import successlogger, progresslogger, warninglogger, errlogger
from google.cloud import storage
from base64 import b64decode
import hashlib
import pathlib

BATCH_SIZE = pow(2,20)

def get_revised_blobs(dst_bucket, collection_id, url, existing_blob_hashes):
    # Get list of (svs) files in package
    client = storage.Client()
    result = run(["ascli", "--progress-bar=no", "--format=json", "faspex5", "packages", "browse", f"--url={url}", collection_id],
                     capture_output=True)
    if result.stderr == b'':
        files = json.load(StringIO(result.stdout.decode()))
    else:
        errlogger.error(f'Failed to get new file list')
        exit -1

    dones = open(successlogger.handlers[0].baseFilename).read().splitlines()

    for file in files:

        if file["path"] not in dones:
            if file["basename"] in existing_blob_hashes:
                # # If we have this file already, see if it's hash has changed
                # with open(f'/mnt/disks/idc-etl/aspera/{file["basename"]}', "rb") as f:
                #     m = hashlib.md5()
                #     while batch := f.read(BATCH_SIZE):
                #         m.update(batch)
                #     hash = m.hexdigest()
                #     if hash == existing_blob_hashes[file["basename"]]:
                #         progresslogger.info(f'{file["path"]} unchanged')
                #         successlogger.info(file['path'])
                #     else:
                #         progresslogger.info(f'{file["path"]} revised')
                #         res = run(['gsutil', '-m', 'cp', f'/mnt/disks/idc-etl/aspera/{file["basename"]}', f'gs://{dst_bucket}/{file["path"]}'])
                #         # client.bucket(dst_bucket).blob(file["path"]).upload_from_file(f)
                #         if res.stderr == b'':
                #             successlogger.info(file['path'])
                #         else:
                #             errlogger.error(f'Transfer to gcs failed: {file["path"]}')
                progresslogger.info(f'{file["path"]} skipped')

            else:
                progresslogger.info(f'{file["path"]} is new')
                # Download the file to disk
                result = run(
                    ["ascli", "--progress-bar=no", "--format=json", f'--to-folder=/mnt/disks/idc-etl/aspera', "faspex5",
                     "packages", "receive",
                     f"--url={url}", file["path"]])

                # Copy to GCS
                res = run(['gsutil', '-m', 'cp', f'/mnt/disks/idc-etl/aspera/{file["basename"]}',
                           f'gs://{dst_bucket}/{file["path"]}'])
                # with open(f'/mnt/disks/idc-etl/aspera/{file["basename"]}', "rb") as f:
                #     client.bucket(dst_bucket).blob(file["path"]).upload_from_file(f)
                if res.stderr is None:
                    successlogger.info(file['path'])
                else:
                    errlogger.error(f'Transfer to gcs failed: {file["path"]}')

                # Delete the file
                run(["rm", f'/mnt/disks/idc-etl/aspera/{file["basename"]}'])
        else:
            progresslogger.info(f'{file["path"]} previously processed')

def get_existing_blob_hashes(collection_id, original_bucket):
    client = storage.Client()
    blobs = {}
    bucket = client.bucket(original_bucket)
    for blob in bucket.list_blobs(prefix=f'{collection_id}/'):
        blobs[blob.name.split('/')[-1]] = b64decode(blob.md5_hash).hex()
    return blobs

    pass


def get_collection(collection_id, url):
    existing_blob_hashes = get_existing_blob_hashes(collection_id, 'idc-source-data-cmb')
    get_revised_blobs('idc-source-data-cmb-20240828', collection_id, url, existing_blob_hashes)


def get_package(dst_dir, collection_id, url):
    try:
        result = run(["ascli", "--progress-bar=no", "--format=json", f'--to-folder={dst_dir}', "faspex5", "packages",
            "receive", f"--url={url}", collection_id])
    except Exception as exc:
        pass


if __name__ == '__main__':
    collections = {
        "CMB-AML": 'https://faspex.cancerimagingarchive.net/aspera/faspex?context=eyJyZXNvdXJjZSI6InBhY2thZ2VzIiwidHlwZSI6ImV4dGVybmFsX2Rvd25sb2FkX3BhY2thZ2UiLCJpZCI6Ijk4NiIsInBhc3Njb2RlIjoiYjc4NzI5ODYxMzAxYzE4ODBkOWExNmUxM2Y1YTkxN2RmYjQ2ZTc1YiIsInBhY2thZ2VfaWQiOiI5ODYiLCJlbWFpbCI6ImhlbHBAY2FuY2VyaW1hZ2luZ2FyY2hpdmUubmV0In0=',
        "CMB-BRCA": 'https://faspex.cancerimagingarchive.net/aspera/faspex/public/package?context=eyJyZXNvdXJjZSI6InBhY2thZ2VzIiwidHlwZSI6ImV4dGVybmFsX2Rvd25sb2FkX3BhY2thZ2UiLCJpZCI6IjkyOSIsInBhc3Njb2RlIjoiZTQxMDI2MmVmN2E3ODNlMDZhN2IyMWFjMzUyYjY1NTNiNDg3MGRiOCIsInBhY2thZ2VfaWQiOiI5MjkiLCJlbWFpbCI6ImhlbHBAY2FuY2VyaW1hZ2luZ2FyY2hpdmUubmV0In0=',
        "CMB-CRC": 'https://faspex.cancerimagingarchive.net/aspera/faspex/public/package?context=eyJyZXNvdXJjZSI6InBhY2thZ2VzIiwidHlwZSI6ImV4dGVybmFsX2Rvd25sb2FkX3BhY2thZ2UiLCJpZCI6Ijk0OSIsInBhc3Njb2RlIjoiMjFiNmU2ZjIzNDQ2ODhlYTk1YmE1ZjFjOTUzYTNjZDA5ZWY0M2IwYSIsInBhY2thZ2VfaWQiOiI5NDkiLCJlbWFpbCI6ImhlbHBAY2FuY2VyaW1hZ2luZ2FyY2hpdmUubmV0In0=',
        "CMB-GEC": 'https://faspex.cancerimagingarchive.net/aspera/faspex/public/package?context=eyJyZXNvdXJjZSI6InBhY2thZ2VzIiwidHlwZSI6ImV4dGVybmFsX2Rvd25sb2FkX3BhY2thZ2UiLCJpZCI6IjkzMSIsInBhc3Njb2RlIjoiZGZhYmY5ZWVhNWQ3OWY1ZjMzODVkZjcxZTI3NGQ5OTBhOTc4YzNkNCIsInBhY2thZ2VfaWQiOiI5MzEiLCJlbWFpbCI6ImhlbHBAY2FuY2VyaW1hZ2luZ2FyY2hpdmUubmV0In0=',
        "CMB-LCA": 'https://faspex.cancerimagingarchive.net/aspera/faspex?context=eyJyZXNvdXJjZSI6InBhY2thZ2VzIiwidHlwZSI6ImV4dGVybmFsX2Rvd25sb2FkX3BhY2thZ2UiLCJpZCI6Ijk4NyIsInBhc3Njb2RlIjoiYTRkNDc0ZGEyYmMyYmUyOTAxODIxYjc0OWMwYzQ1ZmI4NWIzNWE0NCIsInBhY2thZ2VfaWQiOiI5ODciLCJlbWFpbCI6ImhlbHBAY2FuY2VyaW1hZ2luZ2FyY2hpdmUubmV0In0=',
        "CMB-MEL": 'https://faspex.cancerimagingarchive.net/aspera/faspex?context=eyJyZXNvdXJjZSI6InBhY2thZ2VzIiwidHlwZSI6ImV4dGVybmFsX2Rvd25sb2FkX3BhY2thZ2UiLCJpZCI6Ijk4OCIsInBhc3Njb2RlIjoiNWFhZDM0Y2RiYTgzNTFhOTgyOWViYzU4MjQ5ZTY0NzE0MDZjZWViZSIsInBhY2thZ2VfaWQiOiI5ODgiLCJlbWFpbCI6ImhlbHBAY2FuY2VyaW1hZ2luZ2FyY2hpdmUubmV0In0=',
        "CMB-MML": "https://faspex.cancerimagingarchive.net/aspera/faspex?context=eyJyZXNvdXJjZSI6InBhY2thZ2VzIiwidHlwZSI6ImV4dGVybmFsX2Rvd25sb2FkX3BhY2thZ2UiLCJpZCI6Ijk5MCIsInBhc3Njb2RlIjoiMTA1MTkyNzhkZWIwYjE0ZjE0NjA5ZTk1Y2EzNmY0NWJjYWZmMmEwMyIsInBhY2thZ2VfaWQiOiI5OTAiLCJlbWFpbCI6ImhlbHBAY2FuY2VyaW1hZ2luZ2FyY2hpdmUubmV0In0=",
        "CMB-OV": "https://faspex.cancerimagingarchive.net/aspera/faspex/public/package?context=eyJyZXNvdXJjZSI6InBhY2thZ2VzIiwidHlwZSI6ImV4dGVybmFsX2Rvd25sb2FkX3BhY2thZ2UiLCJpZCI6IjkzNSIsInBhc3Njb2RlIjoiYTdiNmVhMjkzMmRiNjQzMjE1ZjFmZGQ1NmNkNDY1OGQxMTNiYzQ1YyIsInBhY2thZ2VfaWQiOiI5MzUiLCJlbWFpbCI6ImhlbHBAY2FuY2VyaW1hZ2luZ2FyY2hpdmUubmV0In0=",
        "CMB-PCA": "https://faspex.cancerimagingarchive.net/aspera/faspex?context=eyJyZXNvdXJjZSI6InBhY2thZ2VzIiwidHlwZSI6ImV4dGVybmFsX2Rvd25sb2FkX3BhY2thZ2UiLCJpZCI6Ijk4OSIsInBhc3Njb2RlIjoiNTlhNmIzZjAwNDU4MTIyNGM4Mzg3YjhiNmQ5MDUxMTMyOGQxM2U3YiIsInBhY2thZ2VfaWQiOiI5ODkiLCJlbWFpbCI6ImhlbHBAY2FuY2VyaW1hZ2luZ2FyY2hpdmUubmV0In0="
    }
    mount_point = '/mnt/disks/idc-etl/aspera/idc-source-data-cmb-20240828-mount'
    dst_bucket = 'idc-source-data-cmb-20240828'
    # try:
    #     # gcsfuse mount the bucket
    #     pathlib.Path(mount_point).mkdir( exist_ok=True)
    #     run(['gcsfuse', dst_bucket, mount_point])
    #     # run(['gcsfuse', '--implicit-dirs',dst_bucket, mount_point])
    #     # run(['gcsfuse', f'--log-file=/mnt/disks/idc-etl/aspera/gcsfuse.log', "--log-severity=trace", "--temp-dir=/mnt/disks/idc-etl/aspera/tmp", dst_bucket, mount_point])
    #     # run(['gcsfuse', "--temp-dir=/mnt/disks/idc-etl/aspera/tmp", '--implicit-dirs', dst_bucket, mount_point])
    #     for collection_id, url in collections.items():
    #         progresslogger.info(f'Processing collection {collection_id}')
    #         # dest_dir = f'{mount_point}/{collection_id}'
    #         # pathlib.Path(dest_dir).mkdir(exist_ok=True)
    #         get_package(mount_point, collection_id, url)
    # finally:
    #     # Always unmount
    #     run(['fusermount', '-u', mount_point])

    for collection_id, url in collections.items():
        progresslogger.info(f'Processing collection {collection_id}')
        # dest_dir = f'{mount_point}/{collection_id}'
        # pathlib.Path(dest_dir).mkdir(exist_ok=True)
        get_collection(collection_id, url)
