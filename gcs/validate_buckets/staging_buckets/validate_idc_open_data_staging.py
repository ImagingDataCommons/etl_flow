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

"""
Validate that th idc-open-pdp-staging bucket holds the correct set of instance blobs
"""

import argparse
import json
import settings
import pandas as pd
from base64 import b64decode
from utilities.logging_config import successlogger, progresslogger, errlogger
from google.cloud import storage, bigquery

def get_expected_blobs_in_bucket(args):
    client = bigquery.Client()
    query = f"""
    SELECT distinct CONCAT(series_uuid, '/', instance_uuid,'.dcm') as blob_name, instance_hash AS md5_hash
    FROM `{settings.PDP_PROJECT}.idc_v{settings.CURRENT_VERSION}.auxiliary_metadata` 
    WHERE series_revised_idc_version = {settings.CURRENT_VERSION}
--    AND split(gcs_url,'/')[offset(2)] = 'idc-open-data'
    AND gcs_bucket = 'idc-open-data'
    """

    df = bigquery.Client().query(query).to_dataframe()
    df.to_csv(args.expected_blobs, index=False)



def get_found_blobs_in_bucket(args):
    client = storage.Client()
    bucket = client.bucket(args.bucket)
    blobs = bucket.list_blobs()
    data = []
    for blob in blobs:
        data.append({
            'blob_name': blob.name,
            'md5_hash': b64decode(blob.md5_hash).hex() if blob.md5_hash else ""
        })
    df = pd.DataFrame(data)
    df.to_csv(args.found_blobs, index=False)


def check_all_instances(args):
    try:
        expected_data = pd.read_csv(args.expected_blobs)
    except:
        get_expected_blobs_in_bucket(args)
        expected_data = pd.read_csv(args.expected_blobs)

    try:
        found_data = pd.read_csv(args.found_blobs)
    except:
        get_found_blobs_in_bucket(args)
        found_data = pd.read_csv(args.found_blobs)

    not_expected= expected_data[~expected_data['blob_name'].isin(found_data['blob_name'])]
    not_found = found_data[~found_data['blob_name'].isin(expected_data['blob_name'])]

    if len(not_found):
        errlogger.error(f"Expected blobs were not found in bucket {args.bucket}")
    if len(not_expected):
        errlogger.error(f"Found blobs were not expected in bucket {args.bucket}")
    if len(not_found) == 0 and len(not_expected) == 0:
        successlogger.info(f"Bucket {args.bucket} has the correct set of blobs")
        # We now test whether the hashes match
        merged = pd.merge(expected_data, found_data, on="blob_name", how="inner")
        non_matching_hashes = merged[merged['md5_hash_x'] != merged['md5_hash_y']]
        non_null_non_matching_hashes = non_matching_hashes.dropna(subset=['md5_hash_y'])
        if len(non_matching_hashes):
            progresslogger.info(f'There are {len(non_matching_hashes)} non-matching hashes')
            progresslogger.info(f'{len(non_null_non_matching_hashes)} of these are not Nulls')



    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # parser.add_argument('--version', default=f'{settings.CURRENT_VERSION}')
    parser.add_argument('--version', default=settings.CURRENT_VERSION)
    parser.add_argument('--bucket', default='idc-open-data-staging')
    parser.add_argument('--dev_or_pub', default = 'pub', help='Validating a dev or pub bucket')
    parser.add_argument('--expected_blobs', default=f'{settings.LOG_DIR}/expected_blobs.txt', help='List of blobs names expected to be in above collections')
    parser.add_argument('--found_blobs', default=f'{settings.LOG_DIR}/found_blobs.txt', help='List of blobs names found in bucket')
    parser.add_argument('--batch', default=1000, help='Size of batch assigned to each process')
    parser.add_argument('--log_dir', default=f'/mnt/disks/idc-etl/logs/validate_open_buckets')

    args = parser.parse_args()
    print(f'args: {json.dumps(args.__dict__, indent=2)}')
    check_all_instances(args)
