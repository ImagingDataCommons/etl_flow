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

# Validate that redacted blobs were deleted from both dev (idc-dev-etl) and public bucket
import argparse
import sys
import json

from utilities.logging_config import successlogger, progresslogger, errlogger

import google
from google.cloud import storage, bigquery
from google.auth.transport import requests
import settings

from utilities.logging_config import successlogger, progresslogger, errlogger



def get_redactions(version):
    client = bigquery.Client()
    query = f"""
    SELECT DISTINCT dev_bucket, pub_gcs_bucket, se_uuid, i_uuid
    FROM `{settings.DEV_MITIGATION_PROJECT}.m{settings.MITIGATION_VERSION}.redactions`
    """

    try:
        results = [dict(row) for row in client.query(query).result()]
    except Exception as exc:
        errlogger.error(f'Error querying redactions table: {exc} ')
        exit(-1)

    return results

def delete_redactions(args):
    client = storage.Client()
    # Get list of previously deleted blobs
    dones = set(open(f'{successlogger.handlers[0].baseFilename}').read().splitlines())
    instances = get_redactions(args)
    legacy_bucket_name = 'public-datasets-idc'
    legacy_bucket = client.bucket(legacy_bucket_name)

    # Delete the blob from the public GCS bucket
    for instance in instances:
        blob_name = f'{instance["se_uuid"]}/{instance["i_uuid"]}.dcm'
        bucket_name = instance['pub_gcs_bucket']
        bucket = client.bucket(bucket_name)
        if f'{bucket_name}/{blob_name}' not in dones:
            if bucket.blob(blob_name).exists():
                bucket.blob(blob_name).delete()
                successlogger.info(f'{bucket_name}/{blob_name}')
            if bucket_name == 'idc-open-data':
                if legacy_bucket.blob(blob_name).exists():
                    legacy_bucket.blob(blob_name).delete()
                    successlogger.info(f'{legacy_bucket_name}/{blob_name}')


if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()
    parser.add_argument('--delete_entire_series', default="True", help='Delete entire series from the archive bucket if True')
    args = parser.parse_args()

    delete_redactions(args)


