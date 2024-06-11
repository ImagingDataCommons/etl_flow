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

# Delete redacted blobs from both dev (idc-dev-etl) and public bucket
import argparse
from utilities.logging_config import successlogger, progresslogger, errlogger

import google
from google.cloud import storage, bigquery
from google.auth.transport import requests
import settings

from utilities.logging_config import successlogger, progresslogger, errlogger



def get_redactions(version):
    client = bigquery.Client()
    query = f"""
    SELECT DISTINCT ingestion_url
    FROM `{settings.DEV_PROJECT}.mitigation.redactions`
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
    for instance in instances:
        # Only instance data sourced by IDC has an ingestion_url and is deleted.
        if instance['ingestion_url']:
            # Delete the blob from the ingestion bucket
            bucket_name, blob_name = instance['ingestion_url'].split('/',3)[2:]
            bucket = client.bucket(bucket_name)
            if f'{bucket_name}/{blob_name}' not in dones:
                if bucket.blob(blob_name).exists():
                    try:
                        bucket.blob(blob_name).delete()
                        successlogger.info(f'{bucket_name}/{blob_name}')
                    except Exception as exc:
                        errlogger(f'{bucket_name}/{blob_name}: {exc}')
                else:
                    errlogger.error(f'{bucket_name}/{blob_name} does not exist in source bucket')
            else:
                progresslogger.info(f'{bucket_name}/{blob_name} previously deleted')

if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    delete_redactions(args)

