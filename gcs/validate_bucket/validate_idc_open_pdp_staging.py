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
import os
import settings

from utilities.logging_config import successlogger, progresslogger, errlogger
from google.cloud import storage, bigquery

def get_expected_blobs_in_bucket(args):
    client = bigquery.Client()
    query = f"""
    SELECT distinct CONCAT(instance_uuid,'.dcm') as blob_name
    FROM `{settings.PDP_PROJECT}.idc_v{settings.CURRENT_VERSION}.auxiliary_metadata` 
    WHERE instance_revised_idc_version = {settings.CURRENT_VERSION}
    """
    query_job = client.query(query)  # Make an API request.
    query_job.result()  # Wait for the query to complete.

    # Get the destination table for the query results.
    # All queries write to a destination table. If a destination table is not
    # specified, the BigQuery populates it with a reference to a temporary
    # anonymous table after the query completes.
    destination = query_job.destination
    destination = client.get_table(destination)
    with open(args.expected_blobs, 'w') as f:
        for page in client.list_rows(destination, page_size=args.batch).pages:
            rows = [f'{row["blob_name"]}\n' for row in page]
            f.write(''.join(rows))


def get_found_blobs_in_bucket(args):
    client = storage.Client()
    bucket = client.bucket(args.bucket)
    page_token = ""
    # iterator = client.list_blobs(bucket, page_token=page_token, max_results=args.batch)
    iterator = client.list_blobs(bucket, versions=False, page_token=page_token, page_size=args.batch)
    with open(args.found_blobs, 'w') as f:
        for page in iterator.pages:
            blobs = [f'{blob.name}\n' for blob in page]
            f.write(''.join(blobs))


def check_all_instances(args):
    try:
        expected_blobs = set(open(args.expected_blobs).read().splitlines())
    except:
        get_expected_blobs_in_bucket(args)
        expected_blobs = set(open(args.expected_blobs).read().splitlines())
        # json.dump(psql_blobs, open(args.blob_names), 'w')

    try:
        found_blobs = set(open(args.found_blobs).read().splitlines())
    except:
        get_found_blobs_in_bucket(args)
        found_blobs = set(open(args.found_blobs).read().splitlines())
        # json.dump(psql_blobs, open(args.blob_names), 'w')
    if found_blobs == expected_blobs:
        successlogger.info(f"Bucket {args.bucket} has the correct set of blobs")
    else:
        errlogger.error(f"Bucket {args.bucket} does not have the correct set of blobs")

    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # parser.add_argument('--version', default=f'{settings.CURRENT_VERSION}')
    parser.add_argument('--version', default=settings.CURRENT_VERSION)
    parser.add_argument('--bucket', default='idc-open-pdp-staging')
    parser.add_argument('--dev_or_pub', default = 'pub', help='Validating a dev or pub bucket')
    parser.add_argument('--expected_blobs', default=f'{settings.LOG_DIR}/expected_blobs.txt', help='List of blobs names expected to be in above collections')
    parser.add_argument('--found_blobs', default=f'{settings.LOG_DIR}/found_blobs.txt', help='List of blobs names found in bucket')
    parser.add_argument('--batch', default=10000, help='Size of batch assigned to each process')
    parser.add_argument('--log_dir', default=f'/mnt/disks/idc-etl/logs/validate_open_buckets')

    args = parser.parse_args()
    check_all_instances(args)
