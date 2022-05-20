#
# Copyright 2015-2022, Institute for Systems Biology
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

# This script validates that a subset of instance indexes resolve to the correct GCS URL.
import sys
import time
import argparse
import settings

from google.cloud import bigquery
from utilities.tcia_helpers import get_url
from utilities.logging_config import successlogger, progresslogger, errlogger


def get_sample(args):
    client = bigquery.Client()
    if args.version <= 2:
        query = f"""
        SELECT
            tcia_api_collection_id, gcs_url, instance_hash, instance_uuid
        FROM
            `bigquery-public-data.idc_v{args.version}.auxiliary_metadata`
        WHERE
            instance_uuid LIKE '{args.starts_with}%'
        """
    else:
        query = f"""
        SELECT
            tcia_api_collection_id, gcs_url, instance_hash, instance_uuid
        FROM
            `bigquery-public-data.idc_v{args.version}.auxiliary_metadata`
        WHERE
            instance_uuid LIKE '{args.starts_with}%'
        AND instance_revised_idc_version = {args.version}
        """

    results = client.query(query).result()
    return results

def find_in_manifest(args, uuid):
    for part in range(5):
        client = bigquery.Client()
        query = f"""
            SELECT * 
            FROM `idc-dev-etl.whc_dev.idc_v1_v2_instance_revision_manifest_{part}`
            WHERE GUID like '%{args.starts_with}'
            """
        results = client.query(query).result()
        data = [{'GUID': row['GUID'], 'md5': row['md5'], 'url': row['url']} for row in results]
        if len(data):
            return (part, data)
    return (0, 'Not found')

def validate_version(args):
    server = 'https://nci-crdc.datacommons.io/ga4gh/drs/v1/objects/dg.4DFC'
    results = get_sample(args)
    start = time.time()
    progresslogger.info(f'Validate {results.total_rows} indexes; ')
    successes = 0
    failures = 0
    for row in results:
        progresslogger.info(f'Validating {row["tcia_api_collection_id"]}:{row["instance_uuid"]}')
        url = f'{server}/{row["instance_uuid"]}'
        try:
            result = get_url(url)
            try:
                assert len(result.json()['access_methods']) == 1
                assert result.json()["access_methods"][0]["access_url"]["url"] == \
                    row['gcs_url']
                assert result.json()["access_methods"][0]["type"] == 'gs'
                successlogger.info(f'{row["instance_uuid"]}')
                successes += 1
            except Exception as exc:
                # part, data = find_in_manifest(args, row["instance_uuid"])
                errlogger.error(f'{row["instance_uuid"]}')
                failures += 1
                progresslogger.info('Validation failure on %s: %s', row["tcia_api_collection_id"], row["instance_uuid"])
                progresslogger.info('access_url = %s', result.json()["access_methods"][0]["access_url"]["url"])
                progresslogger.info('Expected %s',row["gcs_url"])
                # errlogger.error('Found in manifest part %s: %s\n', part, data)
        except Exception as exc:
            errlogger.errorr(f'Server query failure on {row["instance_uuid"]}')
            failures += 1
            pass
    duration = (time.time()-start)
    rate = results.total_rows/duration
    progresslogger.info(f'Validated {results.total_rows} indexes; successes: {successes}; failures: {failures}; rate: {rate:.2f}/s')

if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--version', type=int, default=f'{settings.CURRENT_VERSION}', help='IDC version to validate')
    parser.add_argument('--starts_with', help='Identify UUIDs that start with this pattern')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    validate_version(args)