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

# This script validates that a subset of instance indexes resolve to the correct GCS URL.
import sys
from google.cloud import bigquery
from utilities.tcia_helpers import get_url
from python_settings import settings
import argparse

def get_sample(args):
    client = bigquery.Client()
    query = f"""
    SELECT
        tcia_api_collection_id, gcs_url, instance_hash, instance_uuid
    FROM
        `bigquery-public-data.idc_v{args.version}.auxiliary_metadata`
    WHERE
        instance_uuid LIKE '000%'

    """

    results = client.query(query).result()
    return results

def find_in_manifest(args, uuid):
    for part in range(5):
        client = bigquery.Client()
        query = f"""
            SELECT * 
            FROM `idc-dev-etl.whc_dev.idc_v1_v2_instance_revision_manifest_{part}`
            WHERE GUID like '%{uuid}'
            """
        results = client.query(query).result()
        data = [{'GUID': row['GUID'], 'md5': row['md5'], 'url': row['url']} for row in results]
        if len(data):
            return (part, data)
    return (0, 'Not found')

def validate_version(args):
    results = get_sample(args)
    for row in results:
        server = 'https://nci-crdc.datacommons.io/ga4gh/drs/v1/objects/dg.4DFC'
        url = f'{server}/{row["instance_uuid"]}'
        try:
            result = get_url(url)
            try:
                assert len(result.json()['access_methods']) == 1
                assert result.json()["access_methods"][0]["access_url"]["url"] == \
                    row['gcs_url']
                assert result.json()["access_methods"][0]["type"] == 'gs'
                print(f'Validated {row["instance_uuid"]}')
            except Exception as exc:
                part, data = find_in_manifest(args, row["instance_uuid"])
                print(f'\t\tValidation failure on {row["tcia_api_collection_id"]}: {row["instance_uuid"]}')
                print(f'\t\taccess_url = {result.json()["access_methods"][0]["access_url"]["url"]}')
                print(f'\t\tExpected {row["gcs_url"]}')
                print(f'\t\tFound in manifest part {part}: {data}\n')
        except Exception as exc:
            print(f'Server query failure on {row["instance_uuid"]}')
            pass



if __name__ == '__main__':
    parser =argparse.ArgumentParser()

    parser.add_argument('--version', default=2, help='IDC version to validate')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    validate_version(args)