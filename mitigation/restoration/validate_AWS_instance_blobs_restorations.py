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

import sys
from google.cloud import bigquery, storage

from utilities.logging_config import successlogger, progresslogger, errlogger
import settings
import argparse
import boto3
from botocore.exceptions import ClientError
from botocore import UNSIGNED
from botocore.config import Config

def get_unredactions(args):
    client = bigquery.Client()

    query = f"""
    SELECT DISTINCT dev_bucket, pub_aws_bucket, CONCAT(se_uuid, '/', i_uuid, '.dcm') as blob_name
    FROM `{settings.DEV_MITIGATION_PROJECT}.m{settings.MITIGATION_VERSION}.unredactions`
    """

    try:
        results = [dict(row) for row in client.query(query).result()]
    except Exception as exc:
        errlogger.error(f'Error querying redactions table: {exc} ')
        exit(-1)

    return results


def validate_unredactions(args):
    s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))

    dones = set(open(f'{successlogger.handlers[0].baseFilename}').read().splitlines())
    instances = get_unredactions(args)
    for instance in instances:
        if f'{instance["pub_aws_bucket"]}/{instance["blob_name"]}' not in dones:
            try:
                s3_client.head_object(Bucket=instance['pub_aws_bucket'], Key=instance["blob_name"])
                successlogger.info(f'{instance["pub_aws_bucket"]}/{instance["blob_name"]}')
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    errlogger.error(f'{instance["pub_aws_bucket"]}/{instance["blob_name"]} not found')
                else:
                    # Handle other potential errors (e.g., permissions)
                    errlogger.error(f"An error occurred: {e}")
                    return False
        else:
            progresslogger.info(f'{instance["pub_aws_bucket"]}/{instance["blob_name"]} previously validated')

if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    validate_unredactions(args)

    errors = set(open(f'{errlogger.handlers[0].baseFilename}').read().splitlines())
    if errors:
        sys.exit(1)
    else:
        sys.exit(0)


