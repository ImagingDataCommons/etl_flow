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

from google.cloud import bigquery, storage
from utilities.logging_config import successlogger, progresslogger, errlogger
import settings
import argparse
import boto3

def get_redactions(args):
    client = bigquery.Client()

    query = f"""
    SELECT DISTINCT dev_bucket, pub_aws_bucket, CONCAT(se_uuid, '/', i_uuid, '.dcm') as blob_name
    FROM `{settings.DEV_MITIGATION_PROJECT}.mitigation.redactions`
    """

    try:
        results = [dict(row) for row in client.query(query).result()]
    except Exception as exc:
        errlogger.error(f'Error querying redactions table: {exc} ')
        exit(-1)

    return results


def delete_redactions(args):
    s3 = boto3.client('s3')
    # Get list of previously deleted blobs
    dones = set(open(f'{successlogger.handlers[0].baseFilename}').read().splitlines())
    instances = get_redactions(args)
    for instance in instances:
        try:
            s3.delete_object(Bucket=instance['pub_aws_bucket'], Key=instance["blob_name"])
            # s3.delete_object(Bucket='whc-temp1', Key=instance["blob_name"])
            successlogger.info(f'{instance["pub_aws_bucket"]}/{instance["blob_name"]}')
            # successlogger.info(f'whc-temp1/{instance["blob_name"]}')
        except Exception as e:
            print(f"Error deleting instance {instance}: {e}")

if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    delete_redactions(args)