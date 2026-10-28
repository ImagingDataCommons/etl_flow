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
from collections import Counter

def get_redactions(args):
    client = bigquery.Client()

    query = f"""
    SELECT DISTINCT dev_bucket, pub_aws_bucket, CONCAT(se_uuid, '/', i_uuid, '.dcm') as blob_name
    FROM `{settings.DEV_MITIGATION_PROJECT}.m{settings.MITIGATION_VERSION}.redactions`
    """

    try:
        results = [dict(row) for row in client.query(query).result()]
    except Exception as exc:
        errlogger.error(f'Error querying redactions table: {exc} ')
        exit(-1)

    return results


def delete_redactions(args):
    session = boto3.Session(profile_name='sandbox-mfa')
    s3 = session.client('s3')
    creds = s3._request_signer._credentials  # credentials this client actually signs with
    print("Signing key prefix:", creds.access_key[:4], "| token:", bool(creds.token))

    all_instances = get_redactions(args)
    print("Rows per bucket:", Counter(i['pub_aws_bucket'] for i in all_instances))

    instances = [i for i in all_instances if i['pub_aws_bucket'] == 'idc-open-data']
    print("Deleting", len(instances), "instances from idc-open-data")

    try:
        dones = set(open(successlogger.handlers[0].baseFilename).read().splitlines())
    except FileNotFoundError:
        dones = set()

    errors = 0
    for instance in instances:
        if instance['blob_name'] not in dones:
            bucket, key = instance['pub_aws_bucket'], instance['blob_name']
            if f'{bucket}/{key}' in dones:
                continue
            try:
                s3.delete_object(Bucket=bucket, Key=key)
                successlogger.info(f'{bucket}/{key}')
            except Exception as e:
                print(f'Error deleting {bucket}/{key}: {e}')
                errors += 1
                if errors >= 5:
                    print('Stopping after 5 errors')
                    break
        else:
            progresslogger.info(f'{bucket}/{key} already deleted')
        #
    # # Get list of previously deleted blobs
    # dones = set(open(f'{successlogger.handlers[0].baseFilename}').read().splitlines())
    # instances = get_redactions(args)
    # print(boto3.client('sts').get_caller_identity()['Arn'])
    # for instance in instances:
    #     try:
    #         s3.delete_object(Bucket=instance['pub_aws_bucket'], Key=instance["blob_name"])
    #         # s3.delete_object(Bucket='whc-temp1', Key=instance["blob_name"])
    #         successlogger.info(f'{instance["pub_aws_bucket"]}/{instance["blob_name"]}')
    #         # successlogger.info(f'whc-temp1/{instance["blob_name"]}')
    #     except Exception as e:
    #         print(f"Error deleting instance {instance}: {e}")

if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    delete_redactions(args)