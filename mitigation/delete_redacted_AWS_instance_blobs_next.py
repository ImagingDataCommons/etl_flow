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
import os

import boto3
import json
from botocore.exceptions import ClientError


def get_bucket_names():
    client = bigquery.Client()

    query = f"""
    SELECT DISTINCT pub_aws_bucket bucket_name
    FROM `{settings.DEV_MITIGATION_PROJECT}.m{settings.MITIGATION_VERSION}.redactions`
    """

    try:
        results = [dict(row) for row in client.query(query).result()]
    except Exception as exc:
        errlogger.error(f'Error querying redactions table: {exc} ')
        exit(-1)

    return results

def get_redactions(bucket_name):
    client = bigquery.Client()

    query = f"""
    SELECT DISTINCT dev_bucket, pub_aws_bucket, CONCAT(se_uuid, '/', i_uuid, '.dcm') as blob_name
    FROM `{settings.DEV_MITIGATION_PROJECT}.m{settings.MITIGATION_VERSION}.redactions`
    WHERE pub_aws_bucket = '{bucket_name}'
    """

    try:
        results = [dict(row) for row in client.query(query).result()]
    except Exception as exc:
        errlogger.error(f'Error querying redactions table: {exc} ')
        exit(-1)

    return results

def delete_redactions(bucket_name):
    s3 = boto3.client('s3')
    # Get list of previously deleted blobs
    dones = set(open(f'{successlogger.handlers[0].baseFilename}').read().splitlines())
    instances = get_redactions(bucket_name)
    for instance in instances:
        if f'{instance["pub_aws_bucket"]}/{instance["blob_name"]}' not in dones:
            try:
                s3.delete_object(Bucket=instance['pub_aws_bucket'], Key=instance["blob_name"])
                # s3.delete_object(Bucket='whc-temp1', Key=instance["blob_name"])
                successlogger.info(f'{instance["pub_aws_bucket"]}/{instance["blob_name"]}')
                # successlogger.info(f'whc-temp1/{instance["blob_name"]}')
            except Exception as e:
                print(f"Error deleting instance {instance}: {e}")

def temporarily_allow_deletion(bucket_name, user_arn):
    s3_client = boto3.client('s3')

    # 1. Retrieve the current bucket policy
    try:
        response = s3_client.get_bucket_policy(Bucket=bucket_name)
        existing_policy = response.get('Policy', {})
        new_policy = existing_policy.get('Statement', [])
    except ClientError as e:
        print(f"Error retrieving policy: {e}")
        return

    # 2. Modify the policy: Remove 's3:DeleteObject' from any Deny statements
    for statement in new_policy.get('Statement', []):
        if statement.get('Effect') == 'Deny':
            actions = statement.get('Action', [])

            # Handle if Action is a list
            if isinstance(actions, list) and 's3:DeleteObject' in actions:
                actions.remove('s3:DeleteObject')
                statement['Action'] = actions

            # Handle if Action is a single string
            elif actions == 's3:DeleteObject':
                statement['Action'] = []

    # 3. Modify the policy: Append the new Allow statement
    allow_statement = {
        "Effect": "Allow",
        "Principal": {
            "AWS": user_arn
        },
        "Action": [
            "s3:DeleteObject"
        ],
        "Resource": [
            f"arn:aws:s3:::{bucket_name}",
            f"arn:aws:s3:::{bucket_name}/*"
        ]
    }
    new_policy['Statement'].append(allow_statement)

    # 4. Upload the updated policy back to S3
    try:
        s3_client.put_bucket_policy(
            Bucket=bucket_name,
            Policy=json.dumps(new_policy)
        )
        print("Successfully updated bucket policy to allow deletion.")
    except ClientError as e:
        print(f"Error updating policy: {e}")


def restore_bucket_permissions(bucket_name, existing_policy):
    s3_client = boto3.client('s3')
    try:
        s3_client.put_bucket_policy(
            Bucket=bucket_name,
            Policy=json.dumps(existing_policy)
        )
        print("Successfully restored bucket policy to allow deletion.")
    except ClientError as e:
        print(f"Error updating policy: {e}")

if __name__ == "__main__":
    os.environ["AWS_ACCESS_KEY_ID"] = settings.AWS_ACCESS_KEY_ID
    os.environ["AWS_SECRET_ACCESS_KEY"] = settings.AWS_SECRET_ACCESS_KEY

    USER_ARN = settings.AWS_USER_ARN
    bucket_names = [row['bucket_name'] for row in get_bucket_names()]
    for bucket_name in bucket_names:
        existing_policy = temporarily_allow_deletion(bucket_name, USER_ARN)
        delete_redactions(bucket_name)
        restore_bucket_permissions(bucket_name, existing_policy)