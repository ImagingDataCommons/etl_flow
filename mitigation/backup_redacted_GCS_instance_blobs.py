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

# Backup GCS instances to be redacted to a bucket in default project.
import settings
import argparse
import json
from google.cloud import bigquery, storage
from utilities.logging_config import successlogger, progresslogger, errlogger

def get_redactions(args):
    client = bigquery.Client()
    query = f"""
    SELECT DISTINCT collection_id, pub_gcs_bucket, CONCAT(se_uuid, '/', i_uuid, '.dcm') blob_name
    FROM `{settings.DEV_MITIGATION_PROJECT}.m{settings.MITIGATION_VERSION}.redactions`
    """

    try:
        results = [dict(row) for row in client.query(query).result()]
    except Exception as exc:
        errlogger.error(f'Error querying redactions table: {exc} ')
        exit(-1)

    return results

def backup_redactions(args):
    client = storage.Client()
    instances = get_redactions(args)

    for instance in instances:
        src_bucket = client.bucket(instance['pub_gcs_bucket'])
        src_blob = src_bucket.blob(f'{instance["blob_name"]}')
        trg_bucket = client.bucket(args.trg_bucket)

        dst_blob = trg_bucket.blob(instance["blob_name"])
        rewrite_token = False
        while True:
            try:
                rewrite_token, bytes_rewritten, total_bytes = dst_blob.rewrite(
                    src_blob, token=rewrite_token
                )
                if not rewrite_token:
                    successlogger.info(f'{instance["blob_name"]}')
                    break
            except AttributeError as exc:
                errlogger.warning(
                    f"p{args.id}: Trying to create bucket: {exc}; attempt {i}\n")
                sleep(1)
            except TooManyRequests as exc0:
                errlogger.warning(
                    f"p{args.id}: Blob: Too many requests: {repr(exc0)};  {exc0}")
                sleep(1)
            except ServiceUnavailable as exc0:
                errlogger.warning(
                    f"p{args.id}: Blob: Service unavailable: {repr(exc0)};  {exc0}")
                sleep(1)
            except NotFound as exc:
                errlogger.error(
                    f"p{args.id}: Blob: Source blob {row['bucket']}/{blob_id} not found")
                break

                break
        successlogger.info(instance['blob_name'] + ' copied to ' + args.trg_bucket)

    pass

if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()
    parser.add_argument('--trg_bucket', default=f'm{settings.MITIGATION_VERSION}_redacted_instances', help='Bucket to which to backup redacted instances')

    args = parser.parse_args()

    backup_redactions(args)

