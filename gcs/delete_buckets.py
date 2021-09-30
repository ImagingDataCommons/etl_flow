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

# Copy a bucket from the dev account to the production account (really from any account to any other account)

from google.cloud import storage
from subprocess import run, PIPE
from google.api_core.exceptions import Conflict
import sys
import argparse
from utilities.gcs_helpers import list_buckets

def delete_bucket(args, bucket):
    try:
        result = run(['gsutil', '-m', 'rm', '-r', f'gs://{bucket}'])
        print("   {} copied, results: {}".format(bucket, result), flush=True)
        if result.returncode:
            print('Copy {} failed: {}'.format(result.stderr), flush=True)
            return {"bucket": args.src_bucket_name, "status": -1}
        return {"bucket": bucket, "status": 0}
    except:
        print("Error in copying {}: {},{},{}".format(bucket, sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2]), file=sys.stdout, flush=True)
        # raise


def delete_buckets(args):
    bucket_objects = list_buckets(args.project)
    buckets = [bucket.id for bucket in bucket_objects]
    n = 1
    buckets_to_delete = [bucket for bucket in buckets if bucket.startswith(args.bucket_prefix)]
    for bucket in buckets_to_delete:
        print("Deleting bucket {}, {} of {}".format(bucket, n, len(buckets_to_delete)))
        delete_bucket(args, bucket)
        n += 1
    pass



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', default='canceridc-data')
    parser.add_argument('--bucket_prefix', default='idc-nlst-open')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)

    delete_buckets(args)
