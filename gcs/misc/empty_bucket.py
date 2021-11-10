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

# Delete all blobs from some bucket

from google.cloud import storage
from subprocess import run, PIPE
from google.api_core.exceptions import Conflict
import sys
import argparse
from utilities.gcs_helpers import list_buckets

def empty_bucket(args):
    try:
        result = run(['gsutil', '-m', '-u', f'{args.project}', 'rm', f'gs://{args.bucket}/*'])
        print("   {} emptied, results: {}".format(args.bucket, result), flush=True)
        if result.returncode:
            print('Copy {} failed: {}'.format(result.stderr), flush=True)
            return {"bucket": args.src_bucket_name, "status": -1}
        return {"bucket": args.bucket, "status": 0}
    except Exception as exc:
        print("Error in deleting {}: {}".format(args.bucket, exc))
        # raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', default='canceridc-data')
    parser.add_argument('--bucket', default='idc-nlst-open')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)

    if args.bucket == 'idc-open':
        print("Not allowed")
        exit

    empty_bucket(args)
