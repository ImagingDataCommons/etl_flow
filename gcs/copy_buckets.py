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
import time
from datetime import timedelta

def create_dest_bucket(args):
    # Try to create the destination bucket
    client = storage.Client(project=args.dst_project)
    new_bucket = client.bucket(args.dst_bucket_name, user_project=args.dst_project)
    new_bucket.iam_configuration.uniform_bucket_level_access_enabled = True
    new_bucket.versioning_enabled = False
    try:
        result = client.create_bucket(new_bucket, requester_pays=args.requester_pays, location='us')
        # If we get here, this is a new bucket
        if args.production:
            # Add allAuthenticatedUsers
            policy = new_bucket.get_iam_policy(requested_policy_version=3)
            policy.bindings.append({
                "role": "roles/storage.objectViewer",
                "members": {"allAuthenticatedUsers"}
            })
            new_bucket.set_iam_policy(policy)
    except Conflict:
        # Bucket exists
        return
    except:
        # Bucket creation failed somehow
        print("Error creating bucket {}: {}".format(dst_bucket_name, result), flush=True)
        raise


def copy_buckets(args):
    begin = time.time()

    create_dest_bucket(args)

    for bucket in args.src_buckets:
        print("Copying {}".format(bucket), flush=True)
        try:
            result = run(['gsutil', '-u', args.dst_project, '-m', 'cp', '-L', f'logs/copy_buckets.log', f'gs://{bucket}/*',
                          f'gs://{args.dst_bucket_name}'])
            print("   {} copied, results: {}".format(bucket, result), flush=True)
            if result.returncode:
                print('Copy {} failed: {}'.format(result.stderr), flush=True)
                return {"bucket": bucket, "status": -1}
        except:
            print("Error in copying {}: {},{},{}".format(bucket, sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2]), file=sys.stdout, flush=True)
            raise
    duration = str(timedelta(seconds=(time.time() - begin)))
    print(f"Completed in {duration}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--src_buckets', default=['idc_v5_nlst'])
    parser.add_argument('--dst_bucket_name', default='idc-nlst-open')
    parser.add_argument('--src_project', default='idc-dev-etl')
    parser.add_argument('--dst_project', default='canceridc-data')
    parser.add_argument('--requester_pays', default=True)
    parser.add_argument('--production', type=bool, default=False, help="If a production bucket, enable requester pays, allAuthUsers")
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    client = storage.Client(project=args.dst_project)

    copy_buckets(args)
