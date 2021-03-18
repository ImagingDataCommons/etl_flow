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

# Empty some specified GCS bucket

from google.cloud import storage
import argparse
import sys

#Get info on each blob in a collection
def get_collection_iterator(storage_client, bucket_name, prefix):
    pages = storage_client.list_blobs(bucket_name, prefix=prefix)
    # pages = storage_client.list_blobs(bucket_name, prefix="dicom/")
    return pages


def get_bucket_metadata(storage_client, bucket_name, prefix):
    pages = get_collection_iterator(storage_client, bucket_name, prefix)
    blobs = []

    for page in pages.pages:
        blobs.extend(list(page))
    metadata = {blob.name:blob for blob in blobs}
    return metadata

def empty_bucket(args):
    storage_client = storage.Client()

    bucket = storage_client.bucket(args.bucket)
    blobs = bucket.list_blobs()
    bucket.delete_blobs(blobs)

if __name__ == '__main__':
    parser =argparse.ArgumentParser()

    parser.add_argument('--bucket', default='idc_dev', help='Bucket to empty')
    parser.add_argument('--project', default='idc-dev-etl')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)

    empty_bucket(args)
