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

# Load data in some GCS buckets into a DICOM store.


import sys
import argparse
from time import sleep
from googleapiclient.errors import HttpError
from utilities.gch_helpers import get_dicom_store, get_dataset, create_dicom_store, create_dataset
from googleapiclient import discovery


def get_gch_client():
    """Returns an authorized API client by discovering the Healthcare API and
    creating a service object using the service account credentials in the
    GOOGLE_APPLICATION_CREDENTIALS environment variable."""
    api_version = "v1"
    service_name = "healthcare"

    return discovery.build(service_name, api_version)


def get_dataset_operation(
        project_id,
        cloud_region,
        dataset_id,
        operation):
    client = get_gch_client()
    op_parent = "projects/{}/locations/{}/datasets/{}".format(project_id, cloud_region, dataset_id)
    op_name = "{}/operations/{}".format(op_parent, operation)
    request = client.projects().locations().datasets().operations().get(name=op_name)
    response = request.execute()
    return response


def wait_done(args, sleep_time):
    # operation = response['name'].split('/')[-1]
    operation = args.operation
    while True:
        result = get_dataset_operation(args.dst_project, args.dataset_region, args.gch_dataset_name, operation)
        print("{}".format(result))

        if 'done' in result:
            break
        sleep(sleep_time)
    return result

if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--version', default=7)
    args = parser.parse_args()
    parser.add_argument('--src_buckets', default=['idc-dev-defaced', 'idc-dev-cr', 'idc-dev-open'], help="List of buckets from which to import")
    # parser.add_argument('--src_buckets', default=['idc-dev-cr','idc-dev-open'], help="List of buckets from which to import")
    parser.add_argument('--dst_project', default='idc-dev-etl')
    parser.add_argument('--dataset_region', default='us-central1', help='Dataset region')
    parser.add_argument('--gch_dataset_name', default='idc', help='Dataset name')
    parser.add_argument('--gch_dicomstore_name', default=f'v{args.version}-with-redacted', help='Datastore name')
    parser.add_argument('--operation', default='2765863762417156097')
    parser.add_argument('--period', default=60, help="seconds to sleep between checking operation status")
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)

    wait_done(args, args.period)
