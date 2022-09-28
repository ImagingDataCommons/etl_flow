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
# Delete all version of all instances in a collection from a bucket
import argparse
import json
from gcs.move_collection.delete_collection import delete_all

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--src_project', default='idc-dev-etl')
    parser.add_argument('--src_bucket', default='idc-dev-open')
    parser.add_argument('--processes', default=16, help="Number of concurrent processes")
    parser.add_argument('--batch', default=100, help='Size of batch assigned to each process')

    args = parser.parse_args()
    print(f'args: {json.dumps(args.__dict__, indent=2)}')
    for collection in ['APOLLO', 'APOLLO-5-ESCA', 'APOLLO-5-LSCC',  'APOLLO-5-LUAD', 'APOLLO-5-PAAD', 'APOLLO-5-THYM']:
        args.collection = collection
        delete_all(args)