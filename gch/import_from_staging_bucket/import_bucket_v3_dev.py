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


import argparse
import sys
import os
import logging
from logging import INFO

from gch.import_bucket import import_bucket


if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--src_buckets', default=['idc_dev'], help="List of buckets from which to import")
    parser.add_argument('--dst_project', default='idc-dev-etl')
    parser.add_argument('--dataset_region', default='us-central1', help='Dataset region')
    parser.add_argument('--gch_dataset_name', default='idc', help='Dataset name')
    parser.add_argument('--gch_dicomstore_name', default='v3', help='Datastore name')
    parser.add_argument('--period', default=60, help="seconds to sleep between checking operation status")
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)

    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/dicomstore_import_log_2020_3_25.log'.format(os.environ['PWD']))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/dicomstore_import_err_2020_3_25.log'.format(os.environ['PWD']))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    import_bucket(args)

