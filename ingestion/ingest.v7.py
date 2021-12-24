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

import os
import sys
import argparse
from ingestion.ingest import ingest
from google.cloud import storage

import logging


if __name__ == '__main__':
    print('Why were sizes==0?')
    # exit(-1)

    parser = argparse.ArgumentParser()
    # ]
    parser.add_argument('--previous_version', default=6, help='Previous version')
    parser.add_argument('--version', default=7, help='Version to work on')
    parser.add_argument('--client', default=storage.Client())
    args = parser.parse_args()
    parser.add_argument('--db', default=f'idc_v7', help='Database on which to operate')
    parser.add_argument('--project', default='idc-dev-etl')
    parser.add_argument('--wsi_src_bucket', default=storage.Bucket(args.client,'af-dac-wsi-conversion-results'), help='Bucket in which to find WSI DICOMs')
    parser.add_argument('--prestaging_bucket_prefix', default=f'idc_v{args.version}_', help='Copy instances here before forwarding to --staging_bucket')
    parser.add_argument('--num_processes', default=0, help="Number of concurrent processes")
    # parser.add_argument('--todos', default='{}/logs/path_ingest_v{}_todos.txt'.format(os.environ['PWD'], args.version ), help="Collections to include")
    parser.add_argument('--skips', default='{}/logs/ingest_v{}_skips.txt'.format(os.environ['PWD'], args.version ), help="Collections to skip")
    # parser.add_argument('--source', default=TCIA, help="Source (type of data) from which to ingest: 'Pathology' or 'TCIA'")
    parser.add_argument('--server', default="", help="NBIA server to access. Set to NLST for NLST ingestion")
    parser.add_argument('--dicom', default='/mnt/disks/idc-etl/dicom', help='Directory in which to expand downloaded zip files')
    parser.add_argument('--build_mtm_db', default=False, help='True if we are building many-to-many DB')
    args = parser.parse_args()
    args.id = 0 # Default process ID

    print("{}".format(args), file=sys.stdout)

    rootlogger = logging.getLogger('root')

    errlogger = logging.getLogger('root.err')

    ingest(args)

