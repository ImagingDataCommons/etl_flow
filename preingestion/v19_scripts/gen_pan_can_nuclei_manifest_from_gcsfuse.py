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

# Adds/replaces data to the idc_collection/_patient/_study/_series/_instance DB tables
# from a specified bucket.
#
# For this purpose, the bucket containing the instance blobs is gcsfuse mounted, and
# pydicom is then used to extract needed metadata.
#
# The script walks the directory hierarchy from a specified subdirectory of the
# gcsfuse mount point

import sys
import settings
import argparse
import pathlib
import subprocess

from idc.models import IDC_Collection, IDC_Patient, IDC_Study, IDC_Series, IDC_Instance, Collection, Patient
from utilities.logging_config import successlogger, errlogger, progresslogger
from base64 import b64decode
from preingestion.validation_code.validate_analysis_result import validate_analysis_result
from preingestion.validation_code.validate_original_collection import validate_original_collection

from pydicom import dcmread

from utilities.sqlalchemy_helpers import sa_session
from google.cloud import storage
from preingestion.preingestion_code.gen_manifest_from_gcsfuse import build_manifest

import pandas as pd


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--version', default=settings.CURRENT_VERSION)
    parser.add_argument('--src_bucket', default='pan_can_nuclei', help='Source bucket containing instances')
    parser.add_argument('--mount_point', default='/mnt/disks/idc-etl/gen_manifest_gcsfuse_mount_point', help='Directory on which to mount the bucket.\
                The script will create this directory if necessary.')
    parser.add_argument('--subdir', \
            default='pan_cancer_nuclei_polygon_2d_2024_05_21', \
            help="Subdirectory of mount_point at which to start walking directory")
    parser.add_argument('--collection_id', default='', help='collection_name of the collection or ID of analysis result to which instances belong.')
    parser.add_argument('--manifest', default='./pan_can_nuclei_generated_manifest.csv', help='Manifest file name')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    args.client=storage.Client()

    try:
        # gcsfuse mount the bucket
        pathlib.Path(args.mount_point).mkdir( exist_ok=True)
        subprocess.run(['gcsfuse', '--implicit-dirs', args.src_bucket, args.mount_point])
        build_manifest(args)
    finally:
        # Always unmount
        subprocess.run(['fusermount', '-u', args.mount_point])


