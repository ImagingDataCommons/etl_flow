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
import argparse
import pathlib
import subprocess

from python_settings import settings
from preingestion.preingestion_code.populate_idc_metadata_tables_from_gcsfuse import prebuild_from_gcsfuse
from google.cloud import storage

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--version', default=settings.CURRENT_VERSION)
    parser.add_argument('--src_bucket', default='cmb_pathology', help='Source bucket containing instances')
    parser.add_argument('--mount_point', default='/mnt/disks/idc-etl/preeingestion_gcsfuse_mount_point', help='Directory on which to mount the bucket.\
                The script will create this directory if necessary.')
    parser.add_argument('--subdir', \
                        default='idc-conversion-outputs-cmb', \
                        help="Subdirectory of mount_point at which to start walking directory")
    parser.add_argument('--subset_of_db_expected', default=True,
                        help='If True, validation will not report an error if the instances in the bucket are a subset of the instance in the DB')
    parser.add_argument('--collection_id', default='',
                        help='collection_name of the collection or ID of analysis result to which instances belong.')
    parser.add_argument('--collection_map', default='cmb_collection_map.csv',
                        help='Optional csv file that maps GCS blob name to collection_name. If present, overrides collection_name')
    parser.add_argument('--source_doi', default='10.5281/zenodo.11099111', help='Concept DOI ')
    parser.add_argument('--source_url', default='https://doi.org/10.5281/zenodo.11099111', \
                        help='Info page URL')
    parser.add_argument('--versioned-source_doi', default='10.5281/zenodo.11099112',
                        help='Version specific DOI of this ingestion')
    parser.add_argument('--license', default={"license_url": 'https://creativecommons.org/licenses/by/3.0/', \
                                              "license_long_name": "Creative Commons Attribution 3.0 Unported License", \
                                              "license_short_name": "CC BY 3.0"}, help="(Sub-)Collection license")
    parser.add_argument('--third_party', type=bool, default=False, help='True if from a third party analysis result')
    parser.add_argument('--gen_hashes', default=True, help=' Generate hierarchical hashes of collection if True.')
    parser.add_argument('--validate', type=bool, default=True, help='True if validation is to be performed')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    args.client = storage.Client()

    try:
        # gcsfuse mount the bucket

        pathlib.Path(args.mount_point).mkdir(exist_ok=True)
        subprocess.run(['gcsfuse', '--implicit-dirs', args.src_bucket, args.mount_point])
        prebuild_from_gcsfuse(args)
    finally:
        # Always unmount
        subprocess.run(['fusermount', '-u', args.mount_point])
