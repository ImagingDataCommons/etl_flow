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

from python_settings import settings
from preingestion.preingestion_code.populate_idc_metadata_tables_from_manifest import prebuild_from_manifest
from google.cloud import storage

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--processes', default=8)
    parser.add_argument('--version', default=settings.CURRENT_VERSION)
    parser.add_argument('--tmp_directory', default='/mnt/disks/idc-etl/tmp')
    parser.add_argument('--src_bucket', default='cmb_pathology', help='Source bucket and subfolder hierarchy containing instances')
    parser.add_argument('--subdir', default='idc-conversion-outputs-cmb_reconverted_plus_OV_BRCA', help="Subdirectory of mount_point to add to blob name. Only if needed to prefix file name in manifest")
    # parser.add_argument('--manifest', default='gs://gtex_pathology/v1/identifiers.txt', help='gcs URL of a manifest')
    parser.add_argument('--manifest', default='gs://cmb_pathology/idc-conversion-outputs-cmb_reconverted_plus_OV_BRCA/identifiers.txt', help='gcs URL of a manifest')
    parser.add_argument('--license', default = {"license_url": 'https://creativecommons.org/licenses/by/4.0/',\
            "license_long_name": "Creative Commons Attribution 4.0 International License", \
            "license_short_name": "CC BY 4.0"}, help="(Sub-)Collection license")
    parser.add_argument('--third_party', type=bool, default=False, help='True if an analysis result')
    parser.add_argument('--gen_hashes', default=True, help=' Generate hierarchical hashes of collection if True.')
    parser.add_argument('--validate', type=bool, default=True, help='True if validation is to be performed')
    parser.add_argument("--subset_of_db_expected_in_bucket", default=False)

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    args.client=storage.Client()

    versioned_source_dois = {'CMB-AML': '10.5281/zenodo.13993760',
                   'CMB-BRCA': '10.5281/zenodo.13993762',
                   'CMB-CRC': '10.5281/zenodo.13993770',
                   'CMB-GEC': '10.5281/zenodo.13993774',
                   'CMB-LCA': '10.5281/zenodo.13993777',
                   'CMB-MEL': '10.5281/zenodo.13993788',
                   'CMB-MML': '10.5281/zenodo.13993793',
                   'CMB-OV': '10.5281/zenodo.13993797',
                   'CMB-PCA': '10.5281/zenodo.13993799'}

    source_dois = {
                'CMB-AML': '10.5281/zenodo.13993759',
                'CMB-BRCA': '10.5281/zenodo.13993761',
                'CMB-CRC': '10.5281/zenodo.13993769',
                'CMB-GEC': '10.5281/zenodo.13993773',
                'CMB-LCA': '10.5281/zenodo.13993776',
                'CMB-MEL': '10.5281/zenodo.13993787',
                'CMB-MML': '10.5281/zenodo.13993792',
                'CMB-OV': '10.5281/zenodo.13993796',
                'CMB-PCA': '10.5281/zenodo.13993798'
    }

    args.collection_ids = [
                'CMB-AML',
                'CMB-BRCA',
                'CMB-CRC',
                'CMB-GEC',
                'CMB-LCA',
                'CMB-MEL',
                'CMB-MML',
                'CMB-OV',
                'CMB-PCA'
        ]

    prebuild_from_manifest(args, sep='\t', source_dois=source_dois, versioned_source_dois=versioned_source_dois)


