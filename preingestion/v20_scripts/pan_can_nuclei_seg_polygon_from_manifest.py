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
    parser.add_argument('--src_bucket', default='pan_cancer_nuclei_seg_polygon_2d_binary_tiled_full_2024_05_09', help='Source bucket containing instances')
    parser.add_argument('--subdir', default='', help="Subdirectory of mount_point to add to blob name. Only if needed to prefix file name in manifest")
    parser.add_argument('--manifest', default='./pan_cancer_nuclei_seg_polygon_2d_binary_tiled_full_2024_05_09.csv', help='URL of a manifest')
    parser.add_argument('--source_doi', default='10.5281/zenodo.11099004', help='If there is a single source doi')
    parser.add_argument('--versioned_source_doi', default='10.5281/zenodo.14009675', help='If there is a single versioned source doi')
    parser.add_argument('--license', default = {"license_url": 'https://creativecommons.org/licenses/by/4.0/',\
            "license_long_name": "Creative Commons Attribution 4.0 International License", \
            "license_short_name": "CC BY 4.0"}, help="(Sub-)Collection license")
    parser.add_argument('--third_party', type=bool, default=True, help='True if an analysis result')
    parser.add_argument('--validate', type=bool, default=True, help='True if validation is to be performed')
    parser.add_argument("--subset_of_db_expected_in_bucket", default=True)
    parser.add_argument("--inclusion_filter", default="seg", help='Only include blobs having args.inclusion_filter in the blob name during validation')
    parser.add_argument('--gen_hashes', default=True, help=' Generate hierarchical hashes of collection if True.')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    args.client=storage.Client()

    prebuild_from_manifest(args)


