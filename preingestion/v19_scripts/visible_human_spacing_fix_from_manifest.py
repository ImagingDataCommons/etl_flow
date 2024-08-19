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
    parser.add_argument('--processes', default=1)
    parser.add_argument('--version', default=settings.CURRENT_VERSION)
    parser.add_argument('--tmp_directory', default='/mnt/disks/idc-etl/tmp')
    parser.add_argument('--src_bucket', default='nlm_visible_human_project_radiology', help='Source bucket and subfolder hierarchy containing instances')
    parser.add_argument('--subdir', default='dac-vhm-radiological-fixed-dst', help="Subdirectory of mount_point to add to blob name. Only if needed to prefix file name in manifest")
    # parser.add_argument('--manifest', default='gs://gtex_pathology/v1/identifiers.txt', help='gcs URL of a manifest')
    parser.add_argument('--manifest', default='./visible_human_spacing_fix_generated_manifest.csv', help='gcs URL of a manifest')
    parser.add_argument('--collection_id', default='NLM-Visible-Human-Project', help='collection_name of the collection or ID of analysis result to which instances belong.')
    parser.add_argument('--source_doi', default='10.5281/zenodo.12690049', help='Collection DOI. Might be empty string.')
    parser.add_argument('--source_url', default='https://doi.org/10.5281/zenodo.12690049',\
                        help='Info page URL')
    parser.add_argument('--license', default = {"license_url": 'https://creativecommons.org/licenses/by/4.0/',\
            "license_long_name": "Creative Commons Attribution 4.0 International License", \
            "license_short_name": "CC BY 4.0"}, help="(Sub-)Collection license")
    parser.add_argument('--third_party', type=bool, default=False, help='True if an analysis result')
    parser.add_argument('--gen_hashes', default=True, help=' Generate hierarchical hashes of collection if True.')
    parser.add_argument('--validate', type=bool, default=True, help='True if validation is to be performed')
    parser.add_argument('--subset_of_db_expected_in_bucket', default=True, help='If true, expect that the DB will have more instances than the bucket')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    args.client=storage.Client()

    prebuild_from_manifest(args)


