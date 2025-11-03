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

import sys
import argparse

from python_settings import settings
from preingestion.preingestion_code.populate_idc_metadata_tables_from_manifest import prebuild_from_manifests
from google.cloud import storage

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--processes', default=1)
    parser.add_argument('--version', default=settings.CURRENT_VERSION)
    parser.add_argument('--tmp_directory', default='/mnt/disks/idc-etl/tmp')
    parser.add_argument('--src_bucket', default='tcga-reconverted-bad-images', help='Source bucket containing instances')
    parser.add_argument('--subdir', default='TCGA-BRCA_TCGA-A8-A06U-01A-01-TS1_20250909', help="Subdirectory of mount_point at which to start walking directory")

    parser.add_argument('--source_doi', default='10.5281/zenodo.12689962', help='Collection DOI')
    parser.add_argument('--source_url', default='https://doi.org/10.5281/zenodo.12689962',\
                            help='Info page URL')
    parser.add_argument('--versioned_source_doi', default='10.5281/zenodo.17486654', help='Collection DOI')

    parser.add_argument('--manifest_id', default="identifiers.txt",\
                        help="ID of manifest. If NULL, a manifest will be generated.")
    parser.add_argument('--collection_id', default='TCGA-BRCA', help='collection_id of the collection or ID of analysis result to which instances belong.')
    parser.add_argument('--license', default = {"license_url": 'https://creativecommons.org/licenses/by/4.0/',\
            "license_long_name": "Creative Commons Attribution 4.0 International License", \
            "license_short_name": "CC BY 4.0"}, help="(Sub-)Collection license")
    parser.add_argument('--analysis_result', type=bool, default=False, help='True if an analysis result')
    parser.add_argument('--gen_hashes', default=True, help=' Generate hierarchical hashes of collection if True.')
    parser.add_argument('--validate', type=bool, default=True, help='True if validation is to be performed')
    parser.add_argument('--exclusion_filter', default='', help='Ignore blob name during validation if value is in blob name')
    parser.add_argument('--inclusion_filter', default='', help='Only include blobs having args.inclusion_filter in the blob name during validation')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    args.client=storage.Client()