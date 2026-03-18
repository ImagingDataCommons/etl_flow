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
from preingestion.preingestion_code.populate_idc_metadata_tables_from_manifest import prebuild_from_manifests
from google.cloud import storage

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--processes', default=0)
    parser.add_argument('--version', default=settings.CURRENT_VERSION)
    parser.add_argument('--collection_id', default='CDDP-EAGLE-1', help='collection_name of the collection or ID of analysis result to which instances belong.')

    parser.add_argument("--comet_branch", default = "release/v24")

    parser.add_argument('--gen_hashes', default=True, help=' Generate hierarchical hashes of collection if True.')
    parser.add_argument('--validate', type=bool, default=True, help='True if validation is to be performed')
    parser.add_argument('--exclusion_filter', default='', help='Ignore blob name during validation if value is in blob name')
    parser.add_argument('--inclusion_filter', default='', help='Only include blobs having args.inclusion_filter in the blob name during validation')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    args.client=storage.Client()

    prebuild_from_manifests(args, sep='\t')


