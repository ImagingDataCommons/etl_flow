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

# Generate various manifests for a specified IDC sourced collection
# or analysis result. The instances or series included are identified
# by their source_doi.
## NOTE: Zenodo DOIs are versioned and we associated the version specific
## DOI with data when it is added or revised. It seems possible that objects
## in an IDC-version of a collection or result could have different DOI versions.
## We need to be careful to deal with this.
# The resulting manifest is copied to GCS


import argparse
import sys
from gen_manifest import dcf_manifest, s5cmd_manifest
import settings


if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    # parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version for which to build the table')
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version for which to build the table')
    parser.add_argument('--temp_table_bqdataset', default=f'whc_dev')
    parser.add_argument('--temp_table', default=f'doi_subcollection')
    parser.add_argument('--manifest_bucket', default='doi_manifests')
    parser.add_argument('--collection_id', default='rms_mutation_prediction_expert_annotations')
    parser.add_argument('--manifest_version', default=settings.CURRENT_VERSION, help='IDC revision of the collection whose manifest is to be generated')
    parser.add_argument('--source_dois', default=('10.5281/zenodo.10462857', '10.5281/zenodo.10462858'), help="DOI's of series to be included in the manifest")

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)

    dcf_manifest(args, args.collection_id, args.manifest_version, args.source_dois, 'dcf', 'https://nci-crdc.datacommons.io/ga4gh/drs/v1/objects/')
    s5cmd_manifest(args, args.collection_id, args.manifest_version, args.source_dois, 'gcs', 'https://storage.googleapis.com')
    s5cmd_manifest(args, args.collection_id, args.manifest_version, args.source_dois, 'aws', 'https://s3.amazonaws.com')

