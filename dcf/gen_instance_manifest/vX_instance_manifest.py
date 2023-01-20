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


# Generate a manifest of new instance versions in the current (latest) IDC version

import argparse
import settings
from dcf.gen_instance_manifest.instance_manifest import gen_instance_manifest
import json
from utilities.logging_config import successlogger, progresslogger, errlogger

if __name__ == '__main__':
    version = settings.CURRENT_VERSION
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', default=settings.DEV_PROJECT)
    parser.add_argument('--src_bqdataset', default=settings.BQ_PUB_DATASET, \
            help='BQ dataset containing the auxiliary_metadata table from which to get gcs_urls')
    parser.add_argument('--dst_bqdataset', default=settings.BQ_DEV_INT_DATASET, \
            help='BQ dataset in which to build the temporary table')
    parser.add_argument('--versions', default=f'({settings.CURRENT_VERSION})', \
            help= 'A quoted tuple of version numbers, e.g. "(1,2)"')
    parser.add_argument('--manifest_uri', default=f'gs://indexd_manifests/dcf_input/pdp_hosting/idc_v{settings.CURRENT_VERSION}_instance_manifest_*.tsv',
            help="GCS blob in which to save results")
    parser.add_argument('--temp_table', default=f'idc_v{settings.CURRENT_VERSION}_instance_manifest', \
            help='Temporary table in which to write query results')
    args = parser.parse_args()
    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    gen_instance_manifest(args)


