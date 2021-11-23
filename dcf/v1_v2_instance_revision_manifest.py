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


# Generate a manifest of revised instance URLs V1 and v2 instances.

import argparse
from google.cloud import bigquery
from dcf.instance_manifest import gen_revision_manifest

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--project', default='idc-dev-etl')
    parser.add_argument('--bqdataset', default='idc_v5')
    parser.add_argument('--versions', default=(1,2), help= 'A tuple of version numbers, e.g. (1,2)')
    parser.add_argument('--table', default='instance')
    parser.add_argument('--manifest_uri', default='gs://indexd_manifests/dcf_input/pdp_hosting/idc_v1_v2_instance_revision_manifest_*.tsv',
                        help="GCS file in which to save results")
    parser.add_argument('--temp_table', default='idc_v1_v2_instance_revision_manifest', \
                        help='Table in which to write query results')
    args = parser.parse_args()

    gen_revision_manifest(args)


