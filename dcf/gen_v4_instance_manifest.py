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


# Generate a manifest of instances that are new in V2. We do not
# include instances from collections that are excluded.
import argparse
from dcf.gen_instance_manifest import gen_instance_manifest

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=4)
    args = parser.parse_args()
    parser.add_argument('--project', default='idc-dev-etl')
    parser.add_argument('--bqdataset', default=f'idc_v{args.version}')
    parser.add_argument('--table', default='instance')
    parser.add_argument('--manifest_uri', default=f'gs://indexd_manifests/dcf_input/idc_v{args.version}_instance_manifest-*.tsv',
                        help="GCS file in which to save results")
    parser.add_argument('--temp_table', default=f'gen_v{args.version}_instance_tmp_manifest', \
                        help='Table in which to write query results')

    args = parser.parse_args()


    gen_instance_manifest(args)


