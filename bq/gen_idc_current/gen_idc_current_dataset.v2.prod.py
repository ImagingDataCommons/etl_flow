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

# This script generates the idc_current dataset, which is comprised of a view of every table and view in
# the idc_vX dataset corresponding to the current IDC data version.

import argparse
import sys
from bq.gen_idc_current.gen_idc_current_dataset import gen_idc_current_dataset



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=2, help='Current IDC version')
    args = parser.parse_args()
    parser.add_argument('--project', default='canceridc-data')
    parser.add_argument('--bqdataset', default=f'idc_v{args.version}', help='BQ dataset name')
    parser.add_argument('--current_bqdataset', default=f'idc_current', help='current dataset name')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    gen_idc_current_dataset(args)
