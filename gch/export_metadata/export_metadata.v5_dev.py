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



# Export metadata from a DICOM store to BQ

import argparse
import sys
from gch.export_metadata.export_metadata import export_metadata

# from helpers.dicom_helpers import get_dataset, get_dicom_store, create_dicom_store, import_dicom_instance

if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--version', default=5, help="IDC version")
    args = parser.parse_args()
    # Source
    parser.add_argument('--src_project', default='idc-dev-etl')
    parser.add_argument('--src_region', default='us-central1', help='Dataset region')
    parser.add_argument('--dcmdataset_name', default='idc', help='DICOM dataset name')
    parser.add_argument('--dcmdatastore_name', default=f'v{args.version}', help='DICOM datastore name')
    # Destination
    parser.add_argument('--dst_project', default='idc-dev-etl')
    parser.add_argument('--dst_region', default='us', help='Dataset region')
    parser.add_argument('--bqdataset', default=f'idc_v{args.version}', help="BQ dataset name")
    parser.add_argument('--bqtable', default='dicom_metadata', help="BQ table name")

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    export_metadata(args)

