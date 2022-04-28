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

# Copy most public tables/views from dev to prod.
# Note tha auxiliary_metadata must be generated for prod since urls are different
# Note also that views such as segmentations must also be separately generated.
import argparse
import sys
from python_settings import settings
from bq.copy_tables.copy_tables import copy_tables


if __name__ == '__main__':

    parser =argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version of dataset to which to copy tables')
    parser.add_argument('--src_project', default=f'{settings.DEV_PROJECT}')
    parser.add_argument('--dst_project', default=f'{settings.PDP_PROJECT}')
    parser.add_argument('--src_bqdataset', default=f'idc_v{settings.CURRENT_VERSION}_pub', help='Source BQ dataset')
    parser.add_argument('--dst_bqdataset', default=f'idc_v{settings.CURRENT_VERSION}', help='Destination BQ dataset')
    parser.add_argument('--dataset_description', default = f'IDC V{settings.CURRENT_VERSION} BQ tables and views')
    parser.add_argument('--bqtables', \
        default=[
            'analysis_results_metadata', \
            'dicom_metadata', \
            'nlst_canc', 'nlst_ctab', 'nlst_ctabc', 'nlst_prsn', 'nlst_screen', \
            'original_collections_metadata', \
            'tcga_biospecimen_rel9', 'tcga_clinical_rel9', \
            'version_metadata'
        ], help='BQ tables to be copied')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    copy_tables(args)