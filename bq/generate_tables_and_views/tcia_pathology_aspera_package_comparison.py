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

# Generate a table of TCIA clinical metadata packages

import argparse
import sys
import json
from utilities.tcia_helpers import get_tcia_collection_manager_data, get_url
from google.cloud import bigquery
from utilities.bq_helpers import load_BQ_from_json
from bq.generate_tables_and_views.original_collections_metadata.gen_original_collection_metadata_comet import data_collections_metadata_schema
from utilities.logging_config import errlogger
import settings



def compare_tables(args):
    client = bigquery.Client()
    old_table = client.get_table(f'{settings.DEV_PROJECT}.idc_v{settings.PREVIOUS_VERSION}_dev.{args.bqtable_name}')
    new_table = client.get_table(f'{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.{args.bqtable_name}')

    old_df = client.list_rows(old_table).to_dataframe()
    new_df = client.list_rows(new_table).to_dataframe()

    merged=new_df.merge(old_df,'outer', on='Download_slug', indicator=True, suffixes=('_new', '_old'), sort=True)
    added_files = merged[merged['_merge']=='left_only']
    dropped_files = merged[merged['_merge']=='right_only']
    continuing_files = merged[merged['_merge']=='both']


    print("Dropped files:")
    if len(dropped_files)>0:
        print("TCIA_collection_id    IDC_collection_id   Download_slug   Modified_download_date    Download_title")
        for i, file in dropped_files.iterrows():
            print(file['TCIA_collection_id_old'], ' : ', file['IDC_collection_id_old'], ' : ', file['Download_slug'], ' : ', \
                file['Modified_download_date_old'], ' : ', file['Download_title_old'])

    print("\nAdded files:")
    if len(added_files)>0:
        print("TCIA_collection_id    IDC_collection_id   Download_slug   Modified_download_date    Download_title")
        for i, file in added_files.iterrows():
            print(file['TCIA_collection_id_new'], ' : ', file['IDC_collection_id_new'], ' : ', file['Download_slug'], ' : ', \
                file['Modified_download_date_new'], ' : ', file['Download_title_new'])

    print("\nRevised files:")
    print("TCIA_collection_id    IDC_collection_id    Download_slug   Modified_download_date    Download_title")
    for i, file in continuing_files.iterrows():
        if file['Aspera_URL_old'].split('context')[-1] != file['Aspera_URL_new'].split('context')[-1]:
            print(file['TCIA_collection_id_new'], ' : ', file['IDC_collection_id_new'], ' : ', file['Download_slug'], ' : ', \
                file['Modified_download_date_new'], ' : ', file['Download_title_new'])
            print(f'\t\t{file["Aspera_URL_old"]} \n\t\t--> \n\t\t{file["Aspera_URL_new"]}')

    return


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--bqtable_name', default='tcia_pathology_aspera_package_status', help='BQ table name')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)

    compare_tables(args)