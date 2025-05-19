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
from utilities.tcia_helpers import get_all_tcia_metadata, get_url
from google.cloud import bigquery
from utilities.bq_helpers import load_BQ_from_json
from bq.generate_tables_and_views.original_collections_metadata.schema import data_collections_metadata_schema
from utilities.logging_config import successlogger, progresslogger, errlogger
import settings
import pandas as pd
import hashlib

BOLD="\033[91m"
UNBOLD="\033[0m"

def hash_df_row(row):
    # Convert the row to a string
    row_str = row.to_string(index=False)

    # Create a hash object
    hash_object = hashlib.sha256(row_str.encode())

    # Get the hexadecimal representation of the hash
    hex_dig = hash_object.hexdigest()

    return hex_dig


def compare_tables(args):
    client = bigquery.Client()
    old_table = client.get_table(f'{settings.DEV_PROJECT}.idc_v{settings.PREVIOUS_VERSION}_dev.{args.bqtable_name}')
    new_table = client.get_table(f'{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.{args.bqtable_name}')

    old_df = client.list_rows(old_table).to_dataframe()
    new_df = client.list_rows(new_table).to_dataframe()

    merged=new_df.merge(old_df,'outer', on='download_id', indicator=True, suffixes=('_new', '_old'), sort=True)
    added_files = merged[merged['_merge']=='left_only']
    dropped_files = merged[merged['_merge']=='right_only']
    same_files = merged[merged['_merge']=='both']
    same_file_ids = same_files['download_id'].sort_values()

    successlogger.info("Dropped files:")
    if len(dropped_files)>0:
        successlogger.info("idc_collection_id    download_id   download_slug   date_updated    download_title")
        for i, _file in dropped_files.iterrows():
            successlogger.info(f"{_file['idc_collection_id_old']} : {_file['download_id']} : {_file['download_slug_old']} : {_file['date_updated_old']} : {_file['download_title_old']}")

    successlogger.info("\nAdded files:")
    if len(added_files)>0:
        successlogger.info("idc_collection_id    download_id   download_slug_name   parent_type  date_updated    download_title")
        for i, _file in added_files.iterrows():
            successlogger.info(f"{_file['idc_collection_id_new']} : {_file['download_id']} : {_file['download_slug_new']} : {_file['parent_type']} : {_file['date_updated_new']} : {_file['download_title_new']}")

    old_df['hash'] = old_df.apply(hash_df_row, axis=1)
    new_df['hash'] = new_df.apply(hash_df_row, axis=1)

    successlogger.info("\nChanged files")
    line = ""
    for key, value in old_df.iloc[0].items():
        if not key in new_df.iloc[0]:
            line += BOLD + key + '\t' +UNBOLD
        else:
            line += key + '\t'
    for key, value in new_df.iloc[0].items():
        if  not key in new_df.iloc[0]:
            line += BOLD + key + '\t' + UNBOLD


    successlogger.info(line)
        # successlogger.info(f'{key}\t', end="")
    successlogger.info("")
    for download_id in same_file_ids:
        if new_df[new_df['download_id'] == download_id]['hash'].iloc[0] != old_df[old_df['download_id'] == download_id]['hash'].iloc[0]:
            old_line = ""
            new_line = ""
            old_row = old_df[old_df['download_id'] == download_id].drop("hash", axis=1)
            new_row = new_df[new_df['download_id'] == download_id].drop("hash", axis=1)

            for key, value in old_row.items():
                if key in new_row:
                    new_value = new_row[key]
                    if str(value.iloc[0]) != str(new_value.iloc[0]):
                        old_line += BOLD + str(value.iloc[0]) + ',\t' + UNBOLD
                        new_line += BOLD + str(new_value.iloc[0]) + ',\t' + UNBOLD
                    else:
                        old_line += str(value.iloc[0]) + ',\t'
                        new_line += str(new_value.iloc[0]) + ',\t'
                else:
                    old_line += BOLD + str(value.iloc[0]) + ',\t' + UNBOLD
                    new_line += '\t'
            for key, value in new_row.items():
                if not key in old_row:
                    new_value += BOLD + str(value.iloc[0]) + ',\t' + UNBOLD
                    old_line += '\t'
            successlogger.info(old_line)
            successlogger.info(new_line)
            successlogger.info("")

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--bqtable_name', default='tcia_clinical_and_related_metadata', help='BQ table name')
    parser.add_argument('--results_file', default='tcia_clinical_and_related_metadata_comparison.txt', help='Comparison results file name')

    args = parser.parse_args()
    successlogger.info(f"{args}")

    compare_tables(args)