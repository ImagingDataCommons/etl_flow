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

# Adds pages to a spreadsheet that tracks changes in the TCIA clinical metadata


import argparse
import os

from google.cloud import bigquery
from utilities.logging_config import successlogger, progresslogger, errlogger
import settings
import numpy as np
import gspread
import gspread_formatting
from oauth2client.service_account import ServiceAccountCredentials

import hashlib

def hash_df_row(row):
    # Convert the row to a string
    row_str = row.to_string(index=False)

    # Create a hash object
    hash_object = hashlib.sha256(row_str.encode())

    # Get the hexadecimal representation of the hash
    hex_dig = hash_object.hexdigest()

    return hex_dig


def open_spreadsheet(spreadsheet_key):
    # Define the scope
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']

    # Authenticate using the credentials file
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        f'{os.getenv("HOME")}/.config/gcloud/idc-etl-processing-e691db5d4745.json', scope)
    client = gspread.authorize(creds)

    # Open the Google Sheet (by title or URL key)
    spreadsheet = client.open_by_key(spreadsheet_key) # or client.open_by_key("your_sheet_key").sheet1

    return spreadsheet


def export_to_sheets(args, spreadsheet, df, sheet_id):
    try:
        sheet = spreadsheet.worksheet(f"v{args.version}_{sheet_id}")
    except:
        sheet = spreadsheet.add_worksheet(title=f"v{args.version}_{sheet_id}", rows=df.shape[0], cols=df.shape[1])
    sheet.clear()

    df = df.replace({np.nan: None})
    sheet.update([df.columns.values.tolist()] + df.values.tolist())

    sheet.columns_auto_resize(0, df.shape[1])
    sheet.format(["1"], {"textFormat": {"bold": True}})
    size_col =chr(65 + df.columns.get_loc("download_size"))
    sheet.format([size_col], {"horizontalAlignment": "RIGHT"})
    url_col =chr(65 + df.columns.get_loc("download_url"))
    sheet.format([url_col], {"wrapStrategy": "CLIP"})
    gspread_formatting.set_column_width(sheet, url_col, 400)

def convert_cell(row, col):
    return(chr(65 + col) + str(row+1))


def export_unchanged_to_sheets(args, spreadsheet, same_file_ids, old_df, new_df, sheet_id="unchanged"):
    try:
        sheet = spreadsheet.worksheet(f"v{args.version}_{sheet_id}")
    except:
        sheet = spreadsheet.add_worksheet(title=f"v{args.version}_{sheet_id}", rows=2*old_df.shape[0]+1, \
                                          cols=old_df.shape[1] + new_df.shape[1])
    sheet.clear()

    all_rows = []

    # Construct the columns header
    # Highlight the columns that are in one df but not the other
    line = []
    row = 0
    col = 0
    for key, value in old_df.iloc[0].items():
        line.append(key)
        col += 1
    all_rows.append(line)


    # Generate the data rows as pairs of old and new rows
    # Highlight the cells that are different or are in one df but not the other
    row = 1
    for download_id in same_file_ids:
        if new_df[new_df['download_id'] == download_id]['hash'].iloc[0] == old_df[old_df['download_id'] == download_id]['hash'].iloc[0]:
            col = 0
            old_line = []
            old_row = old_df[old_df['download_id'] == download_id]

            for key, value in old_row.items():
                old_line.append(str(value.iloc[0]))
                col += 1

            all_rows.append(old_line)
            row += 2
    sheet.append_rows(all_rows)


    # Format the sheet a bit
    sheet.columns_auto_resize(0, col)
    sheet.format(["1"], {"textFormat": {"bold": True}})


def export_changed_to_sheets(args, spreadsheet, same_file_ids, old_df, new_df, sheet_id="changed"):
    try:
        sheet = spreadsheet.worksheet(f"v{args.version}_{sheet_id}")
    except:
        sheet = spreadsheet.add_worksheet(title=f"v{args.version}_{sheet_id}", rows=2*old_df.shape[0]+1, \
                                          cols=old_df.shape[1] + new_df.shape[1])
    sheet.clear()

    all_rows = []

    # Construct the columns header
    # Highlight the columns that are in one df but not the other
    line = []
    row = 0
    col = 0
    highlighted = []
    for key, value in old_df.iloc[0].items():
        line.append(key)
        if not key in new_df.iloc[0]:
            highlighted.append(convert_cell(row, col))
        col += 1
    for key, value in new_df.iloc[0].items():
        if  not key in old_df.iloc[0]:
            line.append(key)
            highlighted.append(convert_cell(row, col))
            col += 1
    # sheet.append_row(line)
    all_rows.append(line)


    # Generate the data rows as pairs of old and new rows
    # Highlight the cells that are different or are in one df but not the other
    row = 1
    for download_id in same_file_ids:
        if new_df[new_df['download_id'] == download_id]['hash'].iloc[0] != old_df[old_df['download_id'] == download_id]['hash'].iloc[0]:
            col = 0
            old_line = []
            new_line = []
            old_row = old_df[old_df['download_id'] == download_id]
            new_row = new_df[new_df['download_id'] == download_id]

            for key, value in old_row.items():
                if key in new_row:
                    new_value = new_row[key]
                    old_line.append(str(value.iloc[0]))
                    new_line.append(str(new_value.iloc[0]))
                    if str(value.iloc[0]) != str(new_value.iloc[0]):
                        highlighted.append(convert_cell(row, col))
                        highlighted.append(convert_cell(row+1, col))
                else:
                    old_line.append(str(value.iloc[0]))
                    new_line.append("")
                    highlighted.append(convert_cell(row, col))
                col += 1

            for key, value in new_row.items():
                if not key in old_row:
                    new_line.append(str(value.iloc[0]))
                    old_line.append("")
                    highlighted.append(convert_cell(row + 1, col))
                    col += 1

            all_rows.append(old_line)
            all_rows.append(new_line)
            row += 2
    sheet.append_rows(all_rows)
    sheet.format(highlighted,
         {
             "backgroundColor": {
                 "red": 0.9,
                 "green": 0.8,
                 "blue": 0.8
             }
         }
     )

    # Format the sheet a bit
    sheet.columns_auto_resize(0, col)
    sheet.format(["1"], {"textFormat": {"bold": True}})

def compare_tables(args, spreadsheet):
    client = bigquery.Client()
    old_table = client.get_table(f'{settings.DEV_PROJECT}.idc_v{settings.PREVIOUS_VERSION}_dev.{args.bqtable_name}')
    new_table = client.get_table(f'{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.{args.bqtable_name}')

    old_df = client.list_rows(old_table).to_dataframe()
    new_df = client.list_rows(new_table).to_dataframe()
    old_df['date_updated'] = old_df['date_updated'].astype(str)
    new_df['date_updated'] = new_df['date_updated'].astype(str)

    merged=new_df.merge(old_df,'outer', on='download_id', indicator=True, suffixes=('_new', '_old'), sort=True)
    added_files = merged[merged['_merge']=='left_only']
    added_file_ids = added_files['download_id'].drop_duplicates().sort_values()
    added_files = new_df.merge(added_file_ids, 'right', 'download_id', indicator=True)
    dropped_files = merged[merged['_merge']=='right_only']
    dropped_file_ids = dropped_files['download_id'].drop_duplicates().sort_values()
    dropped_files = old_df.merge(dropped_file_ids, 'right', 'download_id', indicator=True)
    same_files = merged[merged['_merge']=='both']
    same_file_ids = same_files['download_id'].sort_values()

    old_df['hash'] = old_df.apply(hash_df_row, axis=1)
    new_df['hash'] = new_df.apply(hash_df_row, axis=1)

    export_unchanged_to_sheets(args, spreadsheet, same_file_ids, old_df, new_df, 'unchanged_files')
    successlogger.info("\nUnchanged files")
    export_changed_to_sheets(args, spreadsheet, same_file_ids, old_df, new_df, 'changed_files')
    successlogger.info("\nChanged files")

    export_to_sheets(args, spreadsheet, dropped_files, 'dropped_files')
    successlogger.info("\nDropped files")
    export_to_sheets(args, spreadsheet, added_files, 'added_files')
    successlogger.info("\nAdded files")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='Version number')
    parser.add_argument('--bqtable_name', default='tcia_clinical_and_related_metadata', help='BQ table name')
    parser.add_argument('--spreadsheet_key', default='1x4dk7dLlRXh7-V31aCW9e29lsCUdZEpuk7A3B9kKbfQ', help='Google Sheets key')

    args = parser.parse_args()
    successlogger.info(f"{args}")

    spreadsheet = open_spreadsheet(args.spreadsheet_key)

    compare_tables(args, spreadsheet)