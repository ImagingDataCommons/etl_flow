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

# Generate the original_collections_descriptions_end_user table in BQ, from a
# spreadsheet in Google Drive. These descriptions have "normal" hyperlinks...
# no warning about leaving a .gov website.
import settings
import argparse
import pandas as pd
from google.cloud import bigquery
import markdownify
from bq.bq_utilities import read_json_to_dataframe, dataframe_to_bq
import re


# Get the descriptions of collections that are only sourced from IDC
def get_idc_descriptions(args, schema=None):
    # Load the Google Sheets data into a Pandas DataFrame

    file_path = f'{settings.BQ_JSON_PROJECT_PATH}/idc_original_collections_descriptions.json5'
    df = read_json_to_dataframe(file_path)

    # url = f'https://docs.google.com/spreadsheets/d/{args.spreadsheet_id}/gviz/tq?tqx=out:csv&sheet={args.sheet_name}'
    #
    # df = pd.read_csv(url)
    # df = df.map(str)
    # df = df.replace({'nan': None})

    return df

# Get TCIA's descriptions of collections we get from them. For any collection for which there is also pathology, add a
# paragraph to TCIA's description directing to the IDC Zenodo page.
def get_tcia_descriptions(args):
    client = bigquery.Client()
    query = f"""
WITH path AS (
    SELECT DISTINCT ajpc.collection_id, source_url, 
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_public_and_current` ajpc
    JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_EXT_DATASET}.dicom_metadata` dm
    ON ajpc.sop_instance_uid = dm.sopinstanceuid
    WHERE dm.Modality='SM'
)
SELECT DISTINCT 
    REPLACE(REPLACE(LOWER(tcd.collection_id), '-', '_'), ' ', '_') collection_id, 
    if(path.collection_id is not NULL,
        CONCAT(tcd.description, 
        '<p>Please see the <a href="', path.source_url, '" target="_blank">', tcd.collection_id, ': ', ocmis.title, '</a>) wiki page to learn more about the histopathology images and to obtain any supporting metadata for this collection.</p>'),
        tcd.description
        ) description
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.tcia_collection_descriptions` tcd
    RIGHT JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_sources` ac
    ON tcd.collection_id = ac.collection_name
    LEFT JOIN path
    ON tcd.collection_id = path.collection_id
    LEFT JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.original_collections_metadata_idc_source` ocmis
    ON path.collection_id = ocmis.collection_name
    WHERE ac.Access = 'Public' AND tcd.collection_id IS NOT NULL AND (ac.metadata_sunset = 0)
    ORDER BY collection_id
    
    """

    df = client.query_and_wait(query).to_dataframe()
    return df


def convert_to_markdown(df):
    # Convert HTML to Markdown and delete empty lines
    for i, row in df.iterrows():
        description = markdownify.markdownify(df.at[i, 'description'])
        # Clean up hyperlinks
        description = description.replace('[','').replace(']',' ')
        # More clean up
        description = description.replace('**','')

        lines = []
        for line in description.split('\n'):
            if line:
                line = line.replace('\\', '')
                lines.append(line)
        description = '\n'.join(lines)
        df.at[i,'description'] = description

    return df


def modify_full_hyperlinks(html_text):
    # Pattern explanation:
    # Group 1: Opening tag up to the href quote
    # Group 2: The quote type (captured for backreference)
    # Group 3: The URL itself
    # Group 4: The rest of the opening tag
    # Group 5: The inner link content (text/images)
    # Group 6: The closing tag
    pattern = r'(<a\s+[^>]*?href=([\'"]))(.*?)\2([^>]*?>)(.*?)(</a>)'

    def replacement_wrapper(match):
        full_original = match.group(0)
        tag_start = match.group(1)
        quote_type = match.group(2)
        url = match.group(3)
        tag_end = match.group(4)
        inner_content = match.group(5)
        closing_tag = match.group(6)

        # --- Filter Logic ---
        # If ".gov" is in the URL, return the match exactly as it was found
        if ".gov" in url.lower():
            return full_original

        # --- Modification Logic ---
        # Let's say we want to add a CSS class and change the text
        new_url = f"https://myproxy.io/view?url={url}"
        modified_content = f"EXT: {inner_content}"

        # return f"{tag_start}{new_url}{quote_type}{tag_end}{modified_content}{closing_tag}"
        return f'<a  href="" url="{url}" data-toggle="modal" data-target="#external-web-warning" class="external-link">{inner_content}<i class="fa-solid fa-external-link external-link-icon" aria-hidden="true"></i></a>'

    # re.DOTALL allows (.*?) to match across newlines
    # re.IGNORECASE handles <A HREF="..."> as well as <a>
    modified_html = re.sub(pattern, replacement_wrapper, html_text, flags=re.IGNORECASE | re.DOTALL)
    return modified_html


def convert_hyperlinks(df):

    for i, row in df.iterrows():
        description = df.at[i, 'description']
        revised_description = modify_full_hyperlinks(description)
        df.at[i,'description'] = revised_description

    return df


def main(args):
    idc_descriptions = get_idc_descriptions(args)
    tcia_descriptions = get_tcia_descriptions(args)
    all_descriptions = pd.concat([idc_descriptions, tcia_descriptions], ignore_index=True)
    all_descriptions = convert_hyperlinks(all_descriptions)
    # markdown_descriptions = convert_to_markdown(all_descriptions)
    dataframe_to_bq(args, all_descriptions)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', default='idc-dev-etl', help='BQ project')
    parser.add_argument('--bq_dataset_id', default=f'idc_v{settings.CURRENT_VERSION}_dev', help='BQ datasey')
    parser.add_argument('--table_id', default='original_collections_tooltip_descriptions', help='Table name to which to copy data')
    parser.add_argument('--columns', default=[], help='Columns in df to keep. Keep all if list is empty')

    args = parser.parse_args()
    print('args: {}'.format(args))

    main(args)
    # export_table(args)
