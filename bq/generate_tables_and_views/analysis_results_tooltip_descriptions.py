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
from bq.utilities import read_json_to_dataframe, dataframe_to_bq
import re


# Get the descriptions of collections that are only sourced from IDC
def get_idc_descriptions(args, schema=None):
    # Load the Google Sheets data into a Pandas DataFrame

    file_path = f'{settings.BQ_JSON_PROJECT_PATH}/idc_analysis_results_descriptions.json'
    df = read_json_to_dataframe(file_path)

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
    # tcia_descriptions = get_tcia_descriptions(args)
    # all_descriptions = pd.concat([idc_descriptions, tcia_descriptions], ignore_index=True)
    all_descriptions = convert_hyperlinks(idc_descriptions)
    # markdown_descriptions = convert_to_markdown(all_descriptions)
    dataframe_to_bq(args, all_descriptions)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', default='idc-dev-etl', help='BQ project')
    parser.add_argument('--bq_dataset_id', default=f'idc_v{settings.CURRENT_VERSION}_dev', help='BQ datasey')
    parser.add_argument('--table_id', default='analysis_results_tooltip_descriptions', help='Table name to which to copy data')
    parser.add_argument('--columns', default=[], help='Columns in df to keep. Keep all if list is empty')

    args = parser.parse_args()
    print('args: {}'.format(args))

    main(args)
    # export_table(args)
