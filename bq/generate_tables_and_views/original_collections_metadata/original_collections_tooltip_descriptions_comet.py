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
from bq.bq_utilities import read_json_to_dataframe, dataframe_to_bq, get_github_directory_contents_from_comet, \
    get_data_from_comet
import re
import markdown


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

        return f'<a  href="" url="{url}" data-toggle="modal" data-target="#external-web-warning" class="external-link">{inner_content}<i class="fa-solid fa-external-link external-link-icon" aria-hidden="true"></i></a>'

    # re.DOTALL allows (.*?) to match across newlines
    # re.IGNORECASE handles <A HREF="..."> as well as <a>
    modified_html = re.sub(pattern, replacement_wrapper, html_text, flags=re.IGNORECASE | re.DOTALL)
    return modified_html


def convert_hyperlinks(descriptions):
    for collection in descriptions:
        revised_description = modify_full_hyperlinks(collection['description'])
        collection['description'] = revised_description

    return pd.DataFrame(descriptions)

def get_all_descriptions(path, branch):
    collection_files = get_github_directory_contents_from_comet(path, branch=branch)
    descriptions = []
    for collection_file in collection_files:
        # print(collection_file)
        collection_data = get_data_from_comet(f"{path}/{collection_file}", branch=branch)
        descriptions.append(
            {
                "collection_id": collection_data['collection_id'],
                "description": markdown.markdown(collection_data['summary'])
            }
        )
    return descriptions


def main(args):
    all_descriptions = get_all_descriptions("collections/original", args.comet_branch)
    converted_descriptions = convert_hyperlinks(all_descriptions)
    # markdown_descriptions = convert_to_markdown(all_descriptions)
    dataframe_to_bq(args, converted_descriptions)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', default='idc-dev-etl', help='BQ project')
    parser.add_argument('--bq_dataset_id', default=f'idc_v{settings.CURRENT_VERSION}_dev', help='BQ datasey')
    parser.add_argument('--table_id', default='original_collections_tooltip_descriptions', help='Table name to which to copy data')
    parser.add_argument("--comet_branch", default = 'release/v24')

    args = parser.parse_args()
    print('args: {}'.format(args))

    main(args)
    # export_table(args)
