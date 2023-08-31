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

# This is a single use script that creates a BQ table of descriptions of
# original collections. The resulting table will then be output to a
# Google sheet where description can be readily edited and new descriptions
# added.

import json
from google.cloud import bigquery, storage
from utilities.bq_helpers import load_BQ_from_json
from original_collections_metadata.gen_original_data_collection_metadata_table import get_all_descriptions_legacy


def gen_table():
    bq_client = bigquery.Client(project='idc-dev-etl')

    collections = [[key, value['description']] for  key, value in get_all_descriptions_legacy(bq_client, None).items()]

    # query = f"""
    # SELECT idc_webapp_collection_id, description
    # FROM `idc-dev-etl.idc_v15_pub.original_collections_metadata`
    # ORDER BY idc_webapp_collection_id
    # """
    # collections = [[row.idc_webapp_collection_id, row.description] for row in bq_client.query(query).result()]

    json_collections = []
    for index, row in enumerate(collections):
        description = collections[index][1]
        start = 0
        while (anchor_start := description.find('<a', start)) >= 0 :
            anchor_end = description.find('</a>',anchor_start)
            new_anchor = old_anchor = description[anchor_start: anchor_end]
            # If the anchor is not to a .gov site
            if new_anchor.find('.gov') == -1:
                new_anchor = new_anchor.replace('href=', 'href="" url=')
                new_anchor = new_anchor.replace(' target="_blank"', '')
                # Finding the closing '>' of the <a> tag
                gt = new_anchor.find('>', 3)

                if new_anchor.find('class="external-link"') == -1:
                    new_anchor = new_anchor[0:gt] + \
                                 ' data-toggle="modal" data-target="#external-web-warning"' + \
                                 ' class="external-link"' + new_anchor[gt:]
                    # new_anchor += ' class="external-link"'
                else:
                    new_anchor = new_anchor[0:gt] + \
                                 ' data-toggle="modal" data-target="#external-web-warning"' + new_anchor[gt:]
                    # pass
                if new_anchor.find('<i') == -1:
                    new_anchor += ' <i class="fa-solid fa-external-link external-link-icon" aria-hidden="true"></i>'
                else:
                    pass
                description = description.replace(old_anchor, new_anchor)
            else:
                pass
            start = anchor_end + 4
        json_collections.append(
            {
                "collection_id": row[0],
                "description": description
            }
        )

        # collections[index][1] = description
    json_object = '\n'.join([json.dumps(record) for record in json_collections])
    load_BQ_from_json(bq_client, 'idc-dev-etl', 'idc_v16_dev', 'original_collections_descriptions_to_spreadsheet', json_object,
                      aschema=None, \
                      write_disposition='WRITE_TRUNCATE', table_description='')

if __name__ == '__main__':
    gen_table()
