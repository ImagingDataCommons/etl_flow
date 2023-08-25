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

# Create a table of TCIAs descriptions of original collections
# We can compare the table of a new version with that of the
# previous version to determine whther TCIA has changed any
# definitions.
# If so, then we can adjust our original_collection_descriptions
# table accordingly.

import json
from google.cloud import bigquery, storage

import settings
from utilities.bq_helpers import load_BQ_from_json
from original_collections_metadata.gen_original_data_collection_metadata_table import get_collection_descriptions_and_licenses


def gen_table():
    bq_client = bigquery.Client(project='idc-dev-etl')

    collections = [[key, value['description']] for  key, value in get_collection_descriptions_and_licenses().items()]

    # query = f"""
    # SELECT idc_webapp_collection_id, description
    # FROM `idc-dev-etl.idc_v15_pub.original_collections_metadata`
    # ORDER BY idc_webapp_collection_id
    # """
    # collections = [[row.idc_webapp_collection_id, row.description] for row in bq_client.query(query).result()]

    json_collections = []
    for index, row in enumerate(collections):
        description = collections[index][1]
        json_collections.append(
            {
                "collection_id": row[0],
                "description": description
            }
        )

    schema = [
        bigquery.SchemaField('collection_id', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('description', 'STRING', mode='NULLABLE')
        ]

            # collections[index][1] = description
    json_object = '\n'.join([json.dumps(record) for record in json_collections])
    load_BQ_from_json(bq_client, 'idc-dev-etl', f'idc_v{settings.CURRENT_VERSION}_dev', 'tcia_collection_descriptions', json_object,
                      aschema=schema, \
                      write_disposition='WRITE_TRUNCATE', table_description='')

if __name__ == '__main__':
    gen_table()