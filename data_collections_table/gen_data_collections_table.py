#!/usr/bin/env
#
# Copyright 2020-2021 Institute for Systems Biology
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

import json
from collections_ids.gen_collection_ids import build_collections_id_table
from utilities.tcia_helpers import get_collection_descriptions
from utilities.tcia_scrapers import scrape_tcia_data_collections_page, build_TCIA_to_Description_ID_Table

def gen_data_collections_table():
    # Get collection descriptions from TCIA
    collection_descriptions = get_collection_descriptions()
    # Scrape the TCIA Data Collections page for collection metadata
    collection_metadata = scrape_tcia_data_collections_page()
    # Get a table to translate between the 'Collection' value in the scraped collection_metadata, and collection id's
    # of the collections_descriptions
    description_id_map = build_TCIA_to_Description_ID_Table(collection_metadata, collection_descriptions)

    # Load a table that maps from the collection id in collections to ids used by the TCIA API and
    # by IDC in collection bucket names
    # with open(args.file) as f:
    #     collection_ids = json.load(f)
    collection_ids = build_collections_id_table()
    rows = []
    for collection_id, collection_data in collection_metadata.items():
        # print(collection_id)
        ids = next((item for item in collection_ids if item["TCIA_Webapp_CollectionID"] == collection_id), None)
        if ids != None:
            collection_data['TCIA_Webapp_CollectionID'] = collection_id
            collection_data['TCIA_API_CollectionID'] = ids["TCIA_API_CollectionID"]
            collection_data['NBIA_CollectionID'] = ids["NBIA_CollectionID"]
            collection_data['IDC_GCS_CollectionID'] = ids["IDC_GCS_CollectionID"]
            collection_data['IDC_Webapp_CollectionID'] = ids["IDC_Webapp_CollectionID"]
            if ids["NBIA_CollectionID"] in collection_descriptions:
                collection_data['Description'] = collection_descriptions[description_id_map[collection_id]]

             # rows.append(json.dumps(collection_data))
            rows.append(collection_data)

    # metadata = '\n'.join(rows)
    return rows

# def gen_collections_table(args):
#     BQ_client = bigquery.Client()
#
#     metadata = build_metadata(args)
#     job = load_BQ_from_json(BQ_client, args.project, args.bqdataset_name, args.bqtable_name, metadata, data_collections_metadata_schema)
#     while not job.state == 'DONE':
#         print('Status: {}'.format(job.state))
#         time.sleep(args.period * 60)
#     print("{}: Completed collections metatdata upload \n".format(time.asctime()))

if __name__ == '__main__':
    # parser =argparse.ArgumentParser()
    # parser.add_argument('--file', default='{}/{}'.format(os.environ['PWD'], 'BQ/lists/collection_ids_mvp_wave0.json'),
    #                     help='Table to translate between collection IDs ')
    # # parser.add_argument('--collections', default='{}/lists/idc_mvp_wave_0.txt'.format(os.environ['PWD']),
    # #                     help="File containing list of IDC collection IDs or 'all' for all collections")
    # parser.add_argument('--bqdataset_name', default='idc_tcia_dev', help='BQ dataset name')
    # parser.add_argument('--bqtable_name', default='idc_tcia_data_collections_metadata_test', help='BQ table name')
    # parser.add_argument('--region', default='us', help='Dataset region')
    # parser.add_argument('--project', default='idc_peewee-dev-etl')
    #
    # args = parser.parse_args()
    # print("{}".format(args), file=sys.stdout)
    # gen_collections_table(args)
    gen_data_collections_table()

