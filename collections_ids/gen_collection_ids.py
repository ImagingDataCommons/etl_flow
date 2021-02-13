#!/usr/bin/env
#
# Copyright 2020, Institute for Systems Biology
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

import argparse
import sys
import os
import json
from utilities.tcia_scrapers import scrape_tcia_data_collections_page, build_TCIA_to_Description_ID_Table
from utilities.tcia_helpers import get_collection_descriptions, get_TCIA_collections


# Create a table that maps between corresponding NBIA, TCIA and IDC collection IDs

def repair_id(src_id):
    if src_id == 'AAPM RT-MAC Grand Challenge 2019':
        dst_id = 'AAPM RT-MAC'
    elif src_id == 'APOLLO-1-VA':
        dst_id = 'APOLLO'
    else:
        dst_id = src_id

    return dst_id

def build_collections_id_table():
    # if args.collections != "all":
    #     with open(args.collections) as f:
    #         collections = f.readlines()
    #     collections = [collection.strip() for collection in collections if collection[0] != '#' ]
    #
    # Get collection descriptions from NBIA
    collection_descriptions = get_collection_descriptions()
    # Scrape the TCIA Data Collections page for collection metadata
    collection_metadata = scrape_tcia_data_collections_page()
    # Build a table to translate between the 'Collection' collectionID  in the scraped TCIA collection_metadata, and collection id's
    # of the NBIA collections_descriptions
    description_id_map = build_TCIA_to_Description_ID_Table(collection_metadata, collection_descriptions)

    tcia_collection_ids = get_TCIA_collections()
    tcia_collection_ids.sort()
    TCIA_api_getCollectionValues_ids = {collection_id.split('(')[0].split('/')[0].strip(' ').replace(' ','_').lower().replace('_','-'):collection_id for collection_id in tcia_collection_ids}
    # Load a table that maps from the collection id in collections to ids used by the TCIA API and
    # by IDC in collection bucket names
    table = []
    try:
        keys = list(collection_metadata.keys())
        keys.sort()
        for collection_id in keys:
            # print(collection_id)
            if collection_id in description_id_map:
                dst_id = repair_id(collection_id)
                collection = {}
                try:
                    collection['NBIA_CollectionID'] = description_id_map[collection_id]
                except:
                    collection['NBIA_CollectionID'] = ""

                try:
                    collection['TCIA_Webapp_CollectionID'] = collection_id
                except:
                    collection['TCIA_Webapp_CollectionID'] = ""
                try:
                    collection['TCIA_getCollectionValuesID'] = TCIA_api_getCollectionValues_ids[dst_id.split('(')[0].split('/')[0].strip(' ').replace(' ','_').lower().replace('_','-')]
                except:
                    collection['TCIA_getCollectionValuesID'] = ""
                try:
                    collection['TCIA_API_CollectionID'] = dst_id.split('(')[0].split('/')[0].strip(' ').replace(' ','_')
                except:
                    collection['TCIA_API_CollectionID'] = ""
                try:
                    collection['IDC_GCS_CollectionID'] = dst_id.split('(')[0].split('/')[0].strip(' ').replace(' ','_').lower().replace('_','-')
                except:
                    collection['IDC_GCS_CollectionID'] = ""
                try:
                    collection['IDC_Webapp_CollectionID'] = dst_id.split('(')[0].split('/')[0].strip(' ').replace(' ','_').lower().replace('-','_')
                except:
                    collection['IDC_Webapp_CollectionID'] = ""

                table.append(collection)

            else:
                print("Rejected {}. Not in TCIA collections.".format(collection_id))
    except:
        pass

    return table

if __name__ == '__main__':
    collection_ids = build_collections_id_table()

