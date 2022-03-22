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

import os
import argparse
import logging
from logging import INFO
from google.cloud import bigquery, storage
import json
from gen_collection_blobs import gen_collection_object


# Copy the blobs that are new to a version from dev pre-staging buckets
# to dev staging buckets.

def get_collections_in_version(args, version):
    client = bigquery.Client()
    query = f"""
    SELECT
      distinct
      collection_id,
      uuid,
      c_hashes['all_sources'] md5_hash,
      c_init_idc_version init_idc_version,
      c_rev_idc_version rev_idc_version,
      c_final_idc_version final_idc_version
    FROM
      `idc-dev-etl.idc_v{args.version}_dev.all_joined`
    WHERE
      version = {version} 
    """
    # urls = list(client.query(query))
    query_job = client.query(query)  # Make an API request.
    query_job.result()  # Wait for the query to complete.
    destination = query_job.destination
    destination = client.get_table(destination)
    return destination


def gen_version_object(args, idc_version, previous_idc_version, md5_hash):
    bq_client = bigquery.Client()
    destination = get_collections_in_version(args, idc_version)
    collections = [collection for page in bq_client.list_rows(destination, page_size=args.batch).pages for collection in page ]
    version = {
        "encoding": "v1",
        "object_type": "version",
        "version_id": idc_version,
        "md5_hash": md5_hash,
        "self_uri": f"gs://{args.dst_bucket.name}/v{idc_version}.idc",
        "children": {
            "gs":{
                "region": "us-central1",
                "collections":{
                    f"gs://{args.dst_bucket}": [
                        collection.c_uuid for collection in collections
                    ]
                }
            }
        },
        "parents": {
            "gs": {
                "region": "us-central1",
                "roots": [
                    f"gs://{args.dst_bucket}/idc.idc"
                ]
            }
        }
    }
    blob = args.dst_bucket.blob(f"v{idc_version}.idc").upload_from_string(json.dumps(version))
    for collection in collections:
        if not args.dev_bucket.blob(f'{collection.uuid}.dcm').exists():
            gen_collection_object(args,
                collection.collection_id,
                collection.uuid,
                collection.md5_hash,
                collection.init_idc_version,
                collection.rev_idc_version,
                collection.final_idc_version)

    return