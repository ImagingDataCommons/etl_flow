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

from logging import INFO
# from google.cloud import bigquery, storage
from idc.models import Base, Version, Collection, Patient, Study, Series, Instance
from google.cloud import storage
from sqlalchemy.orm import Session
from python_settings import settings
from sqlalchemy import create_engine, update
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
      c_uuid uuid,
      c_hashes.all_hash md5_hash,
      c_init_idc_version init_idc_version,
      c_rev_idc_version rev_idc_version,
      c_final_idc_version final_idc_version
    FROM
      `idc-dev-etl.whc_dev.hfs_all_joined`
    WHERE
      idc_version = {version} AND collection_id in {args.collections}
    ORDER BY collection_id
    """
    # urls = list(client.query(query))
    query_job = client.query(query)  # Make an API request.
    query_job.result()  # Wait for the query to complete.
    destination = query_job.destination
    destination = client.get_table(destination)
    return destination


def gen_version_object(args, idc_version, previous_idc_version, md5_hash):
    # bq_client = bigquery.Client()
    # destination = get_collections_in_version(args, idc_version)
    # collections = [collection for page in bq_client.list_rows(destination, page_size=args.batch).pages for collection in page ]

    version = {
        "encoding": "v1",
        "object_type": "version",
        "version_id": idc_version,
        "md5_hash": md5_hash,
        "self_uri": f"gs://{args.dst_bucket.name}/idc_v{idc_version}.idc",
        "collections": {
            "gs":{
                "region": "us-central1",
                "urls":
                    {
                        "bucket": f"{args.dst_bucket.name}",
                        "blobs":
                            [
                                {"idc_webapp_collection_id": collection.collection_id.lower().replace('-','_').replace(' ','_'),
                                 "blob_name": f"{collection.uuid}.idc"} for collection in collections
                            ]
                    }
                }
            }
        }
    blob = args.dst_bucket.blob(f"idc_v{idc_version}.idc").upload_from_string(json.dumps(version))
    for collection in collections:
        gen_collection_object(args,
            collection.collection_id,
            collection.uuid,
            collection.md5_hash,
            collection.init_idc_version,
            collection.rev_idc_version,
            collection.final_idc_version)
    print(f'\tVersion {idc_version}')
    return