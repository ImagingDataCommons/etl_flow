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


def gen_version_object(args, sess, version):
    idc_version = version.version
    if not args.dst_bucket.blob(f"idc_v{version.version}.idc").exists():
        print(f'\tVersion {version.version} started')
        for collection in version.collections:
            if collection.collection_id in args.collections:
                gen_collection_object(args, sess, idc_version, collection)
        version_data = {
            "encoding": "1.0",
            "object_type": "version",
            "version_id": version.version,
            "md5_hash": version.hashes.all_sources,
            "self_uri": f"{args.dst_bucket.name}/idc_v{version.version}.idc",
            "children": {
                "count": len([collection for collection in version.collections if collection.collection_id in args.collections]),
                "object_ids": [collection.collection_id.lower().replace('-', '_').replace(' ', '_') \
                                              for collection in version.collections if collection.collection_id in args.collections],
                "gs":{
                    "region": "us-central1",
                    "bucket": f"{args.dst_bucket.name}",
                    "gs_object_ids": [
                        f"{collection.uuid}.idc" \
                            for collection in version.collections if collection.collection_id in args.collections
                    ]
                }
            },
            # "parents": {
            #     "count": 1,
            #     "object_ids": ['root'],
            #     "gs": {
            #         "region": "us-central1",
            #         "bucket": f"{args.dst_bucket.name}",
            #         "gs_object_ids": [
            #             'idc.idc'
            #         ]
            #     }
            # }
        }

        blob = args.dst_bucket.blob(f"idc_v{version.version}.idc").upload_from_string(json.dumps(version_data))
        print(f'\tVersion {version.version} completed')
    else:
        print(f'\tVersion {version.version} exists')
    return