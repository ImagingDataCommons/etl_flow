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

import json
from hfs.gen_patient_blobs import gen_patient_object


def gen_collection_object(args, sess, idc_version, collection):
    if not args.dst_bucket.blob(f"{collection.uuid}.idc").exists() and collection.sources.tcia:
        print(f'\t\tCollection {collection.collection_id} started')
        for patient in collection.patients:
            gen_patient_object(args, sess, idc_version, patient)
        collection_data = {
            "encoding": "1.0",
            "object_type": "collection",
            "tcia_api_collection_id": collection.collection_id,
            "idc_webapp_collection_id": collection.collection_id.lower().replace('-','_').replace(' ','_'),
            "uuid": collection.uuid,
            "md5_hash": collection.hashes.all_sources,
            "init_idc_version": collection.init_idc_version,
            "rev_idc_version": collection.rev_idc_version,
            "final_idc_version": collection.final_idc_version,
            "self_uri": f"{args.dst_bucket.name}/{collection.uuid}.idc",
            "children": {
                "count": len(collection.patients),
                "object_ids": [patient.submitter_case_id for patient in collection.patients if patient.sources.tcia],
                "gs":{
                    "region": "us-central1",
                    "bucket": f"gs://{args.dst_bucket.name}",
                    "gs_object_ids": [
                         f"{patient.uuid}.idc" for patient in collection.patients if patient.sources.tcia
                    ]
                }
            },
            # "parents": {
            #     "count": len(collection.versions),
            #     "object_ids": [f"idc_v{version.version}" for version in collection.versions],
            #     "gs": {
            #         "region": "us-central1",
            #         "bucket": f"{args.dst_bucket.name}",
            #         "gs_object_ids": [
            #             f"idc_v{version.version}.idc" for version in collection.versions
            #         ]
            #     }
            # },
        }
        blob = args.dst_bucket.blob(f"{collection.uuid}.idc").upload_from_string(json.dumps(collection_data))
        print(f'\t\tCollection {collection.collection_id} completed')
    else:
        print(f'\t\tCollection {collection.collection_id} skipped')
    return