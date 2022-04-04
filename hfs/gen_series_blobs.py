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


def gen_series_object(args, sess, series):
    if not args.dst_bucket.blob(f"{series.uuid}.idc").exists():
        print(f'\t\t\t\t\tSeries {series.series_instance_uid} started')
        series_data = {
            "encoding": "v1",
            "object_type": "series",
            "SeriesInstanceUID": series.series_instance_uid,
            "source_doi": series.source_doi,
            "source_url": series.source_url,
            "uuid": series.uuid,
            "md5_hash": series.hashes.all_sources,
            "init_idc_version": series.init_idc_version,
            "rev_idc_version": series.rev_idc_version,
            "final_idc_version": series.final_idc_version,
            "self_uri": f"gs://{args.dst_bucket.name}/{series.uuid}.idc",
            "drs_object_id": f"dg.4DFC/{series.uuid}",
            "children": {
                "gs":{
                    "region": "us-central1",
                    "urls":
                        {
                            "bucket": f"{args.dst_bucket.name}",
                            "folder": f"{series.uuid}",
                            "instances":
                                [
                                    {"SOPInstance_UID": f"{instance.sop_instance_uid}",
                                     "blob_name": f"{instance.uuid}.idc"} for instance in series.instances
                                ]
                        }
                    },
                "drs":{
                    "urls":
                        {
                            "server": "drs://nci-crdc.datacommons.io",
                            "object_ids":
                                [
                                    {"SOPInstance_UID": f"{instance.sop_instance_uid}",
                                     "object_id": f"dg.4DFC/{instance.uuid}"} for instance in series.instances\
                                ]
                        }
                    }
                }
            }
        blob = args.dst_bucket.blob(f"{series.uuid}.idc").upload_from_string(json.dumps(series_data))
        print(f'\t\t\t\t\tSeries {series.series_instance_uid} completed')
    else:
        print(f'\t\t\t\t\tSeries {series.series_instance_uid} skipped')
    return