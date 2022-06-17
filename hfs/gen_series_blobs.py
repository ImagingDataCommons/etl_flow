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


def gen_series_object(args, sess, study, series):
    if series.sources.tcia:
        if args.hfs_level == 'series':
            # Create a "folder" blob to support gcsfuse and signed URLs
            if not args.dst_bucket.blob(f"{series.uuid}/").exists():
                blob = args.dst_bucket.blob(f"{series.uuid}/").upload_from_string("")
                print(f'\t\t\t\t\tSeries folder {series.uuid}/ completed')
            else:
                print(f'\t\t\t\t\tSeries folder {series.uuid} skipped')
        else:
            # Create a "folder" blob to support gcsfuse and signed URLs
            if not args.dst_bucket.blob(f"{study.uuid}/{series.uuid}/").exists():
                blob = args.dst_bucket.blob(f"{study.uuid}/{series.uuid}/").upload_from_string("")
                print(f'\t\t\t\t\tSeries folder {study.uuid}/{series.uuid}/ completed')
            else:
                print(f'\t\t\t\t\tSeries folder {study.uuid}/{series.uuid} skipped')
        if not args.dst_bucket.blob(f"{series.uuid}.idc").exists():
            # print(f'\t\t\t\t\tSeries {series.series_instance_uid} started')
            series_data = {
                "encoding": "1.0",
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
                "drs_object_id": f"drs://nci-crdc.datacommons.io/dg.4DFC/{series.uuid}",
                "instances": {
                    "count": len(series.instances),
                    "SOPInstanceUIDs": [instance.sop_instance_uid for instance in series.instances],
                    "gs":{
                        "region": "us-central1",
                        "bucket/folder": f"{args.dst_bucket.name}/{study.uuid}/{series.uuid}" if args.hfs_level=='study' \
                            else f"{args.dst_bucket.name}/{series.uuid}",
                        "gs_object_ids":
                            [
                                f"{instance.uuid}.dcm" for instance in series.instances
                            ]
                        },
                    # "drs": {
                    #     "drs_server": "drs://nci-crdc.datacommons.io",
                    #     "drs_object_ids":
                    #         [
                    #             f"{instance.uuid}" for instance in series.instances
                    #         ]
                    #     }
                    }
                }
            blob = args.dst_bucket.blob(f"{series.uuid}.idc").upload_from_string(json.dumps(series_data))
            print(f'\t\t\t\t\tSeries object {series.series_instance_uid} completed')
        else:
            print(f'\t\t\t\t\tSeries object {series.series_instance_uid} skipped')
    else:
        print(f'\t\t\tSkip series {series.series_instance_uid}; no tcia data')
    return