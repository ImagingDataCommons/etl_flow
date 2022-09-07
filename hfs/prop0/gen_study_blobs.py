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
from hfs.gen_series_blobs import gen_series_object


def gen_study_object(args, sess, idc_version, study):
    # Create a "folder" blob to support gcsfuse and signed URLs
    if study.sources.tcia:
        if not args.dst_bucket.blob(f"{study.uuid}.idc").exists():
            print(f'\t\t\t\tStudy {study.study_instance_uid} started')
            for series in study.seriess:
                gen_series_object(args, sess, study, series)
            if 'root' in args.hfs_levels:
                study_data = {
                    "encoding": "1.0",
                    "object_type": "study",
                    "StudyInstanceUID": study.study_instance_uid,
                    "uuid": study.uuid,
                    "md5_hash": study.hashes.all_sources,
                    "init_idc_version": study.init_idc_version,
                    "rev_idc_version": study.rev_idc_version,
                    "final_idc_version": study.final_idc_version,
                    "self_uri": f"gs://{args.dst_bucket.name}/{study.uuid}.idc",
                    "drs_object_id": f"drs://nci-crdc.datacommons.io/dg.4DFC/{study.uuid}",
                    "children": {
                        "count": len(study.seriess),
                        "object_ids": [f"{series.series_instance_uid}" for series in study.seriess if series.sources.tcia],
                        "gs":{
                            "region": "us-central1",
                            "bucket": f"{args.dst_bucket.name}",
                            "gs_object_ids": [
                                f"{series.uuid}.idc" for series in study.seriess if series.sources.tcia
                            ]
                        },
                        "drs": {
                            "drs_server": "drs://nci-crdc.datacommons.io",
                            "drs_object_ids": [
                                f"dg.4DFC/{series.uuid}" for series in study.seriess if series.sources.tcia
                            ]
                        }
                    },
                    # "parents": {
                    #     "count": len(study.patients),
                    #     "object_ids": [patient.submitter_case_id for patient in study.patients],
                    #     "gs": {
                    #         "region": "us-central1",
                    #         "bucket": f"gs://{args.dst_bucket.name}",
                    #         "gs_object_ids": [
                    #             f"{patient.uuid}.idc" for patient in study.patients
                    #         ]
                    #     }
                    # },
                }
                blob = args.dst_bucket.blob(f"{study.uuid}.idc").upload_from_string(json.dumps(study_data))
            print(f'\t\t\t\tStudy {study.study_instance_uid} completed')



        # if args.hfs_level=='study':
        #     if not args.dst_bucket.blob(f"{study.uuid}/").exists():
        #         blob = args.dst_bucket.blob(f"{study.uuid}/").upload_from_string("")
        #         print(f'\t\t\t\tStudy folder {study.uuid}/ completed')
        #     else:
        #         print(f'\t\t\t\tStudy folder {study.uuid} skipped')
        #
        # if not args.dst_bucket.blob(f"{study.uuid}.idc").exists():
        #     print(f'\t\t\t\tStudy {study.study_instance_uid} started')
        #     for series in study.seriess:
        #         gen_series_object(args, sess, study, series)
        #     if 'root' in args.hfs_levels:
        #         study_data = {
        #             "encoding": "1.0",
        #             "object_type": "study",
        #             "StudyInstanceUID": study.study_instance_uid,
        #             "uuid": study.uuid,
        #             "md5_hash": study.hashes.all_sources,
        #             "init_idc_version": study.init_idc_version,
        #             "rev_idc_version": study.rev_idc_version,
        #             "final_idc_version": study.final_idc_version,
        #             "self_uri": f"gs://{args.dst_bucket.name}/{study.uuid}.idc",
        #             "drs_object_id": f"drs://nci-crdc.datacommons.io/dg.4DFC/{study.uuid}",
        #             "children": {
        #                 "count": len(study.seriess),
        #                 "object_ids": [f"{series.series_instance_uid}" for series in study.seriess if series.sources.tcia],
        #                 "gs":{
        #                     "region": "us-central1",
        #                     "bucket": f"{args.dst_bucket.name}",
        #                     "gs_object_ids": [
        #                         f"{series.uuid}.idc" for series in study.seriess if series.sources.tcia
        #                     ]
        #                 },
        #                 "drs": {
        #                     "drs_server": "drs://nci-crdc.datacommons.io",
        #                     "drs_object_ids": [
        #                         f"dg.4DFC/{series.uuid}" for series in study.seriess if series.sources.tcia
        #                     ]
        #                 }
        #             },
        #             # "parents": {
        #             #     "count": len(study.patients),
        #             #     "object_ids": [patient.submitter_case_id for patient in study.patients],
        #             #     "gs": {
        #             #         "region": "us-central1",
        #             #         "bucket": f"gs://{args.dst_bucket.name}",
        #             #         "gs_object_ids": [
        #             #             f"{patient.uuid}.idc" for patient in study.patients
        #             #         ]
        #             #     }
        #             # },
        #         }
        #         blob = args.dst_bucket.blob(f"{study.uuid}.idc").upload_from_string(json.dumps(study_data))
        #     print(f'\t\t\t\tStudy {study.study_instance_uid} completed')
        else:
            print(f'\t\t\t\tStudy {study.study_instance_uid} skipped')
    else:
        print(f'\t\t\tSkip study {study.study_instance_uid}; no tcia data')
    return