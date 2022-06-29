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
from hfs.gen_study_blobs import gen_study_object


def gen_patient_object(args, sess, idc_version, patient):
    if patient.sources.tcia:
        if not args.dst_bucket.blob(f"{patient.uuid}.idc").exists():
            print(f'\t\t\tPatient {patient.submitter_case_id} started')
            for study in patient.studies:
                gen_study_object(args, sess, idc_version, study)
            patient_data = {
                "encoding": "1.0",
                "object_type": "patient",
                "submitter_case_id": patient.submitter_case_id,
                "idc_case_id": patient.idc_case_id,
                "uuid": patient.uuid,
                "md5_hash": patient.hashes.all_sources,
                "init_idc_version": patient.init_idc_version,
                "rev_idc_version": patient.rev_idc_version,
                "final_idc_version": patient.final_idc_version,
                "self_uri": f"gs://{args.dst_bucket.name}/{patient.uuid}.idc",
                "children": {
                    "count": len(patient.studies),
                    "object_ids": [study.study_instance_uid for study in patient.studies if study.sources.tcia],
                    "gs":{
                        "region": "us-central1",
                        "bucket": f"{args.dst_bucket.name}",
                        "gs_object_ids": [
                            f"{study.uuid}.idc" for study in patient.studies if study.sources.tcia
                        ]
                    },
                    "drs": {
                        "drs_server": "drs://nci-crdc.datacommons.io",
                        "drs_object_ids": [
                            f"dg.4DFC/{study.uuid}" for study in patient.studies if study.sources.tcia
                        ]
                    }
                },
                # "parents": {
                #     "count": len([collection for collection in patient.collections if
                #                   collection.collection_id in args.collections]),
                #     "object_ids": [collection.collection_id.lower().replace('-', '_').replace(' ', '_') \
                #                    for collection in patient.collections if
                #                    collection.collection_id in args.collections],
                #     "gs": {
                #         "region": "us-central1",
                #         "bucket": f"{args.dst_bucket.name}",
                #         "gs_object_ids": [
                #             f"{collection.uuid}.idc" \
                #             for collection in patient.collections if collection.collection_id in args.collections
                #         ]
                #     }
                # },
            }
            blob = args.dst_bucket.blob(f"{patient.uuid}.idc").upload_from_string(json.dumps(patient_data))
            print(f'\t\t\tPatient {patient.submitter_case_id} completed')
        else:
            print(f'\t\t\tPatient {patient.submitter_case_id} skipped')
    else:
        print(f'\t\t\tSkip patient {patient.submitter_case_id}; no tcia data')
    return