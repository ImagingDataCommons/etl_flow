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
    if not args.dst_bucket.blob(f"{patient.uuid}.idc").exists():
        print(f'\t\t\tPatient {patient.submitter_case_id} started')
        for study in patient.studies:
            gen_study_object(args, sess, idc_version, study)
        patient_data = {
            "encoding": "v1",
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
                "gs":{
                    "region": "us-central1",
                    "urls":
                        {
                            "bucket": f"{args.dst_bucket.name}",
                            "studies":
                                [
                                    {"StudyInstanceUID": f"{study.study_instance_uid}",
                                     "object_id": f"{study.uuid}.idc"} \
                                    for study in patient.studies
                                ]
                        }
                    },
                "drs":{
                    "urls":
                        {
                            "server": "drs://nci-crdc.datacommons.io",
                            "studies":
                                [
                                    {"StudyInstanceUID": f"{study.study_instance_uid}",
                                     "object_id": f"dg.4DFC/{study.uuid}"} for study in patient.studies
                                ]
                        }
                    }
                }
            }
        blob = args.dst_bucket.blob(f"{patient.uuid}.idc").upload_from_string(json.dumps(patient_data))
        print(f'\t\t\tPatient {patient.submitter_case_id} completed')
    else:
        print(f'\t\t\tPatient {patient.submitter_case_id} skipped')
    return