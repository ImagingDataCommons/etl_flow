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

# Adds/replaces data to the idc_collection/_patient/_study/_series/_instance DB tables
# from a specified bucket.
#
# For this purpose, the bucket containing the instance blobs is gcsfuse mounted, and
# pydicom is then used to extract needed metadata.
#
# The script walks the directory hierarchy from a specified subdirectory of the
# gcsfuse mount point

import sys
import settings
import argparse
import pathlib
import subprocess

from idc.models import IDC_Collection, IDC_Patient, IDC_Study, IDC_Series, IDC_Instance, Collection, Patient, Study
from utilities.logging_config import successlogger, errlogger, progresslogger
from base64 import b64decode
from preingestion.validation_code.validate_analysis_result import validate_analysis_result
from preingestion.validation_code.validate_original_collection import validate_original_collection
from utilities.logging_config import progresslogger
import pandas as pd

from pydicom import dcmread

from utilities.sqlalchemy_helpers import sa_session
from google.cloud import storage

import pandas as pd

def build_manifest(args):
    client = storage.Client()
    src_bucket = storage.Bucket(client, args.src_bucket)
    manifest = pd.DataFrame(columns=['collection_id', 'patientID', 'StudyInstanceUID', 'SeriesInstanceUID', 'SOPInstanceUID', 'ingestion_url', 'md5_hash'])
    collection_ids = set()
    collection_map = {}

    with (sa_session(echo=False) as sess):
        client = storage.Client()
        iterator = client.list_blobs(src_bucket, prefix=args.subdir)
        for page in iterator.pages:
            if page.num_items:
                for blob in page:
                    if not blob.name.endswith(('DICOMDIR', '.txt', '.csv', '/')) and args.inclusion_filter in blob.name:
                        with src_bucket.blob(blob.name).open('rb') as f:
                            try:
                                r = dcmread(f, specific_tags=['PatientID', 'StudyInstanceUID', 'SeriesInstanceUID', 'SOPInstanceUID'], stop_before_pixels=True)
                                patient_id = r.PatientID
                                study_id = r.StudyInstanceUID
                                series_id = r.SeriesInstanceUID
                                instance_id = r.SOPInstanceUID
                                # if collection_map:
                                #     collection_id = collection_map[patient_id]
                                if not args.collection_id:
                                     # If a collection_id is not provided, search the many-to-many Collection-Patient-Study
                                    # hierarchy for study and get its collection_id
                                    # We cannot use the Collection-Patient hierarchy because the patient_id is not unique
                                    #
                                    collection_id = sess.query(Collection.collection_id).distinct().join(
                                        Collection.patients).join(Patient.studies). \
                                        filter(Study.study_instance_uid == study_id).one()[0]
                                    collection_ids = collection_ids | {collection_id}

                                else:
                                    collection_id = args.collection_id
                            except Exception as exc:
                                errlogger.error(f'pydicom failed for {blob.name}: {exc}')
                                continue
                        hash = b64decode(blob.md5_hash).hex()
                        manifest.loc[len(manifest)] = [collection_id, patient_id, study_id, series_id, instance_id, blob.name, hash]

    return manifest



