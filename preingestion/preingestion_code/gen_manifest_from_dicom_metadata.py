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

from idc.models import IDC_Collection, IDC_Patient, IDC_Study, IDC_Series, IDC_Instance, Collection, Patient, Study
from utilities.logging_config import successlogger, errlogger, progresslogger
from base64 import b64decode
from utilities.logging_config import progresslogger
from ingestion.utilities.utils import streaming_md5_hasher

from pydicom import dcmread

from utilities.sqlalchemy_helpers import sa_session
from google.cloud import storage

import pandas as pd

def build_manifest(args, manifest=None):
    client = storage.Client()
    src_bucket = storage.Bucket(client, args.src_bucket)
    # try:
    #     manifest = pd.read_csv('/mnt/disks/idc-etl/generated_partial_revision.csv')
    # except:
    #     manifest = pd.DataFrame(columns=['collection_id', 'patientID', 'StudyInstanceUID', 'SeriesInstanceUID', 'SOPInstanceUID', 'ingestion_url', 'md5_hash'])
    #     manifest.tail(1).to_csv('/mnt/disks/idc-etl/generated_partial_revision.csv', mode='a', header=False, index=False)

    if manifest is None:
        manifest = pd.DataFrame(columns=['collection_id', 'patientID', 'StudyInstanceUID', 'SeriesInstanceUID', 'SOPInstanceUID', 'ingestion_url', 'md5_hash'])
    done_instances = manifest['ingestion_url'].tolist()
    collection_ids = set()
    collection_map = {}

    with (sa_session(echo=False) as sess):
        client = storage.Client()
        iterator = client.list_blobs(src_bucket, prefix=args.subdir)
        for page in iterator.pages:
            if page.num_items:
                for blob in page:
                    ingestion_url = f'gs://{args.src_bucket}/{blob.name}'
                    if ingestion_url not in done_instances:
                        if not blob.name.endswith(('DICOMDIR', '.txt', '.csv', '/', 'DS_Store')) and args.inclusion_filter in blob.name:
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
                                try:
                                    hash = b64decode(blob.md5_hash).hex()
                                except TypeError:
                                    # Can't get md5 hash for some blobs (maybe multipart copied/)
                                    # So try to compute it
                                    breakpoint()
                                    hash = streaming_md5_hasher(blob)

                                progresslogger.info(f'Added {blob.name} to manifest')
                                # blob_subname = blob.name.removeprefix(f'{args.subdir}/') if args.subdir else blob.name
                                manifest.loc[len(manifest)] = [collection_id, patient_id, study_id, series_id, \
                                       instance_id, ingestion_url, hash]
                                # manifest.tail(1).to_csv('/mnt/disks/idc-etl/generated_partial_revision.csv', mode='a', header=False, index=False)
                    else:
                        progresslogger.info((f'Skipping {blob.name}'))

    return manifest



