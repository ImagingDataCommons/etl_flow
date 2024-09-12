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

from idc.models import IDC_Collection, IDC_Patient, IDC_Study, IDC_Series, IDC_Instance, Collection, Patient
from utilities.logging_config import successlogger, errlogger, progresslogger
from base64 import b64decode
from preingestion.validation_code.validate_analysis_result import validate_analysis_result
from preingestion.validation_code.validate_original_collection import validate_original_collection

from pydicom import dcmread

from utilities.sqlalchemy_helpers import sa_session
from google.cloud import storage

import pandas as pd

def build_manifest(args):
    client = storage.Client()
    src_bucket = storage.Bucket(client, args.src_bucket)

    with open(args.manifest, 'w') as manifest:

        manifest.write('collection_id,patientID,StudyInstanceUID,SeriesInstanceUID,SOPInstanceUID,gcs_url,md5_hash\n');
        collection_ids = set()
        collection_map = {}
        if 'collection_map' in args and args.collection_map:
            # In some cases we must map patientID to collection_id
            df = pd.read_csv(args.collection_map)
            for index, row in df.iterrows():
                collection_map[row['patientID']] = row['collection_id']

        with sa_session(echo=False) as sess:
            client = storage.Client()
            iterator = client.list_blobs(src_bucket, prefix=args.subdir)
            for page in iterator.pages:
                if page.num_items:
                    for blob in page:
                        if not blob.name.endswith(('DICOMDIR', '.txt', '.csv', '/')):
                            with open(f"{args.mount_point}/{blob.name}", 'rb') as f:
                                try:
                                    r = dcmread(f, specific_tags=['PatientID', 'StudyInstanceUID', 'SeriesInstanceUID', 'SOPInstanceUID'], stop_before_pixels=True)
                                    patient_id = r.PatientID
                                    study_id = r.StudyInstanceUID
                                    series_id = r.SeriesInstanceUID
                                    instance_id = r.SOPInstanceUID
                                    if collection_map:
                                        collection_id = collection_map[patient_id]
                                    elif not args.collection_id:
                                        # If a collection_id is not provided, search the many-to-many Collection-Patient
                                        # hierarchy for patient patient_id and get its collection_id
                                        # This assumes that all pathology patients also are radiology patents which is not
                                        # necessarily the case.
                                        collection_id = sess.query(Collection.collection_id).distinct().join(
                                            Collection.patients). \
                                            filter(Patient.submitter_case_id == patient_id).one()[0]
                                        collection_ids = collection_ids | {collection_id}
                                    else:
                                        collection_id = args.collection_id
                                except Exception as exc:
                                    errlogger.error(f'pydicom failed for {blob.name}: {exc}')
                                    continue
                            hash = b64decode(blob.md5_hash).hex()
                            manifest.write(f'{collection_id},{patient_id},{study_id},{series_id},{instance_id},{blob.name},{hash}\n')



if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--version', default=settings.CURRENT_VERSION)
    parser.add_argument('--src_bucket', default='gtex_pathology', help='Source bucket containing instances')
    parser.add_argument('--mount_point', default='/mnt/disks/idc-etl/preeingestion_gcsfuse_mount_point', help='Directory on which to mount the bucket.\
                The script will create this directory if necessary.')
    parser.add_argument('--subdir', \
            default='v1', \
            help="Subdirectory of mount_point at which to start walking directory")
    parser.add_argument('--collection_id', default='GTEx', help='collection_name of the collection or ID of analysis result to which instances belong.')
    parser.add_argument('--manifest', default='./gtex_generated_manifest.csv', help='Manifest file name')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    args.client=storage.Client()

    try:
        # gcsfuse mount the bucket
        pathlib.Path(args.mount_point).mkdir( exist_ok=True)
        subprocess.run(['gcsfuse', '--implicit-dirs', args.src_bucket, args.mount_point])
        build_manifest(args)
    finally:
        # Always unmount
        subprocess.run(['fusermount', '-u', args.mount_point])


