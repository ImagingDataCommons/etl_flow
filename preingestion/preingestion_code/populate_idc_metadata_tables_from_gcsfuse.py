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
from preingestion.preingestion_code.gen_hashes_sql import gen_hashes
from utilities.logging_config import successlogger, errlogger, progresslogger
from base64 import b64decode
from preingestion.validation_code.validate_analysis_result import validate_analysis_result
from preingestion.validation_code.validate_original_collection import validate_original_collection

from pydicom import dcmread

from utilities.sqlalchemy_helpers import sa_session
from google.cloud import storage

import pandas as pd

def build_instance(client, args, sess, series, instance_id, hash, size, blob_name):
    try:
        # Get the record of this instance if it exists
        instance = next(instance for instance in series.instances if instance.sop_instance_uid == instance_id)
        progresslogger.info(f'\t\t\t\tInstance {instance_id} exists')
    except StopIteration:
        instance = IDC_Instance()
        instance.sop_instance_uid = instance_id
        instance.excluded = False
        instance.redacted = False
        instance.mitigation = ""
        series.instances.append(instance)
        progresslogger.info(f'\t\t\t\tInstance {blob_name} added')
    instance.idc_version = args.version
    instance.ingestion_url = f'gs://{args.src_bucket}/{blob_name}'
    instance.hash = hash
    instance.size = size
    successlogger.info(blob_name)


def build_series(client, args, sess, study, series_id, instance_id, hash, size, blob_name):
    try:
        series = next(series for series in study.seriess if series.series_instance_uid == series_id)
        progresslogger.info(f'\t\t\tSeries {series_id} exists')
    except StopIteration:
        series = IDC_Series()
        series.series_instance_uid = series_id
        series.excluded = False
        series.redacted = False
        study.seriess.append(series)
        progresslogger.info(f'\t\t\tSeries {series_id} added')
    series.license_url = args.license['license_url']
    series.license_long_name = args.license['license_long_name']
    series.license_short_name = args.license['license_short_name']
    series.analysis_result = args.analysis_result
    series.source_doi = args.source_doi.lower()
    series.source_url = args.source_url.lower()
    series.versioned_source_doi = args.versioned_source_doi.lower()
    build_instance(client, args, sess, series, instance_id, hash, size, blob_name)
    return


def build_study(client, args, sess, patient, study_id, series_id, instance_id, hash, size, blob_name):
    try:
        study = next(study for study in patient.studies if study.study_instance_uid == study_id)
        progresslogger.info(f'\t\tStudy {study_id} exists')
    except StopIteration:
        study = IDC_Study()
        study.study_instance_uid = study_id
        study.redacted = False
        patient.studies.append(study)
        progresslogger.info(f'\t\tStudy {study_id} added')

    build_series(client, args, sess, study, series_id, instance_id, hash, size, blob_name)
    return


def build_patient(client, args, sess, collection, patient_id, study_id, series_id, instance_id, hash, size, blob_name):
    try:
        patient = next(patient for patient in collection.patients if patient.submitter_case_id == patient_id)
        progresslogger.info(f'\tPatient {patient_id} exists')
    except StopIteration:
        patient = IDC_Patient()
        patient.submitter_case_id = patient_id
        patient.redacted = False
        collection.patients.append(patient)
        progresslogger.info(f'\tPatient {patient_id} added')
    build_study(client, args, sess, patient, study_id, series_id, instance_id, hash, size, blob_name)
    return


def build_collection(client, args, sess, collection_id, patient_id, study_id, series_id, instance_id, hash, size, blob_name):
    collection = sess.query(IDC_Collection).filter(IDC_Collection.collection_id == collection_id).first()
    if not collection:
        # The collection is not currently in the DB, so add it
        collection = IDC_Collection()
        collection.collection_id = collection_id
        collection.redacted = False
        sess.add(collection)
        progresslogger.info(f'Collection {collection_id} added')
    else:
        progresslogger.info(f'Collection {collection_id} exists')
    build_patient(client, args, sess, collection, patient_id, study_id, series_id, instance_id, hash, size, blob_name)
    return


def prebuild_from_gcsfuse(args):
    client = storage.Client()
    src_bucket = storage.Bucket(client, args.src_bucket)

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
                        with src_bucket.blob(blob.name).open('rb') as f:
                        # with open(f"{args.mount_point}/{blob.name}", 'rb') as f:
                            try:
                                r = dcmread(f, specific_tags=['PatientID', 'StudyInstanceUID', 'SeriesInstanceUID', 'SOPInstanceUID'], stop_before_pixels=True)
                                patient_id = r.PatientID
                                study_id = r.StudyInstanceUID
                                series_id = r.SeriesInstanceUID
                                instance_id = r.SOPInstanceUID
                                if collection_map:
                                    collection_id = collection_map[patient_id]
                                elif not args.collection_id:
                                    # # If a collection_id is not provided, search the many-to-many Collection-Patient
                                    # # hierarchy for patient patient_id and get its collection_id
                                    # # This assumes that all pathology patients also are radiology patents which is not
                                    # # necessarily the case.
                                    # collection_id = sess.query(Collection.collection_id).distinct().join(
                                    #     Collection.patients). \
                                    #     filter(Patient.submitter_case_id == patient_id).one()[0]
                                    # collection_ids = collection_ids | {collection_id}

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
                        size = blob.size
                        build_collection(client, args, sess, collection_id, patient_id, study_id, series_id, instance_id, hash, size, blob.name)
        sess.commit()

    if args.validate:
        if not collection_ids:
            if collection_map:
                collection_ids = set(collection_map.values())
            else:
                collection_ids = [args.collection_id]
        if args.analysis_result:
            if validate_analysis_result(args) == -1:
                exit -1
        else:
            if validate_original_collection(args, collection_ids) == -1:
                exit -1

    if args.gen_hashes:
         gen_hashes()
    return

# if __name__ == '__main__':
#     parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
#     parser.add_argument('--version', default=settings.CURRENT_VERSION)
#     parser.add_argument('--src_bucket', default='cmb_pathology', help='Source bucket containing instances')
#     parser.add_argument('--mount_point', default='/mnt/disks/idc-etl/preeingestion_gcsfuse_mount_point', help='Directory on which to mount the bucket.\
#                 The script will create this directory if necessary.')
#     parser.add_argument('--subdir', \
#             default='/idc-conversion-outputs-cmb', \
#             help="Subdirectory of mount_point at which to start walking directory")
#     parser.add_argument('--subset_of_db_expected', default=True, help='If True, validation will not report an error if the instances in the bucket are a subset of the instance in the DB')
#     parser.add_argument('--collection_id', default='', help='collection_name of the collection or ID of analysis result to which instances belong.')
#     parser.add_argument('--collection_map', default='cmb_collection_map.csv', help='Optional csv file that maps GCS blob name to collection_name. If present, overrides collection_name')
#     parser.add_argument('--source_doi', default='10.5281/zenodo.11099111.', help='Concept DOI ')
#     parser.add_argument('--source_url', default='https://doi.org/10.7937/tcia.caem-ys80',\
#                         help='Info page URL')
#     parser.add_argument('--versioned-source_doi', default='10.5281/zenodo.11099112', help='Version specific DOI of this ingestion')
#     parser.add_argument('--license', default = {"license_url": 'https://creativecommons.org/licenses/by/3.0/',\
#             "license_long_name": "Creative Commons Attribution 3.0 Unported License", \
#             "license_short_name": "CC BY 3.0"}, help="(Sub-)Collection license")
#     parser.add_argument('--third_party', type=bool, default=False, help='True if from a third party analysis result')
#     parser.add_argument('--gen_hashes', default=True, help=' Generate hierarchical hashes of collection if True.')
#     parser.add_argument('--validate', type=bool, default=True, help='True if validation is to be performed')
#
#     args = parser.parse_args()
#     print("{}".format(args), file=sys.stdout)
#     args.client=storage.Client()
#
#     try:
#         # gcsfuse mount the bucket
#         pathlib.Path(args.mount_point).mkdir( exist_ok=True)
#         subprocess.run(['gcsfuse', '--implicit-dirs', args.src_bucket, args.mount_point])
#         prebuild_from_gcsfuse(args)
#     finally:
#         # Always unmount
#         subprocess.run(['fusermount', '-u', args.mount_point])


