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

import sys
import os
import argparse
from pathlib import Path
import time
from datetime import datetime, timezone, timedelta
import logging
import pydicom
import hashlib
from subprocess import run, PIPE
import shutil
import requests
from base64 import b64decode
from pydicom.errors import InvalidDicomError
from uuid import uuid4
from google.cloud import storage
from idc.models import Version, Collection, Patient, Study, Series, Instance, sql_engine
from sqlalchemy import select
from sqlalchemy.orm import Session
from utilities.tcia_helpers import  get_TCIA_collections, get_TCIA_patients_per_collection, get_TCIA_studies_per_patient, \
    get_TCIA_series_per_study, get_TCIA_instance_uids_per_series, get_TCIA_instances_per_series, get_series_info
from utilities.identify_third_party_series import get_data_collection_doi, get_analysis_collection_dois


BUF_SIZE = 65536
def md5_hasher(file_path):
    md5 = hashlib.md5()
    with open(file_path, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            md5.update(data)
    return md5.hexdigest()


def rollback_copy_to_bucket(args, series):
    client = storage.Client()
    bucket = client.bucket(args.prestaging_bucket)
    for instance in series.instances:
        try:
            results = bucket.blob(f'{instance.instance_uuid}.dcm').delete()
        except:
            logging.error('Failed to delete blob %s.dcm during validation rollback',instance.instance_uuid)
            raise


def validate_series_in_gcs(storage_client, args, collection, patient, study, series):
    # blobs_info = get_series_info(storage_client, args.project, args.staging_bucket)
    bucket = storage_client.get_bucket(args.prestaging_bucket)
    try:
        for instance in series.instances:
            blob = bucket.blob(f'{instance.instance_uuid}.dcm')
            blob.reload()
            assert instance.instance_hash == b64decode(blob.md5_hash).hex()
            assert instance.instance_size == blob.size

    except Exception as exc:
        rollback_copy_to_bucket(args, series)
        logging.error('GCS validation failed for %s/%s/%s/%s/%s',
            collection.tcia_api_collection_id, patient.submitter_case_id, study.study_instance_uid, instance.sop_instance_uid)
        raise exc


# Copy the series instances downloaded from TCIA/NBIA from disk to the prestaging bucket
def copy_disk_to_prestaging_bucket(args, series):
    # Do the copy as a subprocess in order to use the gsutil -m option
    try:
        # Copy the series to GCS
        src = "{}/{}/*".format(args.dicom, series.series_instance_uid)
        dst = "gs://{}/".format(args.prestaging_bucket)
        result = run(["gsutil", "-m", "-q", "cp", src, dst])
        if result.returncode < 0:
            logging.error('\tcopy_disk_to_prestaging_bucket failed for series %s', series.series_instance_uid)
            raise RuntimeError('copy_disk_to_prestaging_bucket failed for series %s', series.series_instance_uid)
        logging.debug(("Uploaded instances to GCS"))
    except Exception as exc:
        logging.error("\tCopy to prestage bucket failed for series %s", series.series_instance_uid)
        raise RuntimeError("Copy to prestage bucketfailed for series %s", series.series_instance_uid) from exc


# Copy a completed collection from the prestaging bucket to the staging bucket
def copy_prestaging_to_staging_bucket(args, collection):
    try:
        # Copy the series to GCS
        src = "gs://{}/*".format(args.prestaging_bucket)
        dst = "gs://{}/".format(args.staging_bucket)
        result = run(["gsutil", "-m", "-q", "cp", src, dst])
        if result.returncode < 0:
            logging.error('\tcopy_prestaging_to_staging_bucket failed for collection %s', collection.tcia_api_collection_id)
            raise RuntimeError('copy_prestaging_to_staging_bucket failed for collection %s', collection.tcia_api_collection_id)
        logging.debug(("Uploaded instances to GCS"))
    except Exception as exc:
        logging.error("\tCopy from prestaging to staging bucket for collection %s failed", collection.tcia_api_collection_id)
        raise RuntimeError("Copy from prestaging to staging bucket for collection %s failed", collection.tcia_api_collection_id) from exc


def copy_staging_bucket_to_final_bucket(args, version):
    try:
        # Copy the series to GCS
        src = "gs://{}/*".format(args.staging_bucket)
        dst = "gs://{}/".format(args.bucket)
        result = run(["gsutil", "-m", "-q", "cp", src, dst])
        if result.returncode < 0:
            logging.error('\tcopy_staging_bucket_to_final_bucket failed for version %s', version.idc_version_number)
            raise RuntimeError('copy_staging_bucket_to_final_bucket failed for version %s', version.idc_version_number)
        logging.debug(("Uploaded instances to GCS"))
    except Exception as exc:
        logging.error("\tCopy from prestaging to staging bucket for collection %s failed", version.idc_version_number)
        raise RuntimeError("Copy from prestaging to staging bucket for collection %s failed", version.idc_version_number) from exc


def empty_bucket(bucket):
    try:
        src = "gs://{}/*".format(bucket)
        run(["gsutil", "-m", "-q", "rm", src])
        logging.debug(("Emptied bucket %s", bucket))
    except Exception as exc:
        logging.error("\tFailed to empty bucket %s", bucket)
        raise RuntimeError("Failed to empty bucket %s", bucket) from exc


def copy_to_gcs(args, collection, patient, study, series):
    storage_client = storage.Client()

    # Delete the zip file before we copy to GCS so that it is not copied
    os.remove("{}/{}.zip".format(args.dicom, series.series_instance_uid))

    # Copy the instances to the staging bucket
    copy_disk_to_prestaging_bucket(args, series)

    # Delete the series from disk
    shutil.rmtree("{}/{}".format(args.dicom, series.series_instance_uid), ignore_errors=True)

    # validate_series_in_gcs(storage_client, args, collection, patient, study, series)

    # # If it passed validation, move to the final bucket
    # copy_to_final_bucket(args, series)
    #
    # # Delete the contents of the staging bucket
    # # bucket = storage_client.bucket(args.staging_bucket)
    # # bucket.delete_blobs(bucket.list_blobs())
    # empty_staging_bucket(args)


def build_instances(sess, args, version, collection, patient, study, series):
    # Download a zip of the instances in a series
    # It will be write the zip to a file dicom/<series_instance_uid>.zip in the
    # working directory, and expand the zip to directory dicom/<series_instance_uid>

    # When TCIA provided series timestamps, we'll us that for instance_timestamp.
    now = datetime.now(timezone.utc)

    # Delete the series from disk in case it is there from a previous run
    try:
        shutil.rmtree("{}/{}".format(args.dicom, series.series_instance_uid), ignore_errors=True)
    except:
        # It wasn't there
        pass

    get_TCIA_instances_per_series(series.series_instance_uid)

    # Get a list of the files from the download
    dcms = [dcm for dcm in os.listdir("{}/{}".format(args.dicom, series.series_instance_uid))]

    # Ensure that the zip has the expected number of instances
    if not len(dcms) == len(series.instances):
        logging.error("\tInvalid zip file for %s/%s", series.series_instance_uid)
        raise RuntimeError("\tInvalid zip file for %s/%s", series.series_instance_uid)
    logging.debug(("Series %s download successful", series.series_instance_uid))

    # TCIA file names are based on the position of the image in a scan. We need to extract the SOPInstanceUID
    # so that we can know the instance.
    # Use pydicom to open each file to get its UID and rename the file with its associated instance_uuid that we
    # generated when we expanded this series.

    # Replace the TCIA assigned file name
    # Also compute the md5 hash and length in bytes of each
    for dcm in dcms:
        try:
            SOPInstanceUID = pydicom.read_file("{}/{}/{}".format(args.dicom, series.series_instance_uid, dcm)).SOPInstanceUID
        except InvalidDicomError:
            logging.error("\tInvalid DICOM file for %s", series.series_instance_uid)
            raise RuntimeError("\tInvalid DICOM file for %s", series.series_instance_uid)
        instance = next(instance for instance in series.instances if instance.sop_instance_uid == SOPInstanceUID)
        instance_uuid = instance.instance_uuid
        file_name = "./{}/{}/{}".format(args.dicom, series.series_instance_uid, dcm)
        blob_name = "./{}/{}/{}.dcm".format(args.dicom, series.series_instance_uid, instance_uuid)
        os.renames(file_name, blob_name)

        with open(blob_name,'rb') as f:
            instance.instance_hash = md5_hasher(blob_name)
            instance.instance_size = Path(blob_name).stat().st_size
            instance.instance_timestamp = datetime.utcnow()
    logging.debug("Renamed all files for series %s", series.series_instance_uid)

    copy_to_gcs(args, collection, patient, study, series)


def expand_series(sess, series):
    instances = get_TCIA_instance_uids_per_series(series.series_instance_uid)
    # If the study is new, then all the studies are new
    if series.is_new:
        for instance in instances:
            instance_uuid = uuid4().hex
            series.instances.append(Instance(series_id=series.id,
                                         idc_version_number=series.idc_version_number,
                                         instance_timestamp=datetime(1970,1,1,0,0,0),
                                         sop_instance_uid=instance['SOPInstanceUID'],
                                         instance_uuid=instance_uuid,
                                         gcs_url=f'gs://idc_dev/{instance_uuid}',
                                         instance_hash="",
                                         instance_size=0,
                                         revised=True,
                                         done=False,
                                         is_new=True,
                                         expanded=False))
        series.expanded = True
        sess.commit()
    else:
        # Need to add code for the case that the object is not new
        # Do that when we create version 3
        logging.error("Add code to hanndle not-new case in expand_series")


def build_series(sess, args, version, collection, patient, study, series, data_collection_doi, analysis_collection_dois):
    if not series.done:
        begin = time.time()
        if not series.expanded:
            expand_series(sess, series)
        logging.info("      Series %s; %s instances", series.series_instance_uid, len(series.instances))
        build_instances(sess, args, version, collection, patient, study, series)
        series.series_instances = len(series.instances)
        series.series_timestamp = min(instance.instance_timestamp for instance in series.instances)
        series.done = True
        sess.commit()
        duration = str(timedelta(seconds=(time.time() - begin)))
        logging.info("      Series %s completed in %s", series.series_instance_uid, duration)
    else:
        logging.info("      Series %s previously built", series.series_instance_uid)


def expand_study(sess, collection, patient, study, data_collection_doi, analysis_collection_dois):
    rows = get_TCIA_series_per_study(collection.tcia_api_collection_id, patient.submitter_case_id, study.study_instance_uid)
    # If the study is new, then all the studies are new
    if study.is_new:
        for row in rows:
            study.seriess.append(Series(study_id=study.id,
                                         idc_version_number=study.idc_version_number,
                                         series_timestamp=datetime(1970,1,1,0,0,0),
                                         series_instance_uid=row['SeriesInstanceUID'],
                                         series_uuid=uuid4().hex,
                                         series_instances=0,
                                         source_doi=analysis_collection_dois[row['SeriesInstanceUID']] \
                                             if row['SeriesInstanceUID'] in analysis_collection_dois
                                             else data_collection_doi,
                                         revised=True,
                                         done=False,
                                         is_new=True,
                                         expanded=False))
        study.expanded = True
        sess.commit()
    else:
        # Need to add code for the case that the object is not new
        # Do that when we create version 3
        logging.error("Add code to hanndle not-new case in expand_study")


def build_study(sess, args, version, collection, patient, study, data_collection_doi, analysis_collection_dois):
    if not study.done:
        begin = time.time()
        if not study.expanded:
            expand_study(sess, collection, patient, study, data_collection_doi, analysis_collection_dois)
        logging.info("    Study %s; %s series", study.study_instance_uid, len(study.seriess))
        for series in study.seriess:
            build_series(sess, args, version, collection, patient, study, series, data_collection_doi, analysis_collection_dois)
        study.study_instances = sum([series.series_instances for series in study.seriess])
        study.study_timestamp = min([series.series_timestamp for series in study.seriess])
        study.done = True
        sess.commit()
        duration = str(timedelta(seconds=(time.time() - begin)))
        logging.info("    Study %s completed in %s", study.study_instance_uid, duration)
    else:
        logging.info("    Study %s previously built", study.study_instance_uid)


def expand_patient(sess, collection, patient):
    studies = get_TCIA_studies_per_patient(collection.tcia_api_collection_id, patient.submitter_case_id)
    # If the patient is new, then all the studies are new
    if patient.is_new:
        for study in studies:
            patient.studies.append(Study(patient_id=patient.id,
                                         idc_version_number=patient.idc_version_number,
                                         study_timestamp = datetime(1970,1,1,0,0,0),
                                         study_instance_uid=study['StudyInstanceUID'],
                                         study_uuid=uuid4().hex,
                                         study_instances = 0,
                                         revised=True,
                                         done=False,
                                         is_new=True,
                                         expanded=False))
        patient.expanded = True
        sess.commit()
    else:
        # Need to add code for the case that the object is not new
        # Do that when we create version 3
        logging.error("Add code to hanndle not-new case in expand_patient")


def build_patient(sess, args, version, collection, patient, data_collection_doi, analysis_collection_dois):
    if not patient.done:
        begin = time.time()
        if not patient.expanded:
            expand_patient(sess, collection, patient)
        logging.info("  Patient %s; %s studies", patient.submitter_case_id, len(patient.studies))
        for study in patient.studies:
            build_study(sess, args, version, collection, patient, study, data_collection_doi, analysis_collection_dois)
        patient.patient_timestamp = min([study.study_timestamp for study in patient.studies])

        patient.done = True
        sess.commit()
        duration = str(timedelta(seconds=(time.time() - begin)))
        logging.info("  Patient %s completed in %s", patient.submitter_case_id, duration)
    else:
        logging.info("  Patient %s previously built", patient.submitter_case_id)


def expand_collection(sess, collection):
    # If we are here, we are beginning work on this collection.
    # GCS data for the collection being built is accumulated in the prestaging bucket,
    # args.prestaging bucket.
     # Since we are starting, delete everything from the prestaging bucket.
    empty_bucket(args.prestaging_bucket)
    patients = get_TCIA_patients_per_collection(collection.tcia_api_collection_id)
    if collection.is_new:
        # If the collection is new, then all the patients are new
        if collection.is_new:
            for patient in patients:
                collection.patients.append(Patient(collection_id = collection.id,
                                              idc_version_number = collection.idc_version_number,
                                              patient_timestamp = datetime(1970,1,1,0,0,0),
                                              submitter_case_id = patient['PatientId'],
                                              crdc_case_id = "",
                                              revised = True,
                                              done = False,
                                              is_new = True,
                                              expanded = False))
        collection.expanded = True
        sess.commit()
    else:
        # Need to add code for the case that the object is not new
        # Do that when we create version 3
        logging.error("Add code to hanndle not-new case in expand_collection")


def build_collection(sess, args, version, collection):
    # if not collection.done:
    if collection.tcia_api_collection_id == 'RIDER Breast MRI': # Temporary code for development
        begin = time.time()
        if not collection.expanded:
            expand_collection(sess, collection)
        logging.info("Collection %s; %s patients", collection.tcia_api_collection_id, len(collection.patients))
        # Get the lists of data and analyis series in this patient
        data_collection_doi = get_data_collection_doi(collection.tcia_api_collection_id)
        analysis_collection_dois = get_analysis_collection_dois(collection.tcia_api_collection_id)
        for patient in collection.patients:
            build_patient(sess, args, version, collection, patient, data_collection_doi, analysis_collection_dois)
        collection.collection_timestamp = min([patient.patient_timestamp for patient in collection.patients])
        copy_prestaging_to_staging_bucket(args, collection)
        collection.done = True
        # ************ Temporary code during development********************
        duration = str(timedelta(seconds=(time.time() - begin))) # ***********
        logging.info("Collection %s completed in %s", collection.tcia_api_collection_id, duration) # *********
        raise #**********
        # ************ End temporary code ********************
        sess.commit()
        duration = str(timedelta(seconds=(time.time() - begin)))
        logging.info("Collection %s completed in %s", collection.tcia_api_collection_id, duration)
    else:
        logging.info("Collection %s previously built", collection.tcia_api_collection_id)


def expand_version(sess, args, version):
    # This code is special because version 2 is special in that it is partially
    # populated with the collections from version 1. Moreover, we know that each collection
    # that we add is a new collection...it was not in version 1.
    # For subsequent versions, we need to determine whether or not a collection is new.

    # If we are here, we are beginning work on this version.
    # GCS data for the collection being built is accumulated in the staging bucket,
    # args.staging bucket.
    # Since we are starting, delete everything from the staging bucket.

    empty_bucket(args.staging_bucket)

    if version.idc_version_number == 2:
        tcia_collection_ids = [collection['Collection'] for collection in get_TCIA_collections()]
        idc_collection_ids = [collection.tcia_api_collection_id for collection in version.collections]
        new_collections = []
        for tcia_collection_id in tcia_collection_ids:
            if not tcia_collection_id in idc_collection_ids:
                new_collections.append(Collection(version_id = version.id,
                                              idc_version_number = version.idc_version_number,
                                              collection_timestamp = datetime(1970,1,1,0,0,0),
                                              tcia_api_collection_id = tcia_collection_id,
                                              revised = True,
                                              done = False,
                                              is_new = True,
                                              expanded = False))
        sess.add_all(new_collections)
        version.expanded = True
        sess.commit()
        logging.info("Expanded version %s", version.idc_version_number)
    else:
        # Need to add code to handle the normal case
        logging.error("extend_version code needs extension")


def build_version(sess, args, version):
    # Session = sessionmaker(bind= sql_engine)
    # version = version_is_done(sess, args.vnext)
    if not version.done:
        begin = time.time()
        if not version.expanded:
            expand_version(sess, args, version)
        logging.info("Version %s; %s collections", version.idc_version_number, len(version.collections))
        for collection in version.collections:
            build_collection(sess, args, version, collection)
        version.idc_version_timestamp = min([collection.collection_timestamp for collection in version.collections])
        copy_staging_bucket_to_final_bucket(args,version)
        version.done = True
        sess.commit()
        duration = str(timedelta(seconds=(time.time() - begin)))
        logging.info("Built version %s in %s", version.idc_version_number, version)
    else:
        logging.info("    version %s previously built", version.idc_version_number)


def prebuild(args):
    # Create a local working directory
    if os.path.isdir('{}'.format(args.dicom)):
        shutil.rmtree('{}'.format(args.dicom))
    os.mkdir('{}'.format(args.dicom))

    # Basically add a new Version with idc_version_number args.vnext, if it does not already exist
    with Session(sql_engine) as sess:
        stmt = select(Version).distinct()
        result = sess.execute(stmt)
        version = []
        for row in result:
            if row[0].idc_version_number == args.vnext:
                # We've at least started working on vnext
                version = row[0]
                break

        if not version:
        # If we get here, we have not started work on vnext, so add it to Version
            version = Version(idc_version_number=args.vnext,
                              idc_version_timestamp=datetime.datetime.utcnow(),
                              revised=False,
                              done=False,
                              is_new=True,
                              expanded=False)
            sess.add(version)
            sess.commit()
        build_version(sess, args, version)


if __name__ == '__main__':
    logging.basicConfig(filename='{}/logs/build_rider_breast_mri_nbia.log'.format(os.environ['PWD']), filemode='w', level=logging.INFO)

    parser =argparse.ArgumentParser()

    parser.add_argument('--vnext', default=2, help='Next version to generate')
    parser.add_argument('--bucket', default='idc_dev', help='Bucket in which to save instances')
    parser.add_argument('--staging_bucket', default='idc_dev_staging', help='Copy instances here before forwarding to --bucket')
    parser.add_argument('--prestaging_bucket', default='idc_dev_prestaging', help='Copy instances here before forwarding to --staging_bucket')
    parser.add_argument('--gch_dataset', default='idc_dev', help='GCH dataset')
    parser.add_argument('--gch_dicom_store', default='idc_dev', help='GCH DICOM store')
    parser.add_argument('--bq_dataset', default='mvp_wave2', help='BQ dataset')
    parser.add_argument('--bq_aux_name', default='auxilliary_metadata', help='Auxilliary metadata table name')
    parser.add_argument('--project', default='idc-dev-etl')

    args = parser.parse_args()

    # Directory to which to download files from TCIA/NBIA
    args.dicom = 'dicom'
    print("{}".format(args), file=sys.stdout)

    prebuild(args)
