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
import time
from _datetime import datetime
import logging
import pydicom
import hashlib
import subprocess
from pydicom.errors import InvalidDicomError
from uuid import uuid4
from idc_sqlalchemy.sqlalchemy_orm_models import Version, Collection, Patient, Study, Series, Instance, sql_engine
# from idc_sqlalchemy.sqlalchemy_orm_models import Version,    sql_engine
# from idc_sqlalchemy.sqlalchemy_reflected_models import Version, Collection, Patient, Study, Series, Instance, sql_engine
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from utilities.tcia_helpers import  get_TCIA_collections, get_TCIA_patients_per_collection, get_TCIA_studies_per_patient, \
    get_TCIA_series_per_study, get_TCIA_instance_uids_per_series, get_TCIA_instances_per_series
from utilities.identify_third_party_series import get_data_collection_doi, get_analysis_collection_dois

def build_instances(sess, args, version, collection, patient, study, series):
    # Download a zip of the instances in a series
    # It will be write the zip to a file dicom/<series_instance_uid>.zip in the
    # working directory, and expand the zip to directory dicom/<series_instance_uid>

    # When TCIA provided series timestamps, we'll us that for instance_timestamp.
    now = datetime(time.asctime())

    get_TCIA_instances_per_series(series.series_instance_uid)

    # Get a list of the files from the download
    dcms = [dcm for dcm in os.listdir("{}/{}".format("dicom", series.series_instance_uid))]

    # Ensure that the zip has the expected number of instances
    if not len(dcms) == len(series.instances):
        logging.error("\tInvalid zip file for %s/%s", series.series_instance_uid)
        raise RuntimeError("\tInvalid zip file for %s/%s", series.series_instance_uid)
    logging.debug(("Series %s download successful", series.series_instance_uid))

    # TCIA file names are based on the position of the image in a scan. We want names of the form
    #   <studyUID>/<seriesUID>/<instanceUID>
    # So use pydicom to open each file to get its UID and remame it
    num_instances = len(os.listdir("{}/{}".format("dicom", series.series_instance_uid)))
    logging.info("%s: %s instances", series.series_instance_uid, num_instances)
    # Replace the TCIA assigned file name
    # Also compute the md5 hash and length in bytes of each
    try:
        for dcm in dcms:
            SOPInstanceUID = pydicom.read_file("{}/{}/{}".format("dicom", series.series_instance_uid, dcm)).SOPInstanceUID
            instance = series.instances.sop_instance_uid(sop_instance_uid = SOPInstanceUID)
            file_name = "{}/{}/{}".format("dicom", series.series_instance_uid, dcm)
            blob_name = "{}/{}/{}.dcm".format(args.bucket, instance.instance_uuid, SOPInstanceUID)
            os.renames(file_name, blob_name)

            with open(blob_name,'wb') as f:
                blob = f.read()
                instance.instance_md5 = hashlib.md5(blob).hexdigest()
                instance.instance_size = len(blob)
                instance.instance_timestamp = now
    except InvalidDicomError:
        logging.error("\tInvalid DICOM file for %s", series.series_instance_uid)
        raise RuntimeError("\tInvalid DICOM file for %s", series.series_instance_uid)
    logging.debug("Renamed all files for series %s", series.series_instance_uid)

    # Delete the zip file before we copy to GCS so that it is not copied
    os.remove("{}/{}.zip".format("dicom", series.series_instance_uid))

    # Copy the instances to GCS. Do this as a subprocsse because to be able to use the gsutil -m option
    try:
        # Copy the series to GCS
        src = "{}/{}/*".format("dicom", series.series_instance_uids)
        url = "gs://{}/".format(args.bucket)
        subprocess.run(["gsutil", "-m", "-q", "cp", "-r", src, url])
        logging.debug(("Uploaded instances to GCS"))
    except Exception as exc:
        logging.error("\tUpload to GCS failed for series %s", series.series_instance_uid)
        raise RuntimeError("Upload to GCS failed for series %s", series.series_instance_uid) from exc

    # Delete the series from disk
    shutil.rmtree("{}/{}".format(dicom, series.series_instance_uid), ignore_errors=True)

    sess.commit()
    logging.info("Built instances of %s/%s/%s/%s/%s", version, collection, patient, study, series)

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
        if not series.expanded:
            expand_series(sess, series)
        build_instances(sess, args, version, collection, patient, study, series)
        series.series_instances = len(series.instances)
        series.series_timestamp = max(instance.instance_timestamp for instance in series.instances)
        series.done = True
        sess.commit()
        logging.info("Built series %s/%s/%s/%s/%s", version, collection, patient, study, series)
    pass


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
        if not study.expanded:
            expand_study(sess, collection, patient, study, data_collection_doi, analysis_collection_dois)
        for series in study.seriess:
            build_series(sess, args, version, collection, patient, study, series, data_collection_doi, analysis_collection_dois)
        study.done = True
        sess.commit()
        logging.info("Built study %s/%s/%s/%s", version, collection, patient, study)

    pass


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
        if not patient.expanded:
            expand_patient(sess, collection, patient)
        for study in patient.studies:
            build_study(sess, args, version, collection, patient, study, data_collection_doi, analysis_collection_dois)
        patient.done = True
        sess.commit()
        logging.info("Built patient %s/%s/%s", version, collection, patient)

    pass


def expand_collection(sess, collection):
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
    if not collection.done:
        if not collection.expanded:
            expand_collection(sess, collection)
        # Get the lists of data and analyis series in this patient
        data_collection_doi = get_data_collection_doi(collection.tcia_api_collection_id)
        analysis_collection_dois = get_analysis_collection_dois(collection.tcia_api_collection_id)
        for patient in collection.patients:
            build_patient(sess, args, version, collection, patient, data_collection_doi, analysis_collection_dois)
        collection.done = True
        sess.commit()
        logging.info("Built collection %s/%s", version, collection)

    pass


def expand_version(sess, args, version):
    # This code is special because version 2 is special in that it is partially
    # populated with the collections from version 1. Moreover, we know that each collection
    # that we add is a new collection...it was not in version 1.
    # For subsequent versions, we need to determine whether or not a collection is new.
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
        if not version.expanded:
            expand_version(sess, args, version)
        for collection in version.collections:
            build_collection(sess, args, version, collection)
        version.done = True
        sess.commit()
        logging.info("Built version %s", version)

def prebuild(args):
    # Basically add a new Version with idc_version_number args.vnext, if it does not already exist
    with Session(sql_engine) as sess:
        stmt = select(Version).distinct()
        result = sess.execute(stmt)
        version = []
        for row in result:
            if row[0].idc_version_number == args.vnext:
                # We've at least started working on vnext
                version = row[0]

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
    logging.basicConfig(filename='{}/logs/build_version.log'.format(os.environ['PWD']), filemode='w', level=logging.DEBUG)

    parser =argparse.ArgumentParser()

    parser.add_argument('--vnext', default=2, help='Next version to generate')
    parser.add_argument('--bucket', default='idc_dev', help='Bucket in which to save instances')
    parser.add_argument('--gch_dataset', default='idc_dev', help='GCH dataset')
    parser.add_argument('--gch_dicom_store', default='idc_dev', help='GCH DICOM store')
    parser.add_argument('--bq_dataset', default='mvp_wave2', help='BQ dataset')
    parser.add_argument('--bq_aux_name', default='auxilliary_metadata', help='Auxilliary metadata table name')
    parser.add_argument('--project', default='idc-dev-etl')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)

    prebuild(args)
