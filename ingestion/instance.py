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

import os
from pathlib import Path
import time
from datetime import datetime, timezone
from utilities.logging_config import successlogger, progresslogger, errlogger
import pydicom
import shutil
from pydicom.errors import InvalidDicomError
from idc.models import Version, Instance, IDC_Instance
from sqlalchemy import select,delete
from google.cloud import storage
from utilities.tcia_helpers import  get_TCIA_instances_per_series_with_hashes
from ingestion.utilities.utils import validate_hashes, md5_hasher, copy_disk_to_gcs, copy_gcs_to_gcs

# successlogger = logging.getLogger('root.success')
# progresslogger = logging.getLogger('root.progress')
# errlogger = logging.getLogger('root.err')

def clone_instance(instance, uuid):
    new_instance = Instance(uuid=uuid)
    for key, value in instance.__dict__.items():
        if key not in ['_sa_instance_state', 'uuid']:
            setattr(new_instance, key, value)
    return new_instance


def build_instances_tcia(sess, args, collection, patient, study, series):
     try:

        # Delete the series from disk in case it is there from a previous run
        try:
            # shutil.rmtree("{}/{}".format(args.dicom_dir, series.series_instance_uid), ignore_errors=True)
            shutil.rmtree("{}/{}".format(args.dicom_dir, series.uuid), ignore_errors=True)
        except:
            # It wasn't there
            pass

        # Download a zip of the instances in a series
        # It will write the zip to a file dicom/<series_instance_uid>.zip in the
        # working directory, and expand the zip to directory dicom/<series_instance_uid>
        hashes = get_TCIA_instances_per_series_with_hashes(args.dicom_dir, series)
        # Validate that the files on disk have the expected hashes.
        if not validate_hashes(args, collection, patient, study, series, hashes):
            # If validation fails, return. None of the instances will have the done bit set to True
            return

        # Get a list of the files from the download
        dcms = [dcm for dcm in os.listdir("{}/{}".format(args.dicom_dir, series.uuid))]

        # Ensure that the zip has the expected number of instances
        if not len(dcms) == len(series.instances):
            errlogger.error("      p%s: Invalid zip file for %s/%s/%s/%s", args.pid,
                collection.collection_id, patient.submitter_case_id, study.study_instance_uid, series.uuid)
            # Return without marking all instances done. This will prevent the series from being done.
            return

        # TCIA file names are based on the position of the image in a scan. We need to extract the SOPInstanceUID
        # so that we can know the instance.
        # Use pydicom to open each file to get its UID and rename the file with its associated uuid that we
        # generated when we expanded this series.

        # Replace the TCIA assigned file name
        # Also compute the md5 hash and length in bytes of each
        instances = {instance.sop_instance_uid:instance for instance in series.instances}

        for dcm in dcms:
            try:
                reader = pydicom.dcmread("{}/{}/{}".format(args.dicom_dir, series.uuid, dcm), stop_before_pixels=True)
                SOPInstanceUID = reader.SOPInstanceUID
            except InvalidDicomError:
                errlogger.error("       p%s: Invalid DICOM file for %s/%s/%s/%s", args.pid,
                    collection.collection_id, patient.submitter_case_id, study.study_instance_uid, series.uuid)
                if collection.collection_id == 'NLST':
                    breakpoint()
                    # For NLST only, just delete the invalid file
                    os.remove("{}/{}/{}".format(args.dicom_dir, series.uuid, dcm))
                    continue
                else:
                    # Return without marking all instances done. This will be prevent the series from being done.
                    return

            instance = instances[SOPInstanceUID]
            # # If an instance is already done, don't need to do anything more
            # if instance.done:
            #     # Delete file. We already have it.
            #     os.remove("{}/{}/{}".format(args.dicom_dir, series.uuid, dcm))
            #     progresslogger.debug("      p%s: Instance %s previously done, ", args.pid, series.uuid)
            #
            #     continue

            # Validate that DICOM IDs match what we are expecting
            try:
                assert patient.submitter_case_id == reader.PatientID;
                assert study.study_instance_uid == reader.StudyInstanceUID;
                assert series.series_instance_uid == reader.SeriesInstanceUID;
            except:
                errlogger.error(f"       p{args.pid}: DICOM ID mismatch for instance: {instance.sop_instance_uid} ")
                errlogger.error(f'       p{args.pid}: PatientID: TCIA : {patient.submitter_case_id}, \
                    DICOM: {reader.PatientID}')
                errlogger.error(f'       p{args.pid}: StudyInstanceUID: TCIA : {patient.study_instance_uid}, \
                    DICOM: {reader.StudyInstanceUID}')
                errlogger.error(f'       p{args.pid}: SeriesInstanceUID: TCIA : {patient.series_instance_uid}, \
                    DICOM: {reader.SeriesInstanceUID}')
                # Return without marking all instances done. This will be prevent the series from being done.
                return

            uuid = instance.uuid
            file_name = "{}/{}/{}".format(args.dicom_dir, series.uuid, dcm)
            blob_name = "{}/{}/{}.dcm".format(args.dicom_dir, series.uuid, uuid)
            if os.path.exists(blob_name):
                errlogger.error("       p%s: Duplicate DICOM files for %s/%s/%s/%s/%s", args.pid,
                    collection.collection_id, patient.submitter_case_id, study.study_instance_uid, series.series_instance_uid, SOPInstanceUID)
                if collection.collection_id == 'NLST':
                    breakpoint()
                    # For NLST only, just delete the duplicate
                    os.remove("{}/{}/{}".format(args.dicom_dir, series.uuid, dcm))
                    continue
                else:
                    # Return without marking all instances done. This will prevent the series from being done.
                    return

            os.rename(file_name, blob_name)

            instance.hash = md5_hasher(blob_name)
            if len(instance.hash) != 32:
                breakpoint()
                errlogger.error("       p%s: Hash failed for %s/%s/%s/%s/%s", args.pid,
                    collection.collection_id, patient.submitter_case_id, study.study_instance_uid, series.series_instance_uid, SOPInstanceUID)
                # Return without marking all instances done. This will be prevent the series from being done.
                return
            instance.size = Path(blob_name).stat().st_size
            instance.timestamp = datetime.utcnow()

        if collection.collection_id == 'NLST':
            breakpoint()
            # For NLST only, delete any instances for which there is not a corresponding file
            for instance in series.instances:
                if not os.path.exists("{}/{}/{}.dcm".format(args.dicom_dir, series.uuid, instance.uuid)):
                    sess.execute(delete(Instance).where(Instance.uuid==instance.uuid))
                    series.instances.remove(instance)

        # Copy the instance data to a staging bucket
        try:
            copy_disk_to_gcs(args, collection, patient, study, series)
        except:
            # Copy failed. Return without marking all instances done. This will be prevent the series from being done.
            errlogger.error("       p%s: Copy files to GCS failed for %s/%s/%s/%s", args.pid,
                    collection.collection_id, patient.submitter_case_id, study.study_instance_uid, series.series_instance_uid)
            return

        for instance in series.instances:
            instance.done = True
     except Exception as exc:
        errlogger.info('  p%s build_instances failed: %s', args.pid, exc)
        raise exc


def build_instances_idc(sess, args, collection, patient, study, series):

    client=storage.Client()

    # When idc is the source of instance data, the instances are already in a bucket.
    # From idc_xxx DB hierarchy, we get a table of the SOPInstanceUID, hash and GCS URL of
    # all the instances in the series
    stmt = select(IDC_Instance.sop_instance_uid, IDC_Instance.ingestion_url, IDC_Instance.hash ). \
        where(IDC_Instance.series_instance_uid == series.series_instance_uid)
    result = sess.execute(stmt)
    src_instance_metadata = {i.sop_instance_uid:{'ingestion_url':i.ingestion_url, 'hash':i.hash} \
                             for i in result.fetchall()}
    # Now we copy each instance to the staging bucket
    start = time.time()
    total_size = 0
    for instance in series.instances:
        if not instance.done:
            # Copy the instance and validate the hash
            instance.hash = src_instance_metadata[instance.sop_instance_uid]['hash']
            instance.size, hash = copy_gcs_to_gcs(args, client, args.prestaging_idc_bucket, series, instance, src_instance_metadata[instance.sop_instance_uid]['ingestion_url'])
            if hash != instance.hash:
                errlogger.error("       p%s: Copy files to GCS failed for %s/%s/%s/%s/%s", args.pid,
                                collection.collection_id, patient.submitter_case_id, study.study_instance_uid,
                                series.series_instance_uid, instance.sop_instance_uid)
                # Copy failed. Return without marking all instances done. This will be prevent the series from being done.
                return
            instance.ingestion_url = src_instance_metadata[instance.sop_instance_uid]['ingestion_url']
            assert instance.ingestion_url is not None and instance.ingestion_url != ""
            total_size += instance.size
            instance.done = True
    progresslogger.debug("        p%s: Series %s: instances: %s, gigabytes: %.2f, rate: %.2fMB/s",
                     args.pid, series.series_instance_uid,
                     len(series.instances),
                     total_size/(2**30),
                     (total_size/(time.time() - start))/(2**20)
                     )