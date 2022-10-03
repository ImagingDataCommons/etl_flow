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
import logging
import pydicom
import shutil
from pydicom.errors import InvalidDicomError
from idc.models import Version, Instance, WSI_Instance
from sqlalchemy import select,delete
from google.cloud import storage
from utilities.tcia_helpers import  get_TCIA_instances_per_series_with_hashes
from ingestion.utilities.utils import validate_hashes, md5_hasher, copy_disk_to_gcs, copy_gcs_to_gcs



successlogger = logging.getLogger('root.success')
progresslogger = logging.getLogger('root.progress')
errlogger = logging.getLogger('root.err')

def clone_instance(instance, uuid):
    new_instance = Instance(uuid=uuid)
    for key, value in instance.__dict__.items():
        if key not in ['_sa_instance_state', 'uuid']:
            setattr(new_instance, key, value)
    return new_instance


def build_instances_tcia(sess, args, collection, patient, study, series):
    # Download a zip of the instances in a series
    # It will be write the zip to a file dicom/<series_instance_uid>.zip in the
    # working directory, and expand the zip to directory dicom/<series_instance_uid>
    try:
        # When TCIA provided series timestamps, we'll us that for timestamp.
        now = datetime.now(timezone.utc)

        # Delete the series from disk in case it is there from a previous run
        try:
            shutil.rmtree("{}/{}".format(args.dicom_dir, series.series_instance_uid), ignore_errors=True)
        except:
            # It wasn't there
            pass

        download_start = time.time_ns()
        hashes = get_TCIA_instances_per_series_with_hashes(args.dicom_dir, series.series_instance_uid)
        download_time = (time.time_ns() - download_start)/10**9
        if not validate_hashes(args, collection, patient, study, series, hashes):
            return

        # Get a list of the files from the download
        dcms = [dcm for dcm in os.listdir("{}/{}".format(args.dicom_dir, series.series_instance_uid))]

        # Ensure that the zip has the expected number of instances
        if not len(dcms) == len(series.instances):
            errlogger.error("      p%s: Invalid zip file for %s/%s/%s/%s", args.pid,
                collection.collection_id, patient.submitter_case_id, study.study_instance_uid, series.series_instance_uid)
            # Return without marking all instances done. This will prevent the series from being done.
            return

        # TCIA file names are based on the position of the image in a scan. We need to extract the SOPInstanceUID
        # so that we can know the instance.
        # Use pydicom to open each file to get its UID and rename the file with its associated uuid that we
        # generated when we expanded this series.

        # Replace the TCIA assigned file name
        # Also compute the md5 hash and length in bytes of each
        pydicom_times=[]
        psql_times=[]
        rename_times=[]
        metadata_times=[]
        begin = time.time_ns()
        instances = {instance.sop_instance_uid:instance for instance in series.instances}

        for dcm in dcms:
            try:
                pydicom_times.append(time.time_ns())
                reader = pydicom.dcmread("{}/{}/{}".format(args.dicom_dir, series.series_instance_uid, dcm), stop_before_pixels=True)
                SOPInstanceUID = reader.SOPInstanceUID
                pydicom_times.append(time.time_ns())
            except InvalidDicomError:
                errlogger.error("       p%s: Invalid DICOM file for %s/%s/%s/%s", args.pid,
                    collection.collection_id, patient.submitter_case_id, study.study_instance_uid, series.series_instance_uid)
                if collection.collection_id == 'NLST':
                    breakpoint()
                    # For NLST only, just delete the invalid file
                    os.remove("{}/{}/{}".format(args.dicom_dir, series.series_instance_uid, dcm))
                    continue
                else:
                    # Return without marking all instances done. This will be prevent the series from being done.
                    return

            psql_times.append(time.time_ns())
            instance = instances[SOPInstanceUID]
            # If an instance is already done, don't need to do anything more
            if instance.done:
                # Delete file. We already have it.
                os.remove("{}/{}/{}".format(args.dicom_dir, series.series_instance_uid, dcm))
                progresslogger.debug("      p%s: Instance %s previously done, ", args.pid, series.series_instance_uid)

                continue
            psql_times.append(time.time_ns())

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

            rename_times.append(time.time_ns())
            uuid = instance.uuid
            file_name = "{}/{}/{}".format(args.dicom_dir, series.series_instance_uid, dcm)
            blob_name = "{}/{}/{}.dcm".format(args.dicom_dir, series.series_instance_uid, uuid)
            if os.path.exists(blob_name):
                errlogger.error("       p%s: Duplicate DICOM files for %s/%s/%s/%s/%s", args.pid,
                    collection.collection_id, patient.submitter_case_id, study.study_instance_uid, series.series_instance_uid, SOPInstanceUID)
                if collection.collection_id == 'NLST':
                    breakpoint()
                    # For NLST only, just delete the duplicate
                    os.remove("{}/{}/{}".format(args.dicom_dir, series.series_instance_uid, dcm))
                    continue
                else:
                    # Return without marking all instances done. This will be prevent the series from being done.
                    return

            os.rename(file_name, blob_name)
            rename_times.append(time.time_ns())

            metadata_times.append(time.time_ns())
            instance.hash = md5_hasher(blob_name)
            instance.size = Path(blob_name).stat().st_size
            instance.timestamp = datetime.utcnow()
            metadata_times.append(time.time_ns())

        if collection.collection_id == 'NLST':
            breakpoint()
            # For NLST only, delete any instances for which there is not a corresponding file
            for instance in series.instances:
                if not os.path.exists("{}/{}/{}.dcm".format(args.dicom_dir, series.series_instance_uid, instance.uuid)):
                    sess.execute(delete(Instance).where(Instance.uuid==instance.uuid))
                    series.instances.remove(instance)

        instances_time = time.time_ns() - begin

        copy_start = time.time_ns()
        try:
            copy_disk_to_gcs(args, collection, patient, study, series)
        except:
            # Copy failed. Return without marking all instances done. This will be prevent the series from being done.
            errlogger.error("       p%s: Copy files to GCS failed for %s/%s/%s/%s", args.pid,
                    collection.collection_id, patient.submitter_case_id, study.study_instance_uid, series.series_instance_uid)
            return
        copy_time = (time.time_ns() - copy_start)/10**9

        mark_done_start = time.time ()
        for instance in series.instances:
            instance.done = True
        mark_done_time = time.time() - mark_done_start
        # rootlogger.debug("      p%s: Series %s, completed build_instances; %s", args.pid, series.series_instance_uid, time.asctime())
        progresslogger.debug("        p%s: Series %s: download: %s, instances: %s, pydicom: %s, psql: %s, rename: %s, metadata: %s, copy: %s, mark_done: %s",
                         args.pid, series.series_instance_uid,
                         download_time,
                         instances_time/10**9,
                         (sum(pydicom_times[1::2]) - sum(pydicom_times[0::2]))/10**9,
                         (sum(psql_times[1::2]) - sum(psql_times[0::2]))/10**9,
                         (sum(rename_times[1::2]) - sum(rename_times[0::2]))/10 **9,
                         (sum(metadata_times[1::2]) - sum(metadata_times[0::2])) / 10 ** 9,
                         copy_time,
                         mark_done_time)
    except Exception as exc:
        errlogger.info('  p%s build_instances failed: %s', args.pid, exc)
        raise exc


def build_instances_path(sess, args, collection, patient, study, series):
    # Download a zip of the instances in a series
    # It will be write the zip to a file dicom/<series_instance_uid>.zip in the
    # working directory, and expand the zip to directory dicom/<series_instance_uid>

    # When TCIA provided series timestamps, we'll us that for timestamp.
    now = datetime.now(timezone.utc)
    client=storage.Client()

    stmt = select(WSI_Instance.sop_instance_uid, WSI_Instance.url, WSI_Instance.hash ). \
        where(WSI_Instance.series_instance_uid == series.series_instance_uid)
    result = sess.execute(stmt)
    src_instance_metadata = {i.sop_instance_uid:{'gcs_url':i.url, 'hash':i.hash} \
                             for i in result.fetchall()}
    start = time.time()
    total_size = 0
    for instance in series.instances:
        if not instance.done:
            instance.hash = src_instance_metadata[instance.sop_instance_uid]['hash']
            instance.size, hash = copy_gcs_to_gcs(args, client, args.prestaging_path_bucket, instance, src_instance_metadata[instance.sop_instance_uid]['gcs_url'])
            if hash != instance.hash:
                errlogger.error("       p%s: Copy files to GCS failed for %s/%s/%s/%s/%s", args.pid,
                                collection.collection_id, patient.submitter_case_id, study.study_instance_uid,
                                series.series_instance_uid, instance.sop_instance_uid)
                # Copy failed. Return without marking all instances done. This will be prevent the series from being done.
                return
            total_size += instance.size
            instance.done = True
    progresslogger.debug("        p%s: Series %s: instances: %s, gigabytes: %.2f, rate: %.2fMB/s",
                     args.pid, series.series_instance_uid,
                     len(series.instances),
                     total_size/(2**30),
                     (total_size/(time.time() - start))/(2**20)
                     )