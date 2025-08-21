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

# Rename instances in a previously excluded collection that was downloaded from TCIA to disk.
# Read each instance using pydicom to obtain its SOPInstanceUID, map that to the corresponding
# i_uuid and change the file name to <i_uuid>.dcm.
# Finally, copy all the instances from disk to a bucket, idc_v22_tcia_<collection_id>
# in idc-dev-etl.

import os
from pathlib import Path
import argparse
from utilities.logging_config import successlogger, progresslogger, errlogger
import pydicom
from ingestion.utilities.utils import create_prestaging_bucket, md5_hasher
from google.cloud import bigquery, storage
from google.api_core.exceptions import Conflict
from python_settings import settings
import settings as etl_settings
if not settings.configured:
    settings.configure(etl_settings)
    assert settings.configured
from subprocess import run

def get_instance_data(collection_id):
    client = bigquery.Client()
    query = f"""
SELECT DISTINCT collection_id, submitter_case_id, study_instance_uid, series_instance_uid, sop_instance_uid,
    se_uuid, i_uuid, i_hash, i_size
FROM `idc-dev-etl.idc_v22_dev.all_joined_excluded`
WHERE collection_id = '{collection_id}'
"""

    instance_data = client.query(query).result().to_dataframe()
    return instance_data


def create_prestaging_bucket(bucket_name):
    client = storage.Client()

    # Try to create the destination bucket
    new_bucket = client.bucket(bucket_name)
    new_bucket.iam_configuration.uniform_bucket_level_access_enabled = True
    new_bucket.versioning_enabled = False
    try:
        result = client.create_bucket(new_bucket, location='US-CENTRAL1', project=settings.DEV_PROJECT)
        # return(0)
    except Conflict:
        # Bucket exists
        pass
    except Exception as e:
        # Bucket creation failed somehow
        errlogger.error("Error creating bucket %s: %s",bucket_name, e)
        return(-1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--collection_id', default='CBIS-DDSM', help='Collection ID to rename')
    parser.add_argument('--download_path', default='/mnt/disks/idc-etl/aspera', help='Directory containing downloaded instances')
    args = parser.parse_args()

    bucket_name = f'idc_v22_tcia_{args.collection_id.lower().replace("-", "_").replace(" ", "_")}'
    create_prestaging_bucket(bucket_name)


    download_path = ""
    for root, dirs, files in os.walk(args.download_path):
        if dirs and dirs[0] == args.collection_id:
            download_path = root
            break
    if not download_path:
        errlogger.error("Couldn't find download")
        exit(1)

    all_instance_data = get_instance_data(args.collection_id)

    dcms = []
    for root, dirs, files in os.walk(download_path):
        dcm_files = [os.path.join(root,file) for file in files if file.endswith('.dcm')]
        dcms.extend(dcm_files)

    # Ensure that the zip has the expected number of instances
    if not len(dcms) == len(all_instance_data):
        errlogger.error("Invalid zip file")
        exit(1)

    for dcm in dcms:
        try:
            reader = pydicom.dcmread(dcm, stop_before_pixels=True)
            submitter_case_id = reader.PatientID;
            study_instance_uid = reader.StudyInstanceUID;
            series_instance_uid = reader.SeriesInstanceUID;
            sop_instance_uid = reader.SOPInstanceUID
        except:
            errlogger.error(f"Invalid DICOM file {dcm}")
            exit(1)

        instance_data = all_instance_data[all_instance_data["sop_instance_uid"] == sop_instance_uid].iloc[0]
        # assert submitter_case_id == instance_data["submitter_case_id"]
        assert study_instance_uid == instance_data["study_instance_uid"]
        assert series_instance_uid == instance_data["series_instance_uid"]
        assert md5_hasher(dcm) == instance_data['i_hash']
        assert Path(dcm).stat().st_size == instance_data['i_size']

        se_uuid = instance_data["se_uuid"]
        i_uuid = instance_data["i_uuid"]
        # file_name = "{}/{}/{}".format(args.dicom_dir, series.uuid, dcm)
        path = dcm.rsplit('/',1)[0]
        # Create a director for the series
        os.makedirs(os.path.join(path, se_uuid), exist_ok=True)

        if dcm.rsplit('/', 2)[-2:] == [se_uuid, f"{i_uuid}.dcm"]:
            progresslogger.info(f'{dcm} already renamed')
        else:
            blob_name = os.path.join(path, se_uuid, f"{i_uuid}.dcm")
            os.rename(dcm, blob_name)

    try:
        # Copy the series to GCS
        src = f'{download_path}/*/*/*/*/*'
        dst = f'gs://{bucket_name}/'
        # breakpoint() # Check if -J parameter is still broken
        cmmd = f"gsutil -m -q  cp -r {src} {dst}"
        # result = run(["gsutil", "-m", "-q", "cp", "-r", src, dst], check=True)
        result = run(cmmd, shell=True, check=True)
        if result.returncode:
            errlogger.error('copy_disk_to_prestaging_bucket failed')
            exit(1)
        # rootlogger.debug("p%s: Uploaded instances to GCS", args.pid)
    except Exception as exc:
        errlogger.error("Copy to prestage bucket failed")
        exit(1)




