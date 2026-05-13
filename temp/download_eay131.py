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

from google.cloud import storage
from utilities.tcia_helpers import  get_TCIA_instances_per_series_with_hashes, get_TCIA_patients_per_collection, get_TCIA_studies_per_patient, get_TCIA_series_per_study
import requests
import os
import zipfile
from pathlib import Path
from utilities.logging_config import progresslogger, errlogger
from multiprocessing import Process, Queue

NBIA_V1_URL = 'https://services.cancerimagingarchive.net/nbia-api/services/v1'
NBIA_V4_URL = 'https://services.cancerimagingarchive.net/nbia-api/services/v4'

path = '/mnt/disks/idc-etl/eay131'
num_processes = 8

def get_TCIA_instances_per_series_with_hashes(dicom, series):
    filename = "{}/{}.zip".format(dicom, series)
    dirname = "{}/{}".format(dicom, series)

    url = f'{NBIA_V4_URL}/getImageWithMD5Hash?SeriesInstanceUID={series}'
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk:
                f.write(chunk)

    # Now try to extract the instances to a directory DICOM/<series_instance_uid>
    os.makedirs(f"{dirname}", exist_ok=True)
    with zipfile.ZipFile(filename, "r") as zip_ref:
        zip_ref.extractall(f'{dirname}')

    hashes = open(f'{dirname}/md5hashes.csv').read().splitlines()[1:]
    os.remove(f'{dirname}/md5hashes.csv')

    return hashes

def worker(input, id):
    client = storage.Client()
    for n, series in iter(input.get, 'STOP'):
        try:
            get_TCIA_instances_per_series_with_hashes('/mnt/disks/idc-etl/eay131', series['SeriesInstanceUID'])
            print(f"p{id}: {n}:Series {series['SeriesInstanceUID']} downloaded")
        except Exception as exc:
            errlogger.error(f"Series {series['SeriesInstanceUID']}: {exc}")
    return

processes = []
task_queue = Queue()
# Start worker processes
for process in range(num_processes):
    id = process + 1
    processes.append(
        Process(group=None, target=worker, args=(task_queue, id)))
    processes[-1].start()

n=0
patients = get_TCIA_patients_per_collection('EAY131')
for patient in patients:
    patient_tries = 10
    while patient_tries:
        try:
            studies = get_TCIA_studies_per_patient('EAY131', patient['PatientId'])
            break
        except Exception as exc:
            patient_tries -= 1
            errlogger.error(f"Studies in patient {patient['PatientId']} tries left  {patient_tries}")
            errlogger.error(exc)
        if patient_tries == 0:
            errlogger.error(f"Studies in patient {patient['PatientId']} tries left  {patient_tries}")
            errlogger.error(exc)
            break
    for study in studies:
        series_tries = 10
        while series_tries:
            try:
               seriess = get_TCIA_series_per_study('EAY131', patient['PatientId'], study['StudyInstanceUID'])
               break
            except Exception as exc:
                series_tries -= 1
                errlogger.error(f"Series in study {patient['PatientId']}/study['StudyInstanceUID'] tries left  {series_tries}")
                errlogger.error(exc)
            if series_tries == 0:
                errlogger.error(f"Series in study {patient['PatientId']}/study['StudyInstanceUID'] tries left  {series_tries}")
                errlogger.error(exc)
                break
        for series in seriess:
            directory_path = Path(f"/mnt/disks/idc-etl/eay131/{series['SeriesInstanceUID']}")
            if directory_path.is_dir():
                progresslogger.info(f"p0: {n}:Series {series['SeriesInstanceUID']} previously downloaded")
            else:
                task_queue.put((n, series))
            n += 1
            # get_TCIA_instances_per_series_with_hashes('/mnt/disks/idc-etl/eay131', series['SeriesInstanceUID'])
            # print(f"Series {series['SeriesInstanceUID']} downloaded")

# Tell child processes to stop
for i in range(num_processes):
    task_queue.put('STOP')

# Wait for process to terminate
for process in processes:
    progresslogger.info(f'Joining process: {process.name}, {process.is_alive()}')
    process.join()
