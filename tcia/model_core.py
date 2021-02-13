#
# Copyright 2015-2019, Institute for Systems Biology
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

# from helpers import tcia_api.get_collections_metadata
from utilities.tcia_helpers import get_TCIA_patients_per_collection, get_TCIA_studies_per_patient, \
    get_TCIA_series_per_study, get_TCIA_instance_uids_per_series, get_TCIA_instance
from utilities.nbia_helpers import build_data_collections_metadata_table
import hashlib

class tcia():
    def __init__(self):
        pass

class collection(tcia):
    def __init__(self, collections_metadata):
        self.NBIA_CollectionID = collections_metadata["nbia_collection_id"]
        self.DOI = collections_metadata["DOI"]
        self.Status = collections_metadata["Status"]
        self.Access = collections_metadata["Access"]
        self.Updated = collections_metadata["Updated"]
        self.ImageTypes = collections_metadata["ImageTypes"]
        self.CancerType = collections_metadata["CancerType"]
        self.SupportingData = collections_metadata["SupportingData"]
        self.Location = collections_metadata["Location"]
        self.Description = collections_metadata["Description"]
        self.Species = collections_metadata["Species"]
        self.changed = False


class collections(tcia):
    def __init__(self):
        self.collections = [collection(row) for row in build_data_collections_metadata_table()()]
        self.index = len(self.collections)

    def __iter__(self):
        return self

    def __next__(self):
        if self.index == 0:
            raise StopIteration
        self.index -= 1
        return self.collections[self.index]

class patient(tcia):
    def __init__(self, patient_metadata):
        self.collection_id = patient_metadata['Collection']
        self.patientID = patient_metadata['PatientID']
        self.changed = False

class patients(tcia):
    def __init__(self, collection):
        self.patients = [patient(patient_metadata) for patient_metadata in get_TCIA_patients_per_collection(collection.TCIA_API_CollectionID)]
        self.index = len(self.patients)
    def __iter__(self):
        return self
    def __next__(self):
        if self.index == 0:
            raise StopIteration
        self.index -= 1
        return self.patients[self.index]


class study(tcia):
    def __init__(self, study_metadata):
        self.collection_id = study_metadata['Collection']
        self.patientID = study_metadata['PatientID']
        self.studyInstanceUID = study_metadata['StudyInstanceUID']
        self.changed = False

class studies(tcia):
    def __init__(self, collection, patient):
        self.studies = [study(study_metadata) for study_metadata in get_TCIA_studies_per_patient(patient.collection_id, patient.patient_id)]
        self.index = len(self.studies)
    def __iter__(self):
        return self
    def __next__(self):
        if self.index == 0:
            raise StopIteration
        self.index -= 1
        return self.studies[self.index]


class series(tcia):
    def __init__(self, series_metadata):
        self.collection_id = series_metadata['Collection']
        self.patientID = series_metadata['PatientID']
        self.studyInstanceUID = series_metadata['StudyInstanceUID']
        self.seriesInstanceUID = series_metadata['SeriesInstanceUID']
        self.changed = False

class seriess(tcia):
    def __init__(self, collection, patient, study):
        self.seriess = [study(series_metadata) for series_metadata in get_TCIA_series_per_study(study.studyInstanceUID)]
        self.index = len(self.seriess)
    def __iter__(self):
        return self
    def __next__(self):
        if self.index == 0:
            raise StopIteration
        self.index -= 1
        return self.seriess[self.index]

class instance(tcia):
    def __init__(self, instance_metadata):
        self.collection_id = instance_metadata['Collection']
        self.patientID = instance_metadata['PatientID']
        self.studyInstanceUID = instance_metadata['StudyInstanceUID']
        self.seriesInstanceUID = instance_metadata['SeriesInstanceUID']
        self.sopInstanceUID = instance_metadata['SOPInstanceUID']
        self.changed = False
        self.md5 = hashlib.md5


class instances(tcia):
    def __init__(self, collection, patient, study, series):
        self.instances = [instance(instance_metadata) for instance_metadata in get_TCIA_instance_uids_per_series(series.seriesInstanceUID)]
        self.index = len(self.instances)
    def __iter__(self):
        return self
    def __next__(self):
        if self.index == 0:
            raise StopIteration
        self.index -= 1
        return self.instances[self.index]


if __name__ == '__main__':

    c = collections()
    for collection in c:
        p = patients(collection)
        pass