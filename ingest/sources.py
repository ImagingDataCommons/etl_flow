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

from utilities.tcia_helpers import  get_access_token, get_hash, get_TCIA_studies_per_patient, get_TCIA_patients_per_collection,\
    get_TCIA_series_per_study, get_TCIA_instance_uids_per_series, get_TCIA_instances_per_series, get_collection_values_and_counts
from idc.models import Collection, Patient, Study, Series, Instance, Retired, WSI_metadata, instance_source
from sqlalchemy import select
import hashlib


# Hash a sorted list of hashes
def get_merkle_hash(hashes):
    md5 = hashlib.md5()
    hashes.sort()
    for hash in hashes:
        md5.update(hash.encode())
    return md5.hexdigest()


class Source:
    def __init__(self, source_id):
        self.source_id = source_id; # ID for indexing a "sources" column in PSQL
        self.nbia_server = 'NBIA' # Default. Set to 'NLST' only when getting NLST radiology

    # Compute object's hash as hash of sort childrens' hashes
    def idc_version_hash(self):
        objects = self.sess.execute(select(Collection))
        hashes = []
        for object in objects:
            hashes.append(object.hashes[self.source_id])
        hash = get_merkle_hash(hashes)
        return hash

    # Compute object's hash as hash of sort childrens' hashes
    def idc_collection_hash(self, collection):
        objects = collection.patients
        hashes = []
        for object in objects:
            hash = object.hashes[self.source_id] if object.hashes[self.source_id] != None else ""
            hashes.append(hash)
        hash = get_merkle_hash(hashes)
        return hash

    # Compute object's hash as hash of sort childrens' hashes
    def idc_patient_hash(self, patient):
        objects = patient.studies
        hashes = []
        for object in objects:
            hash = object.hashes[self.source_id] if object.hashes[self.source_id] != None else ""
            hashes.append(hash)
        hash = get_merkle_hash(hashes)
        return hash

    # Compute object's hash as hash of sort childrens' hashes
    def idc_study_hash(self, study):
        objects = study.series
        hashes = []
        for object in objects:
            hash = object.hashes[self.source_id] if object.hashes[self.source_id] != None else ""
            hashes.append(hash)
        hash = get_merkle_hash(hashes)
        return hash

    # Compute object's hash as hash of sort childrens' hashes
    def idc_series_hash(self, series):
        objects = series.instances
        hashes = []
        for object in objects:
            hash = object.hash if object.hash != None else ""
            hashes.append(hash)
        hash = get_merkle_hash(hashes)
        return hash




class TCIA(Source):
    def __init__(self):
        super().__init__(instance_source['tcia'].value)
        self.hash_access_token = get_access_token(auth_server = "https://public-dev.cancerimagingarchive.net/nbia-api/oauth/token")


    def collections(self):
        collections = get_collection_values_and_counts(self.nbia_server)
        return collections


    def patients(self, collection):
        patients = get_TCIA_patients_per_collection(collection.collection_id, self.nbia_server)
        return patients


    def studies(self, patient):
        studies = get_TCIA_studies_per_patient(patient.collection.collection_id, patient.submitter_case_id, self.nbia_server)
        return studies


    def series(self, study):
        series = get_TCIA_series_per_study(study.patient.collection.collection_id, study.patient.submitter_case_id, study.study_instance_uid,
                                         self.nbia_server)
        return series


    def instance(self, series):
        instances = get_TCIA_instance_uids_per_series(series.series_instance_uid, self.nbia_server)
        return instances

    # def instance_data(self, series):
    #     download_start = time.time_ns()
    #     get_TCIA_instances_per_series(args.dicom, series.series_instance_uid, args.server)
    #     download_time = (time.time_ns() - download_start) / 10 ** 9
    #     # Get a list of the files from the download
    #     dcms = [dcm for dcm in os.listdir("{}/{}".format(args.dicom, series.series_instance_uid))]
    #     return dcms

    def collection_hash(self, collection):
        result = get_hash({'Collection': collection.collection_id}, access_token=self.hash_access_token)


class Pathology(Source):
    def __init__(self, sess):
        super().__init__(instance_source['path'].value)
        self.sess = sess


    def collections(self):
        stmt = select(WSI_metadata.collection_id).distinct()
        result = self.sess.execute(stmt)
        collections = [row['collection_id'].lower() for row in result.fetchall()]
        return collections


    def patients(self, collection):
        stmt = select(WSI_metadata.submitter_case_id).distinct().where(WSI_metadata.collection_id==collection.collection_id)
        result = self.sess.execute(stmt)
        patients = [row['submitter_case_id'] for row in result.fetchall()]
        return patients


    def studies(self, patient):
        stmt = select(WSI_metadata.study_instance_uid).distinct().where(WSI_metadata.submitter_case_id==patient.submitter_case_id)
        result = self.sess.execute(stmt)
        studies = [row['study_instance_uid'] for row in result.fetchall()]
        return studies


    def series(self, study):
        stmt = select(WSI_metadata.series_instance_uid).distinct().where(WSI_metadata.study_instance_uid==study.study_instance_uid)
        result = self.sess.execute(stmt)
        series = [row['series_instance_uid'] for row in result.fetchall()]
        return series


    def instances(self, series):
        stmt = select(WSI_metadata.sop_instance_uid).where(WSI_metadata.series_instance_uid==series.series_instance_uid)
        result = self.sess.execute(stmt)
        instances = [row['sop_instance_uid'] for row in result.fetchall()]
        return instances

    def instance_data(self, series):
        pass


    def src_version_hash(self):
        collections = self.sess.execute(select(WSI_metadata.collection_id).distinct())
        hashes = []
        for collection in collections:
            hashes.append(self.src_collection_hash(collection['collection_id']))
        hash = get_merkle_hash(hashes)
        return hash


    def src_collection_hash(self, collection_id):
        patients = self.sess.execute(select(WSI_metadata.submitter_case_id).distinct().where(WSI_metadata.collection_id == collection_id))
        hashes = []
        for patient in patients:
            hashes.append(self.src_patient_hash(patient['submitter_case_id']))
        hash = get_merkle_hash(hashes)
        return hash


    def src_patient_hash(self, submitter_case_id):
        studies = self.sess.execute(select(WSI_metadata.study_instance_uid).distinct().where(WSI_metadata.submitter_case_id == submitter_case_id))
        hashes = []
        for study in studies:
            hashes.append(self.src_study_hash(study['study_instance_uid']))
        hash = get_merkle_hash(hashes)
        return hash


    def src_study_hash(self, study_instance_uid):
        seriess = self.sess.execute(select(WSI_metadata.series_instance_uid).distinct().where(WSI_metadata.study_instance_uid == study_instance_uid))
        hashes = []
        for series in seriess:
            hashes.append(self.src_series_hash(series['series_instance_uid']))
        hash = get_merkle_hash(hashes)
        return hash


    def src_series_hash(self, series_instance_uid):
        hashes = [hash['hash'] for hash in self.sess.execute(select(WSI_metadata.hash).where(WSI_metadata.series_instance_uid == series_instance_uid))]
        hash = get_merkle_hash(hashes)
        return hash


    def version_hashes_differ(self, version):
        return version.hashes[self.source_id] != self.src_version_hash()


    def collection_hashes_differ(self, collection):
        # return self.idc_collection_hash(collection) != self.src_collection_hash(collection)
        return collection.hashes[self.source_id] != self.src_collection_hash(collection.collection_id)


    def patient_hashes_differ(self, patient):
        # return self.idc_patient_hash(patient) != self.src_patient_hash(patient)
        return patient.hashes[self.source_id] != self.src_patient_hash(patient.submitter_case_id)


    def study_hashes_differ(self, study):
        # return self.idc_study_hash(study) != self.src_study_hash(study)
        return study.hashes[self.source_id] != self.src_study_hash(study.study_instance_uid)


    def series_hashes_differ(self, series):
        # return self.idc_series_hash(series) != self.src_series_hash(series)
        return series.hashes[self.source_id] != self.src_series_hash(series.series_instance_uid)


    # def instance_hashes_differ(self, instance):
    #     return instance.path_hash != self.src_instance_hash(instance)












