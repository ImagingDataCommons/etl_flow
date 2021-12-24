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
    get_TCIA_series_per_study, get_TCIA_instance_uids_per_series, get_TCIA_instances_per_series, get_collection_values_and_counts,\
    get_updated_series, get_instance_hash, refresh_access_token
from uuid import uuid4
from idc.models  import Collection_id_map, WSI_metadata, instance_source
from sqlalchemy import select
from google.cloud import bigquery
from ingestion.utils import get_merkle_hash
import logging
rootlogger = logging.getLogger('root')
errlogger = logging.getLogger('root.err')


class Source:
    def __init__(self, source_id):
        self.source_id = source_id; # ID for indexing a "sources" column in PSQL
        self.nbia_server = 'NBIA' # Default. Set to 'NLST' only when getting NLST radiology


class TCIA(Source):
    def __init__(self, sess, lock):
        super().__init__(instance_source['tcia'].value)
        self.source = instance_source.tcia
        self.access_token, self.refresh_token = get_access_token()
        self.sess = sess
        self.lock = lock

    def get_hash(self, request_data, access_token=None, refresh_token=None):
        # if not access_token:
        #     access_token, refresh_token = get_access_token(NBIA_AUTH_URL)
        # headers = dict(
        #     Authorization=f'Bearer {access_token}'
        # )
        # # url = "https://public-dev.cancerimagingarchive.net/nbia-api/services/getMD5Hierarchy"
        # url = f"{NBIA_V2_URL}/getM5HashForImage?SOPInstanceUid={sop_instance_uid}"
        # result = requests.get(url, headers=headers)
        self.lock.acquire()
        try:
            result = get_hash(request_data, self.access_token)
            if result.status_code == 401:
                # Refresh the token and try once more to get the hash
                self.access_token, self.refresh_token = refresh_access_token(self.refresh_token)
                result = get_hash(request_data, self.access_token)
                if result.status_code != 200:
                    result = None
            elif result.status_code != 200:
                result = None
        finally:
            self.lock.release()
            return result

    ###-------------------Versions-----------------###


    def src_version_hash(self):
        collections = self.sess.execute(select(WSI_metadata.collection_id).distinct())
        hashes = []
        for collection in collections:
            hashes.append(self.src_collection_hash(collection['collection_id']))
        hash = get_merkle_hash(hashes)
        return hash


    ###-------------------Collections-----------------###


    def collections(self):
        collections = get_collection_values_and_counts(self.nbia_server)
        return collections


    # Return True if the source's object is updated relative to the version in our DB, else
    # return False (objects are the same).
    def collection_was_revised(self,collection):
        result = self.collection_hashes_differ(collection)
        if result == -1:
            last_scan = collection.min_timestamp
            last_scan_string = f'{last_scan.day}/{last_scan.month}/{last_scan.year}'
            serieses = get_updated_series(last_scan_string)
            updated = next((item for item in serieses if item["Collection"] == collection.collection_id), None)
            return updated
        else:
            return result


    # Return True (1) if hash of object from source differs from hash of object in our DB, else
    # return 0. Return -1 if could not get hash from the source
    def collection_hashes_differ(self, collection):
        # return self.idc_collection_hash(collection) != self.src_collection_hash(collection)
        nbia_hash = self.src_collection_hash(collection.collection_id)
        if nbia_hash:
            if collection.hashes[self.source_id] != nbia_hash:
                return 1
            else:
                return 0
        else:
            return -1


    def src_collection_hash(self, collection_id):
        try:
            # result, self.access_token, refresh_token = get_hash(
            #     {'Collection': collection_id}, \
            #     access_token=self.access_token, refresh_token=self.refresh_token)
            result = self.get_hash({'Collection': collection_id})
        except Exception as exc:
            errlogger.error('Exception %s in src_collection_hash', exc)
            raise Exception('Exception %s in src_collection_hash', exc)
        if result:
            return result.content.decode()
        else:
            rootlogger.info('get_hash failed for collection %s', collection_id)
            raise Exception('get_hash failed for collection %s', collection_id)

    ###-------------------Patients-----------------###

    def patients(self, collection):
        patients = [patient['PatientId'] for patient in get_TCIA_patients_per_collection(collection.collection_id, self.nbia_server)]
        return patients

        # Return True if the source's object is updated relative to the version in our DB, else
        # return False (objects are the same).


    def patient_was_revised(self, patient):
        result = self.patient_hashes_differ(patient)
        if result == -1:
            last_scan = patient.min_timestamp
            last_scan_string = f'{last_scan.day}/{last_scan.month}/{last_scan.year}'
            serieses = get_updated_series(last_scan_string)
            updated = next((item for item in serieses if item["PatientID"] == patient.submitter_case_id), None)
            return updated
        else:
            return result

    # Return True (1) if hash of object from source differs from hash of object in our DB, else
    # return 0. Return -1 if could not get hash from the source
    def patient_hashes_differ(self, patient):
        nbia_hash = self.src_patient_hash(patient.collections[0].collection_id, patient.submitter_case_id)
        if nbia_hash != -1:
            if patient.hashes[self.source_id] != nbia_hash:
                return 1
            else:
                return 0
        else:
            return -1


    def src_patient_hash(self, collection_id, submitter_case_id):
        try:
            result = self.get_hash({'Collection':collection_id, 'PatientID': submitter_case_id})
        except Exception as exc:
            errlogger.error('Exception %s in src_patient_hash', exc)
            # raise Exception('Exception %s in src_patient_hash', exc)
            return -1
        if result:
            return result.content.decode()
        else:
            rootlogger.info('get_hash failed for patient %s', submitter_case_id)
            # raise Exception('get_hash failed for patient %s', submitter_case_id)
            return -1


    ###-------------------Studies-----------------###

    def studies(self, patient):
        studies = [study['StudyInstanceUID'] for study in get_TCIA_studies_per_patient(patient.collections[0].collection_id, patient.submitter_case_id, self.nbia_server)]
        return studies


     # Return True if the source's object is updated relative to the version in our DB, else
    # return False (objects are the same).
    def study_was_revised(self,study):
        result = self.study_hashes_differ(study)
        if result == -1:
            last_scan = study.min_timestamp
            last_scan_string = f'{last_scan.day}/{last_scan.month}/{last_scan.year}'
            serieses = get_updated_series(last_scan_string)
            updated = next((item for item in serieses if item["StudyInstanceUID"] == study.study_instance_uid), None)
            return updated
        else:
            return result


    # Return True (1) if hash of object from source differs from hash of object in our DB, else
    # return 0. Return -1 if could not get hash from the source
    def study_hashes_differ(self, study):
        nbia_hash = self.src_study_hash(study.study_instance_uid,)
        if nbia_hash:
            if study.hashes[self.source_id] != nbia_hash:
                return 1
            else:
                return 0
        else:
            return -1


    def src_study_hash(self, study_instance_uid):
        try:
            result = self.get_hash({'StudyInstanceUID': study_instance_uid})
        except Exception as exc:
            errlogger.error('Exception %s in src_study_hash', exc)
            raise Exception('Exception %s in src_study_hash', exc)
        if result:
            return result.content.decode()
        else:
            rootlogger.info('get_hash failed for study %s', study_instance_uid)
            raise Exception('get_hash failed for study %s', study_instance_uid)


    ###-------------------Series-----------------###

    def series(self, study):
        series = [series['SeriesInstanceUID'] for series in get_TCIA_series_per_study(study.patients[0].collections[0].collection_id, study.patients[0].submitter_case_id, study.study_instance_uid, \
                                         self.nbia_server)]
        return series


    # Return True if the source's object is updated relative to the version in our DB, else
    # return False (objects are the same).
    def series_was_revised(self,series):
        result = self.series_hashes_differ(series)
        if result == -1:
            last_scan = series.min_timestamp
            last_scan_string = f'{last_scan.day}/{last_scan.month}/{last_scan.year}'
            serieses = get_updated_series(last_scan_string)
            updated = next((item for item in serieses if item["SeriesInstanceUID"] == series.series_instance_uid), None)
            return updated
        else:
            return result


    # Return True (1) if hash of object from source differs from hash of object in our DB, else
    # return 0. Return -1 if could not get hash from the source
    def series_hashes_differ(self, series):
        nbia_hash = self.src_series_hash(series.series_instance_uid,)
        if nbia_hash:
            if series.hashes[self.source_id] != nbia_hash:
                return 1
            else:
                return 0
        else:
            return -1


    def src_series_hash(self, series_instance_uid):
        try:
            result = self.get_hash({'SeriesInstanceUID': series_instance_uid})
        except Exception as exc:
            errlogger.error('Exception %s in src_series_hash', exc)
            raise Exception('Exception %s in src_series_hash', exc)
        if result:
            return result.content.decode()
        else:
            rootlogger.info('get_hash failed for series %s', series_instance_uid)
            raise Exception('get_hash failed for series %s', series_instance_uid)

    ###-------------------Instance-----------------###

    def instances(self, series):
        instances = [instance['SOPInstanceUID'] for instance in get_TCIA_instance_uids_per_series(series.series_instance_uid, self.nbia_server)]
        return instances


    def instance_hashes_differ(self, instance):
        nbia_hash = self.src_instance_hash(instance.sop_instance_uid, )
        if nbia_hash:
            if instance.hash != nbia_hash:
                return 1
            else:
                return 0
        else:
            return -1


    def src_instance_hash(self, sop_instance_uid):
        try:
            result = self.get_instance_hash(sop_instance_uid)
        except Exception as exc:
            errlogger.error('Exception %s in src_instance_hash', exc)
            raise Exception('Exception %s in src_instance_hash', exc)
        if result:
            return result.content.decode()
        else:
            errlogger.info('get_hash failed for instance %s', sop_instance_uid)
            raise Exception('get_hash failed for instance %s', sop_instance_uid)

    def get_instance_hash(self, sop_instance_uid, access_token=None, refresh_token=None):
        # if not access_token:
        #     access_token, refresh_token = get_access_token(NBIA_AUTH_URL)
        # headers = dict(
        #     Authorization=f'Bearer {access_token}'
        # )
        # # url = "https://public-dev.cancerimagingarchive.net/nbia-api/services/getMD5Hierarchy"
        # url = f"{NBIA_V2_URL}/getM5HashForImage?SOPInstanceUid={sop_instance_uid}"
        # result = requests.get(url, headers=headers)
        self.lock.acquire()
        try:
            result = get_instance_hash(sop_instance_uid, self.access_token)
            if result.status_code == 401:
                # Refresh the token and try once more to get the hash
                self.access_token, self.refresh_token = refresh_access_token(self.refresh_token)
                result = get_instance_hash(sop_instance_uid, self.access_token)
                if result.status_code != 200:
                    result = None
            elif result.status_code != 200:
                result = None
        finally:
            self.lock.release()
            return result


class Pathology(Source):
    def __init__(self, sess):
        super().__init__(instance_source['path'].value)
        self.source = instance_source.path
        self.sess = sess

    ###-------------------Versions-----------------###

    def src_version_hash(self):
        collections = self.sess.execute(select(WSI_metadata.collection_id).distinct())
        hashes = []
        for collection in collections:
            hashes.append(self.src_collection_hash(collection['collection_id']))
        hash = get_merkle_hash(hashes)
        return hash

    ###-------------------Collections-----------------###

    def collections(self):
        stmt = select(WSI_metadata.collection_id).distinct()
        result = self.sess.execute(stmt)
        # collections = [row['collection_id'].lower() for row in result.fetchall()]
        collections = [row['collection_id'] for row in result.fetchall()]
        return collections


    def collection_was_revised(self, collection):
        return self.collection_hashes_differ(collection)


    def collection_hashes_differ(self, collection):
        # return self.idc_collection_hash(collection) != self.src_collection_hash(collection)
        return collection.hashes[self.source_id] != self.src_collection_hash(collection.collection_id)


    def src_collection_hash(self, collection_id):
        patients = self.sess.execute(select(WSI_metadata.submitter_case_id).distinct().where(WSI_metadata.collection_id == collection_id))
        hashes = []
        for patient in patients:
            hashes.append(self.src_patient_hash(collection_id, patient['submitter_case_id']))
        hash = get_merkle_hash(hashes) if len(hashes) else ""
        return hash

    ###-------------------Patients-----------------###

    def patients(self, collection):
        stmt = select(WSI_metadata.submitter_case_id).distinct().where(WSI_metadata.collection_id==collection.collection_id)
        result = self.sess.execute(stmt)
        patients = [row['submitter_case_id'] for row in result.fetchall()]
        return patients


    def patient_was_revised(self, patient):
        return self.patient_hashes_differ(patient)


    def patient_hashes_differ(self, patient):
        # return self.idc_patient_hash(patient) != self.src_patient_hash(patient)
        return patient.hashes[self.source_id] != self.src_patient_hash(patient.submitter_case_id)


    def src_patient_hash(self, collection_id, submitter_case_id):
        studies = self.sess.execute(select(WSI_metadata.study_instance_uid).distinct().where(WSI_metadata.submitter_case_id == submitter_case_id))
        hashes = []
        for study in studies:
            hashes.append(self.src_study_hash(study['study_instance_uid']))
        hash = get_merkle_hash(hashes) if len(hashes) else ""
        return hash

    ###-------------------Studies-----------------###

    def studies(self, patient):
        stmt = select(WSI_metadata.study_instance_uid).distinct().where(WSI_metadata.submitter_case_id==patient.submitter_case_id)
        result = self.sess.execute(stmt)
        studies = [row['study_instance_uid'] for row in result.fetchall()]
        return studies


    def study_was_revised(self, study):
        return self.study_hashes_differ(study)


    def src_study_hash(self, study_instance_uid):
        seriess = self.sess.execute(select(WSI_metadata.series_instance_uid).distinct().where(WSI_metadata.study_instance_uid == study_instance_uid))
        hashes = []
        for series in seriess:
            hashes.append(self.src_series_hash(series['series_instance_uid']))
        hash = get_merkle_hash(hashes) if len(hashes) else ""
        return hash

    ###-------------------Series-----------------###

    def series(self, study):
        stmt = select(WSI_metadata.series_instance_uid).distinct().where(WSI_metadata.study_instance_uid==study.study_instance_uid)
        result = self.sess.execute(stmt)
        series = [row['series_instance_uid'] for row in result.fetchall()]
        return series


    def series_was_revised(self, series):
        return self.series_hashes_differ(series)


    def study_hashes_differ(self, study):
        # return self.idc_study_hash(study) != self.src_study_hash(study)
        return study.hashes[self.source_id] != self.src_study_hash(study.study_instance_uid)


    def series_hashes_differ(self, series):
        # return self.idc_series_hash(series) != self.src_series_hash(series)
        return series.hashes[self.source_id] != self.src_series_hash(series.series_instance_uid)


    def src_series_hash(self, series_instance_uid):
        hashes = [hash['hash'] for hash in self.sess.execute(select(WSI_metadata.hash).where(WSI_metadata.series_instance_uid == series_instance_uid))]
        hash = get_merkle_hash(hashes) if len(hashes) else ""
        return hash

    ###-------------------Instances-----------------###

    def instances(self, series):
        stmt = select(WSI_metadata.sop_instance_uid).where(WSI_metadata.series_instance_uid==series.series_instance_uid)
        result = self.sess.execute(stmt)
        instances = [row['sop_instance_uid'] for row in result.fetchall()]
        return instances


    def instance_data(self, series):
        pass





