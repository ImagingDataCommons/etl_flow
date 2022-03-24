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
import time

from utilities.tcia_helpers import  get_access_token, get_hash, get_TCIA_studies_per_patient, get_TCIA_patients_per_collection,\
    get_TCIA_series_per_study, get_TCIA_instance_uids_per_series, get_TCIA_instances_per_series, get_collection_values_and_counts,\
    get_updated_series, get_instance_hash, refresh_access_token
from uuid import uuid4
from idc.models  import WSI_Version, WSI_Collection, WSI_Patient, WSI_Study, WSI_Series, WSI_Instance, instance_source
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
    def __init__(self, pid, sess, access, skipped_collections, lock):
        super().__init__(instance_source['tcia'].value)
        self.source = instance_source.tcia
        # self.access_token, self.refresh_token = get_access_token()
        self.pid = pid
        self.sess = sess
        self.access = access
        self.skipped_collections = skipped_collections
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
            # result = get_hash(request_data, self.access_token)
            result = get_hash(request_data, self.access[0])
            if result.status_code == 401:
                # Refresh the token and try once more to get the hash
                # errlogger.error('%s Refreshing access token %s', access_token)
                # self.access_token, self.refresh_token = refresh_access_token(self.refresh_token)
                # result = get_hash(request_data, self.access_token)
                errlogger.error('p%s Refreshing access token %s, refresh token %s at %s', self.pid, self.access[0], self.access[1], time.asctime(time.localtime()))
                self.access[0], self.access[1] = refresh_access_token(self.access[1])
                errlogger.error('p%s After refresh, token %s, refresh token %s', self.pid, self.access[0], self.access[1])
                result = get_hash(request_data, self.access[0])
                if result.status_code != 200:
                    result = None
            elif result.status_code != 200:
                result = None
        finally:
            self.lock.release()
            return result

    ###-------------------Versions-----------------###


    def src_version_hash(self):
        collections = self.sess.execute(select(WSI_Collection.collection_id).distinct())
        hashes = []
        for collection in collections:
            hashes.append(self.src_collection_hash(collection['collection_id']))
        hash = get_merkle_hash(hashes)
        return hash


    ###-------------------Collections-----------------###


    def collections(self):
        # Get TCIAs list of collections
        collections = get_collection_values_and_counts(self.nbia_server)
        # Remove any collections to be skipped
        for collection in self.skipped_collections:
            try:
                collections.remove(collection)
            except:
                continue
        return collections


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


    def src_study_hash(self, collection_id, study_instance_uid):
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

    def instances(self, collection, series):
        instances = [instance['SOPInstanceUID'] for instance in get_TCIA_instance_uids_per_series(collection.collection_id, series.series_instance_uid, self.nbia_server)]
        return instances


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
    def __init__(self, sess, skipped_collections):
        super().__init__(instance_source['path'].value)
        self.source = instance_source.path
        self.sess = sess
        self.skipped_collections = skipped_collections

    ###-------------------Versions-----------------###

    def src_version_hash(self):
        query = select(WSI_Version.hash)
        hash = self.sess.execute(query).fetchone().hash
        return hash

    ###-------------------Collections-----------------###

    def collections(self):
        query = select(WSI_Collection.collection_id)
        collections = [row.collection_id for row in self.sess.execute(query).fetchall()]
        for collection in self.skipped_collections:
            try:
                collections.remove(collection)
            except:
                continue
        return collections


    def src_collection_hash(self, collection_id):
        query = select(WSI_Collection.hash).where(WSI_Collection.collection_id == collection_id)
        hash = self.sess.execute(query).fetchone().hash if self.sess.execute(query).fetchone() else ""
        return hash

    ###-------------------Patients-----------------###

    def patients(self, collection):
        query = select(WSI_Patient.submitter_case_id).where(WSI_Patient.collection_id == collection.collection_id)
        patients = [row.submitter_case_id for row in self.sess.execute(query).fetchall()]
        return patients


    def src_patient_hash(self, collection_id, submitter_case_id):
        query = select(WSI_Patient.hash).where(WSI_Patient.submitter_case_id == submitter_case_id)
        hash = self.sess.execute(query).fetchone().hash if self.sess.execute(query).fetchone() else ""
        return hash

    ###-------------------Studies-----------------###

    def studies(self, patient):
        query = select(WSI_Study.study_instance_uid).where(WSI_Study.submitter_case_id == patient.submitter_case_id)
        studies = [row.study_instance_uid for row in self.sess.execute(query).fetchall()]
        return studies


    def src_study_hash(self, collection_id, study_instance_uid):
        query = select(WSI_Study.hash).where(WSI_Study.study_instance_uid == study_instance_uid)
        hash = self.sess.execute(query).fetchone().hash if self.sess.execute(query).fetchone() else ""
        return hash

    ###-------------------Series-----------------###

    def series(self, study):
        query = select(WSI_Series.series_instance_uid).where(WSI_Series.study_instance_uid == study.study_instance_uid)
        series = [row.series_instance_uid for row in self.sess.execute(query).fetchall()]
        return series


    def src_series_hash(self, series_instance_uid):
        query = select(WSI_Series.hash).where(WSI_Series.series_instance_uid == series_instance_uid)
        hash = self.sess.execute(query).fetchone().hash if self.sess.execute(query).fetchone() else ""
        return hash

    ###-------------------Instances-----------------###

    def instances(self, collection, series):
        query = select(WSI_Instance.sop_instance_uid).where(WSI_Instance.series_instance_uid == series.series_instance_uid)
        instances = [row.sop_instance_uid for row in self.sess.execute(query).fetchall()]
        return instances