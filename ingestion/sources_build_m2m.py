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
from idc.models import Collection, Patient, Study, Series, Instance, WSI_metadata, instance_source
from sqlalchemy import select
from google.cloud import bigquery
import hashlib
import logging
rootlogger = logging.getLogger('root')
errlogger = logging.getLogger('root.err')




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
        self.client = bigquery.Client()


    # Compute object's hash as hash of sort childrens' hashes
    def idc_version_hash(self):
        objects = self.sess.execute(select(Collection)).fetchall()
        hashes = []
        for object in objects:
            hash = object[0].hashes[self.source_id] if object[0].hashes[self.source_id] != None else ""
            hashes.append(hash)
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
        objects = study.seriess
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




# class TCIA(Source):
#     def __init__(self, sess):
#         super().__init__(instance_source['tcia'].value)
#         self.source = instance_source.tcia
#         self.access_token, self.refresh_token = get_access_token()
#         self.sess = sess
#
#     def get_instance_hash(self, sop_instance_uid, access_token=None, refresh_token=None):
#         # if not access_token:
#         #     access_token, refresh_token = get_access_token(NBIA_AUTH_URL)
#         # headers = dict(
#         #     Authorization=f'Bearer {access_token}'
#         # )
#         # # url = "https://public-dev.cancerimagingarchive.net/nbia-api/services/getMD5Hierarchy"
#         # url = f"{NBIA_V2_URL}/getM5HashForImage?SOPInstanceUid={sop_instance_uid}"
#         # result = requests.get(url, headers=headers)
#         self.lock.acquire()
#         try:
#             result = get_instance_hash(sop_instance_uid, self.access_token)
#             if result.status_code == 401:
#                 # Refresh the token and try once more to get the hash
#                 self.access_token, self.refresh_token = refresh_access_token(self.refresh_token)
#                 result = get_instance_hash(sop_instance_uid, self.access_token)
#                 if result.status_code != 200:
#                     result = None
#             elif result.status_code != 200:
#                 result = None
#         finally:
#             self.lock.release()
#             return result
#
#
#     def get_hash(self, request_data, access_token=None, refresh_token=None):
#         # if not access_token:
#         #     access_token, refresh_token = get_access_token(NBIA_AUTH_URL)
#         # headers = dict(
#         #     Authorization=f'Bearer {access_token}'
#         # )
#         # # url = "https://public-dev.cancerimagingarchive.net/nbia-api/services/getMD5Hierarchy"
#         # url = f"{NBIA_V2_URL}/getM5HashForImage?SOPInstanceUid={sop_instance_uid}"
#         # result = requests.get(url, headers=headers)
#         self.lock.acquire()
#         try:
#             result = get_hash(request_data, self.access_token)
#             if result.status_code == 401:
#                 # Refresh the token and try once more to get the hash
#                 self.access_token, self.refresh_token = refresh_access_token(self.refresh_token)
#                 result = get_hash(request_data, self.access_token)
#                 if result.status_code != 200:
#                     result = None
#             elif result.status_code != 200:
#                 result = None
#         finally:
#             self.lock.release()
#             return result
#
#
#     def collections(self):
#         collections = get_collection_values_and_counts(self.nbia_server)
#         return collections
#
#
#     def patients(self, collection):
#         patients = [patient['PatientId'] for patient in get_TCIA_patients_per_collection(collection.collection_id, self.nbia_server)]
#         return patients
#
#
#     def studies(self, patient):
#         studies = [study['StudyInstanceUID'] for study in get_TCIA_studies_per_patient(patient.collection.collection_id, patient.submitter_case_id, self.nbia_server)]
#         return studies
#
#
#     def series(self, study):
#         series = [series['SeriesInstanceUID'] for series in get_TCIA_series_per_study(study.patient.collection.collection_id, study.patient.submitter_case_id, study.study_instance_uid, \
#                                          self.nbia_server)]
#         return series
#
#
#     def instances(self, series):
#         instances = [instance['SOPInstanceUID'] for instance in get_TCIA_instance_uids_per_series(series.series_instance_uid, self.nbia_server)]
#         return instances
#
#     # def instance_data(self, series):
#     #     download_start = time.time_ns()
#     #     get_TCIA_instances_per_series(args.dicom, series.series_instance_uid, args.server)
#     #     download_time = (time.time_ns() - download_start) / 10 ** 9
#     #     # Get a list of the files from the download
#     #     dcms = [dcm for dcm in os.listdir("{}/{}".format(args.dicom, series.series_instance_uid))]
#     #     return dcms
#
#     # def collection_hash(self, collection):
#
#
#     def src_version_hash(self):
#         collections = self.sess.execute(select(WSI_metadata.collection_id).distinct())
#         hashes = []
#         for collection in collections:
#             hashes.append(self.src_collection_hash(collection['collection_id']))
#         hash = get_merkle_hash(hashes)
#         return hash
#
#
#     def src_collection_hash(self, collection_id):
#         try:
#             # result, self.access_token, refresh_token = get_hash(
#             #     {'Collection': collection_id}, \
#             #     access_token=self.access_token, refresh_token=self.refresh_token)
#             result = self.get_hash({'Collection': collection_id})
#         except Exception as exc:
#             errlogger.error('Exception %s in src_collection_hash', exc)
#             raise Exception('Exception %s in src_collection_hash', exc)
#         if result:
#             return result.content.decode()
#         else:
#             rootlogger.info('get_hash failed for collection %s', collection_id)
#             raise Exception('get_hash failed for collection %s', collection_id)
#
#
#     def src_patient_hash(self, collection_id, submitter_case_id):
#         try:
#             result = self.get_hash({'Collection':collection_id, 'PatientID': submitter_case_id})
#         except Exception as exc:
#             errlogger.error('Exception %s in src_patient_hash', exc)
#             # raise Exception('Exception %s in src_patient_hash', exc)
#             return -1
#         if result:
#             return result.content.decode()
#         else:
#             rootlogger.info('get_hash failed for patient %s', submitter_case_id)
#             # raise Exception('get_hash failed for patient %s', submitter_case_id)
#             return -1
#
#
#     def src_study_hash(self, study_instance_uid):
#         try:
#             result = self.get_hash({'StudyInstanceUID': study_instance_uid})
#         except Exception as exc:
#             errlogger.error('Exception %s in src_study_hash', exc)
#             raise Exception('Exception %s in src_study_hash', exc)
#         if result:
#             return result.content.decode()
#         else:
#             rootlogger.info('get_hash failed for study %s', study_instance_uid)
#             raise Exception('get_hash failed for study %s', study_instance_uid)
#
#
#     def src_series_hash(self, series_instance_uid):
#         try:
#             result = self.get_hash({'SeriesInstanceUID': series_instance_uid})
#         except Exception as exc:
#             errlogger.error('Exception %s in src_series_hash', exc)
#             raise Exception('Exception %s in src_series_hash', exc)
#         if result:
#             return result.content.decode()
#         else:
#             rootlogger.info('get_hash failed for series %s', series_instance_uid)
#             raise Exception('get_hash failed for series %s', series_instance_uid)
#
#
#     def src_instance_hash(self, sop_instance_uid):
#         try:
#             result = self.get_instance_hash(sop_instance_uid)
#         except Exception as exc:
#             errlogger.error('Exception %s in src_instance_hash', exc)
#             raise Exception('Exception %s in src_instance_hash', exc)
#         if result:
#             return result.content.decode()
#         else:
#             errlogger.info('get_hash failed for instance %s', sop_instance_uid)
#             raise Exception('get_hash failed for instance %s', sop_instance_uid)
#
#
#     def version_hashes_differ(self, version):
#         return version.hashes[self.source_id] != self.src_version_hash()
#
#
#     def collection_hashes_differ(self, collection):
#         # return self.idc_collection_hash(collection) != self.src_collection_hash(collection)
#         nbia_hash = self.src_collection_hash(collection.collection_id)
#         if nbia_hash:
#             if collection.hashes[self.source_id] != nbia_hash:
#                 return 1
#             else:
#                 return 0
#         else:
#             return -1
#
#
#     def patient_hashes_differ(self, patient):
#         nbia_hash = self.src_patient_hash(patient.collection_id, patient.submitter_case_id)
#         if nbia_hash != -1:
#             if patient.hashes[self.source_id] != nbia_hash:
#                 return 1
#             else:
#                 return 0
#         else:
#             return -1
#
#
#     def study_hashes_differ(self, study):
#         nbia_hash = self.src_study_hash(study.study_instance_uid,)
#         if nbia_hash:
#             if study.hashes[self.source_id] != nbia_hash:
#                 return 1
#             else:
#                 return 0
#         else:
#             return -1
#
#
#     def series_hashes_differ(self, series):
#         nbia_hash = self.src_series_hash(series.series_instance_uid,)
#         if nbia_hash:
#             if series.hashes[self.source_id] != nbia_hash:
#                 return 1
#             else:
#                 return 0
#         else:
#             return -1
#
#
#     def instance_hashes_differ(self, instance):
#         nbia_hash = self.src_instance_hash(instance.sop_instance_uid,)
#         if nbia_hash:
#             if instance.hash != nbia_hash:
#                 return 1
#             else:
#                 return 0
#         else:
#             return -1
#
#
#     def collection_was_updated(self,collection):
#         result = self.collection_hashes_differ(collection)
#         if result == -1:
#             last_scan = collection.min_timestamp
#             last_scan_string = f'{last_scan.day}/{last_scan.month}/{last_scan.year}'
#             serieses = get_updated_series(last_scan_string)
#             updated = next((item for item in serieses if item["Collection"] == collection.collection_id), None)
#             return updated
#         else:
#             return result
#
#     def patient_was_updated(self,patient):
#         result = self.patient_hashes_differ(patient)
#         if result == -1:
#             last_scan = patient.min_timestamp
#             last_scan_string = f'{last_scan.day}/{last_scan.month}/{last_scan.year}'
#             serieses = get_updated_series(last_scan_string)
#             updated = next((item for item in serieses if item["PatientID"] == patient.submitter_case_id), None)
#             return updated
#         else:
#             return result
#
#     def study_was_updated(self,study):
#         result = self.study_hashes_differ(study)
#         if result == -1:
#             last_scan = study.min_timestamp
#             last_scan_string = f'{last_scan.day}/{last_scan.month}/{last_scan.year}'
#             serieses = get_updated_series(last_scan_string)
#             updated = next((item for item in serieses if item["StudyInstanceUID"] == study.study_instance_uid), None)
#             return updated
#         else:
#             return result
#
#     def series_was_updated(self,series):
#         result = self.series_hashes_differ(series)
#         if result == -1:
#             last_scan = series.min_timestamp
#             last_scan_string = f'{last_scan.day}/{last_scan.month}/{last_scan.year}'
#             serieses = get_updated_series(last_scan_string)
#             updated = next((item for item in serieses if item["SeriesInstanceUID"] == series.series_instance_uid), None)
#             return updated
#         else:
#             return result
#
#     # def instance_hashes_differ(self, instance):
#     #     return instance.path_hash != self.src_instance_hash(instance)


class TCIA(Source):
    def __init__(self, cur, version):
        super().__init__(instance_source['path'].value)
        self.source = instance_source.tcia
        self.cur = cur
        self.idc_version = version
        self.access_token, self.refresh_token = get_access_token()


    def collections(self):
        query = f"""
             SELECT tcia_api_collection_id, collection_timestamp, collection_hash
             FROM `idc-dev-etl.idc_v{self.idc_version}.collection`
             --WHERE collection_initial_idc_version={self.idc_version}
             ORDER BY tcia_api_collection_id
           """
        collections_metadata = {row['tcia_api_collection_id']:{
            'min_timestamp':row['collection_timestamp'],
            'sources':['True','False'],
            'hashes':[row['collection_hash'],'',row['collection_hash']]
            }
            for row in self.client.query(query).result()}
        return collections_metadata


    def patients(self, collection):
        query = f"""
            SELECT p.submitter_case_id, p.patient_timestamp, p.idc_case_id, p.patient_hash
                FROM `idc-dev-etl.idc_v{self.idc_version}.collection` as c 
                JOIN `idc-dev-etl.idc_v{self.idc_version}.patient` as p 
                ON  c.id = p.collection_id
                WHERE c.tcia_api_collection_id='{collection.collection_id}'
                ORDER BY p.submitter_case_id
        """
        patient_metadata = {row['submitter_case_id']: {
            'idc_case_id':row['idc_case_id'],
            'min_timestamp': row['patient_timestamp'],
            'sources': ['True', 'False'],
            'hashes': [row['patient_hash'], '', row['patient_hash']]
            }
            for row in self.client.query(query).result()}
        return patient_metadata


    def studies(self, patient):
        query = f"""
                SELECT st.study_instance_uid, st.study_timestamp, st.study_uuid, st.study_hash, st.study_instances
                FROM `idc-dev-etl.idc_v{self.idc_version}.patient` as p
                JOIN `idc-dev-etl.idc_v{self.idc_version}.study` as st
                ON  p.id = st.patient_id
                WHERE p.submitter_case_id='{patient.submitter_case_id}'
                ORDER BY st.study_instance_uid
        """
        try:
            study_metadata = {row['study_instance_uid']: {
            'uuid':row['study_uuid'],
            'min_timestamp': row['study_timestamp'],
            'study_instances':row['study_instances'],
            'sources': ['True', 'False'],
            'hashes': [row['study_hash'], '', row['study_hash']]
            }
            for row in self.client.query(query).result()}
        except Exception as exc:
            print(exc)
        return study_metadata


    def series(self, study):
        query = f"""
                SELECT se.series_instance_uid, se.series_timestamp, se.series_uuid, se.source_doi, se.series_hash, se.series_instances
                FROM `idc-dev-etl.idc_v{self.idc_version}.study` as st
                JOIN `idc-dev-etl.idc_v{self.idc_version}.series` as se
                ON  st.id = se.study_id
                WHERE st.study_instance_uid='{study.study_instance_uid}'
                ORDER BY se.series_instance_uid
        """
        series_metadata = {row['series_instance_uid']: {
            'uuid':row['series_uuid'],
            'min_timestamp': row['series_timestamp'],
            'source_doi':row['source_doi'],
            'series_instances':row['series_instances'],
            'sources': ['True', 'False'],
            'hashes': [row['series_hash'], '', row['series_hash']]
            }
            for row in self.client.query(query).result()}
        return series_metadata


    def instances(self, series):
        query = f"""
                SELECT i.sop_instance_uid, i.instance_timestamp, i.instance_uuid, i.instance_hash, i.instance_size
                FROM `idc-dev-etl.idc_v{self.idc_version}.series` as se
                JOIN `idc-dev-etl.idc_v{self.idc_version}.instance` as i
                ON  se.id = i.series_id
                WHERE se.series_instance_uid='{series.series_instance_uid}'
        """
        instance_metadata = {row['sop_instance_uid']: {
            'timestamp': row['instance_timestamp'],
            'uuid':row['instance_uuid'],
            'size':row['instance_size'],
            'source': self.source,
            'hash': row['instance_hash']
            } for row in self.client.query(query).result()}
        return instance_metadata


    def instance_data(self, series):
        pass

    def collection_was_updated(self,collection):
        result = self.collection_hashes_differ(collection)
        if result == -1:
            last_scan = collection.min_timestamp
            last_scan_string = f'{last_scan.day}/{last_scan.month}/{last_scan.year}'
            serieses = get_updated_series(last_scan_string)
            updated = next((item for item in serieses if item["Collection"] == collection.collection_id), None)
            return updated
        else:
            return result

    def patient_was_updated(self,patient):
        result = self.patient_hashes_differ(patient)
        if result == -1:
            last_scan = patient.min_timestamp
            last_scan_string = f'{last_scan.day}/{last_scan.month}/{last_scan.year}'
            serieses = get_updated_series(last_scan_string)
            updated = next((item for item in serieses if item["PatientID"] == patient.submitter_case_id), None)
            return updated
        else:
            return result

    def study_was_updated(self,study):
        result = self.study_hashes_differ(study)
        if result == -1:
            last_scan = study.min_timestamp
            last_scan_string = f'{last_scan.day}/{last_scan.month}/{last_scan.year}'
            serieses = get_updated_series(last_scan_string)
            updated = next((item for item in serieses if item["StudyInstanceUID"] == study.study_instance_uid), None)
            return updated
        else:
            return result

    def series_was_updated(self,series):
        result = self.series_hashes_differ(series)
        if result == -1:
            last_scan = series.min_timestamp
            last_scan_string = f'{last_scan.day}/{last_scan.month}/{last_scan.year}'
            serieses = get_updated_series(last_scan_string)
            updated = next((item for item in serieses if item["SeriesInstanceUID"] == series.series_instance_uid), None)
            return updated
        else:
        return result

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


class Pathology(Source):
    def __init__(self, cur):
        super().__init__(instance_source['path'].value)
        self.source = instance_source.tcia
        self.cur = cur
        self.access_token, self.refresh_token = get_access_token()


    def collections(self):
        query = f"""
             SELECT collection_id
             FROM collection
             ORDER BY collection_id
           """

        self.cur.execute(query)
        collections = [row.collection_id for row in self.cur.fetchall()]
        return collections


    def patients(self, collection):
        query = f"""
            SELECT submitter_case_id
            FROM patient
            ORDER BY submitter_case_id"""
        self.cur.execute(query)
        patients = [row.submitter_case_id for row in self.cur.fetchall()]
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










