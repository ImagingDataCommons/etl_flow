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
from ingestion.sources import All, TCIA, Pathology
from idc.models_v5  import Collection, Patient, Study, Series, Instance, WSI_metadata, instance_source
from sqlalchemy import select
from google.cloud import bigquery
import hashlib
import logging
rootlogger = logging.getLogger('root')
errlogger = logging.getLogger('root.err')

class TCIA_mtm(TCIA):
    def __init__(self, sess, version):
        super().__init__(instance_source['path'].value)
        self.source = instance_source.tcia
        self.sess = sess
        self.idc_version = version
        self.access_token, self.refresh_token = get_access_token()
        self.client = bigquery.Client()


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


class Pathology_mtm(Pathology):
    def __init__(self, sess):
        super().__init__(instance_source['path'].value)
        self.client = bigquery.Client()
        self.source = instance_source.path
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




class All_mtm(All):

    def __init__(self, sess, mtm_sess, version):
        super().__init__(sess, version)
        self.sources = {}
        self.sess = mtm_sess
        self.sources[instance_source.tcia] = TCIA_mtm(mtm_sess, version)
        self.sources[instance_source.path] = Pathology_mtm(mtm_sess)

    # def __init__(self, sess, version):
    #     super().__init__(instance_source['path'].value)
    #     self.source = instance_source.tcia
    #     self.sess = sess
    #     self.idc_version = version
    #     self.access_token, self.refresh_token = get_access_token()
    #     self.client = bigquery.Client()


    def collections(self):
        # stmt = select(Collection.collection_id, Collection.rev_idc_version, Collection.min_timestamp, \
        #               Collection.max_timestamp, Collection.hashes, Collection.sources)
        stmt = select(Collection)
        collection_metadata = {row.collection_id: {
            'rev_idc_version':row.rev_idc_version,
            'min_timestamp': row.min_timestamp,
            'max_timestamp': row.max_timestamp,
            'sources': list(row.sources._asdict().values()),
            'hashes': list(dict({key: val if not val is None else "" for key, val in row.hashes._asdict().items()}).values())
            } for row in self.sess.query(Collection).all()}

        # Make a copy for subsequent access by other sources functions
        self.collection_metadata = collection_metadata
        return collection_metadata

    # def collections(self):
    #     if self.idc_version in [1,2]:
    #         query = f"""
    #              SELECT tcia_api_collection_id, collection_timestamp, collection_hash
    #              FROM `idc-dev-etl.idc_v{self.idc_version}.collection`
    #              --WHERE collection_initial_idc_version={self.idc_version}
    #              ORDER BY tcia_api_collection_id
    #            """
    #         collection_metadata = {row['tcia_api_collection_id']:{
    #             'min_timestamp':row['collection_timestamp'],
    #             'max_timestamp':row['collection_timestamp'],
    #             'sources':['True','False'],
    #             'hashes':[row['collection_hash'],'',row['collection_hash']]
    #             } for row in self.client.query(query).result()}
    #     elif self.idc_version in [3]:
    #         query = f"""
    #             SELECT collection_id, rev_idc_version, min_timestamp, max_timestamp, hashes, sources
    #             FROM `idc-dev-etl.idc_v{self.idc_version}.collection`
    #              --WHERE collection_initial_idc_version={self.idc_version}
    #              ORDER BY collection_id
    #            """
    #         collection_metadata = {row['collection_id']:{
    #             'rev_idc_version':row['rev_idc_version'],
    #             'min_timestamp':row['min_timestamp'],
    #             'max_timestamp':row['max_timestamp'],
    #             'sources':row['sources'],
    #             # 'hashes':dict({key:val for key,val in row['hashes'] if not val is None else ''}, **{'all':""})
    #             'hashes':dict({key:val if not val is None else "" for key,val in row['hashes'].items()} , **{'all':""})
    #             } for row in self.client.query(query).result()}
    #     else:
    #         query = f"""
    #             SELECT collection_id, rev_idc_version, min_timestamp, max_timestamp, hashes, sources
    #             FROM `idc-dev-etl.idc_v{self.idc_version}.collection`
    #             ORDER BY collection_id
    #             """
    #         collection_metadata = {row['collection_id']: {
    #             'rev_idc_version':row['rev_idc_version'],
    #             'min_timestamp': row['min_timestamp'],
    #             'max_timestamp': row['max_timestamp'],
    #             'sources': row['sources'],
    #             'hashes': dict({key: val if not val is None else "" for key, val in row['hashes'].items()})
    #             # 'hashes': row['hashes']
    #             } for row in self.client.query(query).result()}
    #
    #     self.collection_metadata = collection_metadata
    #     return collection_metadata


    def patients(self, collection):
        if self.idc_version in [1,2]:
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
                'max_timestamp': row['patient_timestamp'],
                'sources': ['True', 'False'],
                'hashes': [row['patient_hash'], '', row['patient_hash']]
                } for row in self.client.query(query).result()}
        elif self.idc_version in [3]:
            query = f"""
                SELECT p.submitter_case_id, p.rev_idc_version, p.idc_case_id, p.min_timestamp,  p.max_timestamp, p.hashes, p.sources
                    FROM `idc-dev-etl.idc_v{self.idc_version}.collection` as c 
                    JOIN `idc-dev-etl.idc_v{self.idc_version}.patient` as p 
                    ON  c.collection_id = p.collection_id
                    WHERE c.collection_id='{collection.collection_id}'
                    ORDER BY p.submitter_case_id
            """
            patient_metadata = {row['submitter_case_id']: {
                'idc_case_id': row['idc_case_id'],
                'rev_idc_version':row['rev_idc_version'],
                'min_timestamp': row['min_timestamp'],
                'max_timestamp': row['max_timestamp'],
                'sources': row['sources'],
                # 'hashes': dict(row['hashes'], **{'all':""})
                'hashes':dict({key:val if not val is None else "" for key,val in row['hashes'].items()} , **{'all':""})
                } for row in self.client.query(query).result()}
        else:
            query = f"""
                SELECT p.submitter_case_id, p.rev_idc_version, p.idc_case_id, p.min_timestamp,  p.max_timestamp, p.hashes, p.sources
                    FROM `idc-dev-etl.idc_v{self.idc_version}.collection` as c 
                    JOIN `idc-dev-etl.idc_v{self.idc_version}.patient` as p 
                    ON  c.collection_id = p.collection_id
                    WHERE c.collection_id='{collection.collection_id}'
                    ORDER BY p.submitter_case_id
            """
            patient_metadata = {row['submitter_case_id']: {
                'idc_case_id': row['idc_case_id'],
                'rev_idc_version':row['rev_idc_version'],
                'min_timestamp': row['min_timestamp'],
                'max_timestamp': row['max_timestamp'],
                'sources': row['sources'],
                'hashes': dict({key: val if not val is None else "" for key, val in row['hashes'].items()})
                # 'hashes': row['hashes']
                } for row in self.client.query(query).result()}

        self.patient_metadata = patient_metadata
        return patient_metadata


    def studies(self, patient):
        if self.idc_version in [1,2]:
            query = f"""
                    SELECT st.study_instance_uid, st.study_timestamp, st.study_uuid, st.study_hash, st.study_instances
                    FROM `idc-dev-etl.idc_v{self.idc_version}.patient` as p
                    JOIN `idc-dev-etl.idc_v{self.idc_version}.study` as st
                    ON  p.id = st.patient_id
                    WHERE p.submitter_case_id='{patient.submitter_case_id}'
                    ORDER BY st.study_instance_uid
            """
            study_metadata = {row['study_instance_uid']: {
                'uuid':row['study_uuid'],
                'min_timestamp': row['study_timestamp'],
                'max_timestamp': row['study_timestamp'],
                'study_instances':row['study_instances'],
                'sources': ['True', 'False'],
                'hashes': [row['study_hash'], '', row['study_hash']]
                } for row in self.client.query(query).result()}
        elif self.idc_version in [3]:
            query = f"""
                     SELECT st.study_instance_uid, st.rev_idc_version, st.uuid, st.min_timestamp, st.max_timestamp, st.hashes, st.sources, 
                            st.study_instances
                     FROM `idc-dev-etl.idc_v{self.idc_version}.patient` as p
                     JOIN `idc-dev-etl.idc_v{self.idc_version}.study` as st
                     ON  p.submitter_case_id = st.submitter_case_id
                     WHERE p.submitter_case_id='{patient.submitter_case_id}'
                     ORDER BY st.study_instance_uid
             """
            study_metadata = {row['study_instance_uid']: {
                'uuid': row['uuid'],
                'rev_idc_version':row['rev_idc_version'],
                'study_instances': row['study_instances'],
                'min_timestamp': row['min_timestamp'],
                'max_timestamp': row['max_timestamp'],
                'sources': row['sources'],
                # 'hashes': dict(row['hashes'], **{'all':""})
                'hashes':dict({key:val if not val is None else "" for key,val in row['hashes'].items()} , **{'all':""})
                } for row in self.client.query(query).result()}
        else:
            query = f"""
                     SELECT st.study_instance_uid, st.rev_idc_version, st.uuid, st.min_timestamp, st.max_timestamp, st.hashes, st.sources, 
                            st.study_instances
                     FROM `idc-dev-etl.idc_v{self.idc_version}.patient` as p
                     JOIN `idc-dev-etl.idc_v{self.idc_version}.study` as st
                     ON  p.submitter_case_id = st.submitter_case_id
                     WHERE p.submitter_case_id='{patient.submitter_case_id}'
                     ORDER BY st.study_instance_uid
             """
            study_metadata = {row['study_instance_uid']: {
                'uuid': row['uuid'],
                'rev_idc_version':row['rev_idc_version'],
                'study_instances': row['study_instances'],
                'min_timestamp': row['min_timestamp'],
                'max_timestamp': row['max_timestamp'],
                'sources': row['sources'],
                'hashes': dict({key: val if not val is None else "" for key, val in row['hashes'].items()})
                # 'hashes': row['hashes']
                } for row in self.client.query(query).result()}

        self.study_metadata = study_metadata
        return study_metadata


    def series(self, study):
        if self.idc_version in [1,2]:
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
                'max_timestamp': row['series_timestamp'],
                'source_doi':row['source_doi'],
                'series_instances':row['series_instances'],
                'sources': ['True', 'False'],
                'hashes': [row['series_hash'], '', row['series_hash']]
                }
                for row in self.client.query(query).result()}
        elif self.idc_version in [3]:
            query = f"""
                    SELECT se.series_instance_uid, se.rev_idc_version, se.source_doi, se.uuid, se.min_timestamp, se.max_timestamp, se.hashes, se.sources, 
                            se.series_instances
                    FROM `idc-dev-etl.idc_v{self.idc_version}.study` as st
                    JOIN `idc-dev-etl.idc_v{self.idc_version}.series` as se
                    ON  st.study_instance_uid = se.study_instance_uid
                    WHERE st.study_instance_uid='{study.study_instance_uid}'
                    ORDER BY se.series_instance_uid
            """
            series_metadata = {row['series_instance_uid']: {
                'uuid':row['uuid'],
                'rev_idc_version':row['rev_idc_version'],
                'source_doi':row['source_doi'],
                'series_instances':row['series_instances'],
                'min_timestamp': row['min_timestamp'],
                'max_timestamp': row['max_timestamp'],
                'sources': row['sources'],
                # 'hashes': dict(row['hashes'], **{'all':""})
                'hashes':dict({key:val if not val is None else "" for key,val in row['hashes'].items()} , **{'all':""})
                }
                for row in self.client.query(query).result()}
        else:
            query = f"""
                    SELECT se.series_instance_uid, se.rev_idc_version, se.source_doi, se.uuid, se.min_timestamp, se.max_timestamp, se.hashes, se.sources, 
                            se.series_instances
                    FROM `idc-dev-etl.idc_v{self.idc_version}.study` as st
                    JOIN `idc-dev-etl.idc_v{self.idc_version}.series` as se
                    ON  st.study_instance_uid = se.study_instance_uid
                    WHERE st.study_instance_uid='{study.study_instance_uid}'
                    ORDER BY se.series_instance_uid
            """
            series_metadata = {row['series_instance_uid']: {
                'uuid':row['uuid'],
                'rev_idc_version':row['rev_idc_version'],
                'source_doi':row['source_doi'],
                'series_instances':row['series_instances'],
                'min_timestamp': row['min_timestamp'],
                'max_timestamp': row['max_timestamp'],
                'sources': row['sources'],
                'hashes': dict({key: val if not val is None else "" for key, val in row['hashes'].items()})
                # 'hashes': row['hashes']
                } for row in self.client.query(query).result()}

        self.series_metadata = series_metadata
        return series_metadata


    def instances(self, series):
        if self.idc_version in [1,2]:
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
                'source': 'tcia',
                'hash': row['instance_hash']
                } for row in self.client.query(query).result()}
        else:
            query = f"""
                    SELECT i.sop_instance_uid, i.rev_idc_version, i.timestamp, i.uuid, i.hash, i.source, i.size
                    FROM `idc-dev-etl.idc_v{self.idc_version}.series` as se
                    JOIN `idc-dev-etl.idc_v{self.idc_version}.instance` as i
                    ON  se.series_instance_uid = i.series_instance_uid
                    WHERE se.series_instance_uid='{series.series_instance_uid}'
            """
            instance_metadata = {row['sop_instance_uid']: {
                'rev_idc_version':row['rev_idc_version'],
                'timestamp': row['timestamp'],
                'uuid':row['uuid'],
                'size':row['size'],
                'source': row['source'],
                'hash': row['hash']
                } for row in self.client.query(query).result()}


        self.instance_metadata = instance_metadata
        return instance_metadata


    def collection_was_updated(self, collection):
        updated=False;
        for source in self.sources:
            # updated |= (collection.hashes[source.value] if not collection.hashes[source.value] is None else '') != \
            #            (self.collection_metadata[collection.collection_id]['hashes'][source.name] if  \
            #     not self.collection_metadata[collection.collection_id]['hashes'][source.name] is None else '')
            updated |= collection.hashes[source.value] != self.collection_metadata[collection.collection_id]['hashes'][source.name]
        return updated


    def patient_was_updated(self, patient):
        updated=False;
        for source in self.sources:
            updated |= patient.hashes[source.value] != self.patient_metadata[patient.submitter_case_id]['hashes'][source.name]
        return updated


    def study_was_updated(self, study):
        updated=False;
        for source in self.sources:
            updated |= study.hashes[source.value] != self.study_metadata[study.study_instance_uid]['hashes'][source.name]
        return updated


    def series_was_updated(self, series):
        updated=False;
        for source in self.sources:
            updated |= series.hashes[source.value] != self.series_metadata[series.series_instance_uid]['hashes'][source.name]
        return updated



















