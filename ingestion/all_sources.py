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
from ingestion.sources import TCIA, Pathology
import logging
rootlogger = logging.getLogger('root')
errlogger = logging.getLogger('root.err')


class All:

    def __init__(self, sess, version, lock):
        self.sess = sess
        self.idc_version = version
        self.client = bigquery.Client()
        self.sources = {}
        try:
            self.sources[instance_source.tcia] = TCIA(sess, lock)
            self.sources[instance_source.path] = Pathology(sess)
        except Exception as exc:
            print(exc)

    ###-------------------Versions-----------------###

    def idc_version_hashes(self, version):
        objects = version.collections
        hashes = {'tcia': [], 'path': []}
        for object in objects:
            for source in instance_source:
                if object.hashes[source.value]:
                    hashes[source.name].append(object.hashes[source.value])
        version_hashes = ['','','']
        version_hashes[0] = get_merkle_hash(hashes['tcia']) if len(hashes['tcia']) else ''
        version_hashes[1] = get_merkle_hash(hashes['path']) if len(hashes['path']) else ''

        # Now compute the hash of all sources
        version_hashes[-1] = get_merkle_hash([hash for hash in version_hashes[:-1]if hash])
        return version_hashes

    # Compute object's hashes according to sources
    def src_version_hashes(self, version):
        version_hashes = ['','']
        for source in self.sources:
            version_hashes[source.value] = self.sources[source].src_version_hash(version)
        return version_hashes

    ###-------------------Collections-----------------###

    # Get the collections from all sources
    def collections(self):
        collections = {}
        for source in instance_source:
            # Ideally, we would only collect collections from a source
            # we know that the version is revised in that source.
            # Because we cannot know this, at least for TCIA, we
            # have to assume that the version is revised in all sources.
            objects = self.sources[source].collections()
            for object in objects:
                if not object in collections:
                    collections[object] = [False, False]
                collections[object][source.value] = True

        # We explicitly add NLST, because it is not returned the above
        collections['NLST'] = [True, False]

        # Get a map from collection IDs to idc_collection_ids
        map = {row.collection_id: row.idc_collection_id for row in self.sess.query(Collection_id_map).all()}

        # Now generate entries for new collection IDs
        # First look for IDs that are like those already in the map
        for collection in collections:
            if not collection in map:
                # First check whether their is already a collection id that is the same except for case
                for collection_id in map:
                    if collection.lower == collection_id.lower:
                        print(f'Collection id change: {collection_id } to {collection}')
                        row = Collection_id_map(collection_id=collection, idc_collection_id=map[collection_id])
                        self.sess.add(row)
                        map[collection] = map[collection_id]
                        break
                # If we didn't find a similar collection id, assume this is a new collection
                if not collection in map:
                    print(f'New collection {collection}')
                    idc_collection_id = uuid4()
                    row = Collection_id_map(collection_id=collection, idc_collection_id=idc_collection_id)
                    self.sess.add(row)
                    map[collection] = idc_collection_id

        # Now we restructure collection_metadata so that it is indexed by the idc_collection_id
        # Note that it only includes those collection ids returned by the sources
        collection_metadata = {
            map[collection]: {
                'collection_id': collection,
                'sources': collections[collection]
            } for collection in collections
        }

        # Make a copy for subsequent access by other sources functions
        self.collection_metadata = collection_metadata
        return collection_metadata

    # Get objects per-source hashes from DB
    def idc_collection_hashes(self, collection):
        objects = collection.patients
        hashes = {'tcia': [], 'path': []}
        for object in objects:
            for source in instance_source:
                if object.hashes[source.value]:
                    hashes[source.name].append(object.hashes[source.value])
        collection_hashes = ['','','']
        collection_hashes[0] = get_merkle_hash(hashes['tcia']) if len(hashes['tcia']) else ''
        collection_hashes[1] = get_merkle_hash(hashes['path']) if len(hashes['path']) else ''

        # Now compute the hash of all sources
        collection_hashes[-1] = get_merkle_hash([hash for hash in collection_hashes[:-1]if hash])
        return collection_hashes


    # Compute object's hashes according to sources
    def src_collection_hashes(self, collection_id):
        collection_hashes = ['','']
        for source in self.sources:
            collection_hashes[source.value] = self.sources[source].src_collection_hash(collection_id)
        return collection_hashes

    ###-------------------Patients-----------------###

    # Get the patients in some collection from all sources
    def patients(self, collection):
        patients = {}
        for source in instance_source:
            # # Only collect patients from a source if the collection is in the source
            # # and is revised
            # if collection.revised[source.value]:
            if True:
                patient_ids = self.sources[source].patients(collection)
                for patient_id in patient_ids:
                    if not patient_id in patients:
                        patients[patient_id] = [False, False]
                    patients[patient_id][source.value] = True

        # Make a copy for subsequent access by other sources functions
        self.patient_metadata = patients
        return patients


    # Get objects per-source hashes from DB
    def idc_patient_hashes(self, patient):
        objects = patient.studies
        hashes = {'tcia': [], 'path': []}
        for object in objects:
            for source in instance_source:
                if object.hashes[source.value]:
                    hashes[source.name].append(object.hashes[source.value])
        patient_hashes = ['','','']
        patient_hashes[0] = get_merkle_hash(hashes['tcia']) if len(hashes['tcia']) else ''
        patient_hashes[1] = get_merkle_hash(hashes['path']) if len(hashes['path']) else ''

        # Now compute the hash of all sources
        patient_hashes[-1] = get_merkle_hash([hash for hash in patient_hashes[:-1]if hash])
        return patient_hashes

    # Compute object's hashes according to sources
    def src_patient_hashes(self, collection_id, submitter_case_id):
        patient_hashes = ['','']
        for source in self.sources:
            patient_hashes[source.value] = self.sources[source].src_patient_hash(collection_id,
                                                                             submitter_case_id)
        return patient_hashes

    ###-------------------Studies-----------------###

    # Get the studies in some patient from all sources
    def studies(self, patient):
        studies = {}
        for source in instance_source:
            # # Only collect patients from a source if the collection is in the source
            # # and is revised
            # if patient.revised[source.value]:
            if True:
                study_ids = self.sources[source].studies(patient)
                for study_id in study_ids:
                    if not study_id in studies:
                        studies[study_id] = [False, False]
                    studies[study_id][source.value] = True

        # Make a copy for subsequent access by other sources functions
        self.study_metadata = studies
        return studies

    # Get objects per-source hashes from DB
    def idc_study_hashes(self, study):
        objects = study.seriess
        hashes = {'tcia': [], 'path': []}
        for object in objects:
            for source in instance_source:
                if object.hashes[source.value]:
                    hashes[source.name].append(object.hashes[source.value])
        study_hashes = ['','','']
        study_hashes[0] = get_merkle_hash(hashes['tcia']) if len(hashes['tcia']) else ''
        study_hashes[1] = get_merkle_hash(hashes['path']) if len(hashes['path']) else ''

        # Now compute the hash of all sources
        study_hashes[-1] = get_merkle_hash([hash for hash in study_hashes[:-1]if hash])
        return study_hashes

    # Compute object's hashes according to sources
    def src_study_hashes(self, study_instance_uid):
        study_hashes = ['','']
        for source in self.sources:
            study_hashes[source.value] = self.sources[source].src_study_hash(study_instance_uid)
        return study_hashes

    ###-------------------Series-----------------###

    # Get the series in some study from all sources
    def series(self, study):
        seriess = {}
        for source in instance_source:
            # # Only collect series from a source if the collection is in the source
            # # and is revised
            # if study.revised[source.value]:
            if True:
                series_ids = self.sources[source].series(study)
                for series_id in series_ids:
                    if not series_id in seriess:
                        seriess[series_id] = [False, False]
                    seriess[series_id][source.value] = True

        # Make a copy for subsequent access by other sources functions
        self.series_metadata = seriess
        return seriess

    # Get objects per-source hashes from DB
    def idc_series_hashes(self, series):
        objects = series.instances
        hashes = {'tcia': [], 'path': []}
        for object in objects:
            if object.hash:
                hashes[object.source.name].append(object.hash)
        series_hashes = ['', '', '']
        series_hashes[0] = get_merkle_hash(hashes['tcia']) if len(hashes['tcia']) else ''
        series_hashes[1] = get_merkle_hash(hashes['path']) if len(hashes['path']) else ''

        # Now compute the hash of all sources
        series_hashes[-1] = get_merkle_hash([hash for hash in series_hashes[:-1] if hash])
        return series_hashes


    # Compute object's hashes according to sources
    def src_series_hashes(self, series_instance_uid):
        series_hashes = ['', '']
        for source in self.sources:
            series_hashes[source.value] = self.sources[source].src_series_hash(series_instance_uid)

        return series_hashes

    ###-------------------Instances-----------------###

    # Get the instances in some series from all sources
    def instances(self, series):
        instance_metadata = {}
        for source in instance_source:
            # # Only collect series from a source if the series is
            # # is revised in the source.
            # # We assume that all instances is a series are from a
            # # a single source, therefor we only return a scalar
            # if series.revised[source.value]:
            if True:
                instance_ids = self.sources[source].instances(series)
                for instance_id in instance_ids:
                    instance_metadata[instance_id] = source.name
                break

        # Make a copy for subsequent access by other sources functions
        self.instance_metadata = instance_metadata
        return instance_metadata

    # Compute object's hashes according to sources
    def src_instance_hashes(self, sop_instance_uid, source):
        instance_hash = self.sources[instance_source[source]].src_instance_hash(sop_instance_uid)
        return instance_hash















