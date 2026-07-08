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
# distributed under the License is distributed on an "AS IS" BASIS
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sqlalchemy as sa
from sqlalchemy import Integer, String, Boolean, BigInteger,\
    Column, DateTime, ForeignKey, create_engine, MetaData, Table, ForeignKeyConstraint, Enum, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref
from sqlalchemy_utils import CompositeType

import enum


class instance_source(enum.Enum):
    # tcia = 0
    idc = 0
    # all_sources = 2

Base = declarative_base()
# sql_engine = create_engine(sql_uri, echo=True)
# sql_engine = create_engine(sql_uri)

# These tables define the ETL database. There is a separate DB for each IDC version.
# Note that earlier IDC versions used a one-to-many schema.
# Flattened idc hierarchy (idc_collection, idc_patient,...). The underlying PSQL is a view.
class All_Data_Snapshot(Base):
    __tablename__ = 'all_data_snapshot'
    collection_name = Column(String, nullable=False, comment='TCIA/NBIA collection ID')
    collection_id = Column(String, nullable=False, comment='webapp collection ID')
    c_hash = Column(String, comment='Collection hash')

    patientID = Column(String, nullable=False, unique=True, primary_key=True, comment="Submitter's patient ID")
    p_hash = Column(String, comment='Patient hash')

    StudyInstanceUID = Column(String, unique=True, primary_key=True, nullable=False)
    st_hash = Column(String, comment='Study hash')

    SeriesInstanceUID = Column(String, unique=True, primary_key=True, nullable=False)
    se_hash = Column(String, comment='Series hash')
    se_excluded = Column(Boolean, default=False, comment='True if this series should be excluded from ingestion')
    source_doi = Column(String, comment='Source DOI of this series\' wiki')
    source_url = Column(String, comment='Source URL of this series\' wiki')
    versioned_source_doi = Column(String, comment='If present, a DOI to the wiki page of this version of this series')
    versioned_source_url = Column(String, comment='If present, a URL to the wiki page of this version of this series')
    analysis_result = Column(Boolean, comment='True if this series is from an analysis result, else False')
    SOPInstanceUID = Column(String, primary_key=True, nullable=False)
    i_hash = Column(String, comment='Instance hash')
    gcs_url = Column(String, comment='GCS URL of instance source')
    size = Column(BigInteger, comment='Instance size in bytes')
    i_excluded = Column(Boolean, default=False, comment='True if this instance should be excluded from ingestion')
    idc_version = Column(Integer, comment='IDC version when this instance was added/revised')
    mitigation = Column(String, default="", comment="ID of the mitigation which redacted this instance")
    source_file_hash = Column(String, default="", comment="md5 hash of the source file from which this instance was derived")

# Flattened idc hierarchy (idc_collection, idc_patient,...). The underlying PSQL is a view.
class IDC_All_Joined(Base):
    __tablename__ = 'idc_all_joined'
    collection_id = Column(String, nullable=False, comment='TCIA/NBIA collection ID')
    c_hash = Column(String, comment='Collection hash')

    submitter_case_id = Column(String, nullable=False, unique=True, primary_key=True, comment="Submitter's patient ID")
    p_hash = Column(String, comment='Patient hash')

    study_instance_uid = Column(String, unique=True, primary_key=True, nullable=False)
    st_hash = Column(String, comment='Study hash')

    series_instance_uid = Column(String, unique=True, primary_key=True, nullable=False)
    se_hash = Column(String, comment='Series hash')
    se_excluded = Column(Boolean, default=False, comment='True if this series should be excluded from ingestion')
    source_doi = Column(String, comment='Source DOI of this series\' wiki')
    source_url = Column(String, comment='Source URL of this series\' wiki')
    versioned_source_doi = Column(String, comment='If present, a DOI to the wiki page of this version of this series')
    third_party = Column(Boolean, comment='True if this series is from an analysis result, else False')
    license_long_name = Column(String, comment='Long name of license')
    license_short_name = Column(String, comment='short name of license')
    license_url = Column(String, comment='URL of license description')
    # redacted = Column(Boolean, default=False, comment="True if object has been redacted")
    # ingestion_script = Column(String, comment='Script which added this instance')

    sop_instance_uid = Column(String, primary_key=True, nullable=False)
    i_hash = Column(String, comment='Instance hash')
    gcs_url = Column(String, comment='GCS URL of instance source')
    size = Column(BigInteger, comment='Instance size in bytes')
    i_excluded = Column(Boolean, default=False, comment='True if this instance should be excluded from ingestion')
    idc_version = Column(Integer, comment='IDC version when this instance was added/revised')
    # redacted = Column(Boolean, default=False, comment="True if object has been redacted")
    # mitigation = Column(String, default="", comment="ID of the mitigation which redacted this instance")
    # source_file_hash = Column(String, default="", comment="md5 hash of the source file from which this instance was derived")


# Flattened hierarchy. The underlying PSQL is a view.
class All_Joined(Base):
    __tablename__ = 'all_joined'
    idc_version = Column(Integer, nullable=False, comment="Target version of revision")
    previous_idc_version = Column(Integer, nullable=False, comment="ID of the previous version")
    v_hash = Column(String, nullable=True, comment="Hex format MD5 hash of TCIA data at this level")

    collection_name = Column(String, nullable=False, comment='TCIA/NBIA collection ID')
    idc_collection_uuid = Column(String, nullable=False, comment="IDC assigned collection ID")
    c_uuid = Column(String, nullable=False, comment="IDC assigned UUID of a version of this object")
    c_hash = Column(String, nullable=True, comment="Hex format MD5 hash of TCIA data at this level")
    c_init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    c_rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    c_final_idc_version = Column(Integer, default=0, comment="Final IDC version of this version of this object")

    patientID = Column(String, nullable=False, comment="Submitter's patient ID")
    idc_case_id = Column(String, nullable=False, comment="IDC assigned patient ID")
    p_uuid = Column(String, nullable=False, comment="IDC assigned UUID of a version of this object")
    p_hash = Column(String, nullable=True, comment="Hex format MD5 hash of TCIA data at this level")
    p_init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    p_rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    p_final_idc_version = Column(Integer, default=0, comment="Final IDC version of this version of this object")

    studyinstanceuid = Column(String, nullable=False, comment="DICOM StudyInstanceUID")
    st_uuid = Column(String, nullable=False, comment="IDC assigned UUID of a version of this object")
    study_instances = Column(Integer, nullable=True, comment="Instances in this study")
    st_hash = Column(String, nullable=True, comment="Hex format MD5 hash of TCIA data at this level")
    st_init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    st_rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    st_final_idc_version = Column(Integer, default=0, comment="Final IDC version of this version of this object")

    seriesinstanceuid = Column(String, nullable=False, comment="DICOM SeriesInstanceUID")
    se_uuid = Column(String, nullable=False, comment="IDC assigned UUID of a version of this object")
    series_instances = Column(Integer, nullable=True, comment="Instances in this series")
    source_doi = Column(String, nullable=True, comment="A doi to the wiki page of the source of this series")
    source_url = Column(String, nullable=True, comment="A url to the wiki page of the source of this series")
    versioned_source_doi = Column(String, nullable=True, comment="A versioned doi to the wiki page of the source of this series")
    se_hash = Column(String, nullable=True, comment="Hex format MD5 hash of data at this level")
    se_init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    se_rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    se_final_idc_version = Column(Integer, default=0, comment="Final IDC version of this version of this object")

    sopinstanceuid = Column(String, nullable=False, unique=False, comment='DICOM SOPInstanceUID')
    i_uuid = Column(String, primary_key=True, comment="IDC assigned UUID of a version of this object")
    i_hash = Column(String, nullable=True, comment="Hex format MD5 hash of data at this level")
    i_source = Column(String, nullable=True, comment="'tcia' or 'idc'")
    i_size = Column(BigInteger, nullable=True, comment='Instance blob size (bytes)')
    i_excluded = Column(Boolean, default=False, comment="True if instance should be excluded from auxiliary_metacata, etc.")
    i_init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    i_rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    i_final_idc_version = Column(Integer, default=0, comment="Final IDC version of this version of this object")
    i_redacted = Column(Boolean, default=False, comment="True if instance has been redacted")
    i_mitigation = Column(String, default="", comment="ID of the mitigation which redacted this instance")
    ingestion_url = Column(String, default="", comment="GCS URL of the blob from which this instance was ingested. Does not apply if source='tcia'")
    source_file_hash = Column(String, default="", comment="md5 hash of the source file from which this instance was derived")

'''
---------------------------------------------------------------------------------------------------------------------
'''
version_collection = Table('version_collection', Base.metadata,
                           Column('version', ForeignKey('version.version'), primary_key=True),
                           Column('collection_uuid', ForeignKey('collection.uuid'), primary_key=True))

'''
---------------------------------------------------------------------------------------------------------------------
'''
class Version(Base):
    __tablename__ = 'version'

    version = Column(Integer, primary_key=True, comment="Target version of revision")
    previous_version = Column(Integer, nullable=False, comment="ID of the previous version")
    min_timestamp = Column(DateTime, nullable=True, comment="Time when building this object started")
    max_timestamp = Column(DateTime, nullable=True, comment="Time when building this object completed")
    # revised = Column(Boolean, default=False, comment="If True, this object is revised relative to the previous IDC version")
    done = Column(Boolean, default=False, comment="Set to True if this object has been processed")
    is_new = Column(Boolean, default=False, comment="True if this object is new in this version")
    expanded = Column(Boolean, default=False, comment="True if the next lower level has been populated")
    hash = Column(String, nullable=True, comment="Hex format MD5 hash of TCIA data at this level")
    revised = Column(Boolean, default=False, comment="True if object is revised relative to the previous IDC version in the corresponding source")

    collections = relationship('Collection',
                               secondary=version_collection,
                               back_populates='versions')

'''
---------------------------------------------------------------------------------------------------------------------
Many-to-many table between collection and patient
'''
collection_patient = Table('collection_patient', Base.metadata,
                           Column('collection_uuid', ForeignKey('collection.uuid'), primary_key=True),
                           Column('patient_uuid', ForeignKey('patient.uuid'), primary_key=True))

'''
---------------------------------------------------------------------------------------------------------------------
'''
class Collection(Base):

    __tablename__ = 'collection'
    collection_name = Column(String, nullable=False, comment='TCIA/NBIA collection ID')
    idc_collection_uuid = Column(String, nullable=False, comment="IDC assigned collection ID")
    uuid = Column(String, nullable=False, primary_key=True, comment="IDC assigned UUID of a version of this object")

    min_timestamp = Column(DateTime, nullable=True, comment="Time when building this object started")
    max_timestamp = Column(DateTime, nullable=True, comment="Time when building this object completed")
    init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    final_idc_version = Column(Integer, default=0, comment="Final IDC version of this version of this object")
    # revised = Column(Boolean, default=False, comment="If True, this object is revised relative to the previous IDC version")
    done = Column(Boolean, default=False, comment="Set to True if this object has been processed")
    is_new = Column(Boolean, default=False, comment="True if this object is new in this version")
    expanded = Column(Boolean, default=False, comment="True if the next lower level has been populated")
    hash = Column(String, nullable=True, comment="Hex format MD5 hash of TCIA data at this level")
    revised = Column(Boolean, default=False, comment="True if object is revised relative to the previous IDC version in the corresponding source")
    redacted = Column(Boolean, default=False, comment="True if object has been redacted")
    mitigation = Column(String, default="", comment="ID of the mitigation which redacted this object")

    versions = relationship('Version',
                               secondary=version_collection,
                               back_populates='collections')

    patients = relationship('Patient',
                               secondary=collection_patient,
                               back_populates='collections')

'''
---------------------------------------------------------------------------------------------------------------------
Many-to-many table between patient and study
'''
patient_study = Table('patient_study', Base.metadata,
                           Column('patient_uuid', ForeignKey('patient.uuid'), primary_key=True),
                           Column('study_uuid', ForeignKey('study.uuid'), primary_key=True))

'''
---------------------------------------------------------------------------------------------------------------------
'''
class Patient(Base):
    __tablename__ = 'patient'

    patientid = Column(String, nullable=False, comment="Submitter's patient ID")
    idc_case_id = Column(String, nullable=False, comment="IDC assigned patient ID")
    uuid = Column(String, nullable=False, primary_key=True, comment="IDC assigned UUID of a version of this object")

    min_timestamp = Column(DateTime, nullable=True, comment="Time when building this object started")
    max_timestamp = Column(DateTime, nullable=True, comment="Time when building this object completed")
    init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    final_idc_version = Column(Integer, default=0, comment="Final IDC version of this version of this object")
    # revised = Column(Boolean, default=False, comment="If True, this object is revised relative to the previous IDC version")
    done = Column(Boolean, default=False, comment="Set to True if this object has been processed")
    is_new = Column(Boolean, default=False, comment="True if this object is new in this version")
    expanded = Column(Boolean, default=False, comment="True if the next lower level has been populated")
    hash = Column(String, nullable=True, comment="Hex format MD5 hash of TCIA data at this level")
    revised = Column(Boolean, default=False, comment="True if object is revised relative to the previous IDC version in the corresponding source")
    redacted = Column(Boolean, default=False, comment="True if object has been redacted")
    mitigation = Column(String, default="", comment="ID of the mitigation which redacted this object")

    collections = relationship('Collection',
                               secondary=collection_patient,
                               back_populates='patients')
    studies = relationship('Study',
                            secondary=patient_study,
                            back_populates='patients')

'''
---------------------------------------------------------------------------------------------------------------------
Many-to-many table between study and series
'''
study_series = Table('study_series', Base.metadata,
                      Column('study_uuid', ForeignKey('study.uuid'), primary_key=True),
                      Column('series_uuid', ForeignKey('series.uuid'), primary_key=True))

'''
---------------------------------------------------------------------------------------------------------------------
'''
class Study(Base):
    __tablename__ = 'study'

    studyinstanceuid = Column(String, nullable=False, comment="DICOM StudyInstanceUID")
    uuid = Column(String, nullable=False, unique=True, primary_key=True, comment="IDC assigned UUID of a version of this object")
    study_instances = Column(Integer, nullable=True, comment="Instances in this study")

    min_timestamp = Column(DateTime, nullable=True, comment="Time when building this object started")
    max_timestamp = Column(DateTime, nullable=True, comment="Time when building this object completed")
    init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    final_idc_version = Column(Integer, default=0, comment="Final IDC version of this version of this object")
    # revised = Column(Boolean, default=False, comment="If True, this object is revised relative to the previous IDC version")
    done = Column(Boolean, default=False, comment="Set to True if this object has been processed")
    is_new = Column(Boolean, default=False, comment="True if this object is new in this version")
    expanded = Column(Boolean, default=False, comment="True if the next lower level has been populated")
    hash = Column(String, nullable=True, comment="Hex format MD5 hash of TCIA data at this level")
    revised = Column(Boolean, default=False, comment="True if object is revised relative to the previous IDC version in the corresponding source")
    redacted = Column(Boolean, default=False, comment="True if object has been redacted")
    mitigation = Column(String, default="", comment="ID of the mitigation which redacted this object")

    patients = relationship('Patient',
                            secondary=patient_study,
                            back_populates='studies')
    seriess = relationship('Series',
                           secondary=study_series,
                           back_populates='studies')

'''
---------------------------------------------------------------------------------------------------------------------
Many-to-many table between series and instance
'''
series_instance = Table('series_instance', Base.metadata,
                     Column('series_uuid', ForeignKey('series.uuid'), primary_key=True),
                     Column('instance_uuid', ForeignKey('instance.uuid'), primary_key=True))

'''
---------------------------------------------------------------------------------------------------------------------
'''
class Series(Base):
    __tablename__ = 'series'

    seriesnstanceuid = Column(String, nullable=False, comment="DICOM SeriesInstanceUID")
    uuid = Column(String, primary_key=True, comment="IDC assigned UUID of a version of this object")
    series_instances = Column(Integer, nullable=True, comment="Instances in this series")
    source_doi = Column(String, nullable=True, comment="A DOI to the wiki page of this series")

    min_timestamp = Column(DateTime, nullable=True, comment="Time when building this object started")
    max_timestamp = Column(DateTime, nullable=True, comment="Time when building this object completed")
    init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    final_idc_version = Column(Integer, default=0, comment="Final IDC version of this version of this object")
    # revised = Column(Boolean, default=False, comment="If True, this object is revised relative to the previous IDC version")
    done = Column(Boolean, default=False, comment="Set to True if this object has been processed")
    is_new = Column(Boolean, default=False, comment="True if this object is new in this version")
    expanded = Column(Boolean, default=False, comment="True if the next lower level has been populated")
    hash = Column(String, nullable=True, comment="Hex format MD5 hash of TCIA data at this level")
    revised = Column(Boolean, default=False, comment="True if object is revised relative to the previous IDC version in the corresponding source")
    source_url = Column(String, nullable=True, comment="A url to the wiki page of this series")
    excluded = Column(Boolean, default=False, comment="True if object should be excluded from auxiliary_metadata, etc.")
    redacted = Column(Boolean, default=False, comment="True if object has been redacted")
    versioned_source_doi = Column(String, comment='If present, a DOI to the wiki page of this version of this series')
    mitigation = Column(String, default="", comment="ID of the mitigation which redacted this object")
    collection_type = Column(String, comment="'original_collection' or 'analysis_result'")

    studies = relationship('Study',
                           secondary=study_series,
                           back_populates='seriess')
    instances = relationship('Instance',
                          secondary=series_instance,
                          back_populates='seriess')

'''
---------------------------------------------------------------------------------------------------------------------
'''
class Instance(Base):
    __tablename__ = 'instance'

    sopinstanceuid = Column(String, nullable=False, comment='DICOM SOPInstanceUID')
    uuid = Column(String, primary_key=True, comment="IDC assigned UUID of a version of this object")
    hash = Column(String, nullable=True, comment="Hex format MD5 hash of TCIA data at this level")
    size = Column(BigInteger, nullable=True, comment='Instance blob size (bytes)')

    revised = Column(Boolean, default=False, comment="If True, this object is revised relative to the previous IDC version")
    done = Column(Boolean, default=False, comment="Set to True if this object has been processed")
    is_new = Column(Boolean, default=False, comment="True if this object is new in this version")
    expanded = Column(Boolean, default=False, comment="True if the next lower level has been populated")
    init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    final_idc_version = Column(Integer, default=0, comment="Final IDC version of this version of this object")
    # source = Column(Enum(instance_source), nullable=True, comment='Source of this object; "tcia", "idc"')
    timestamp = Column(DateTime, nullable=True, comment="Time when this object was last built")
    # Excluded instances are somehow invalid, but are included in the DB to maintain the hash
    excluded = Column(Boolean, default=False, comment="True if object should be excluded from auxiliary_metadata, etc.")
    redacted = Column(Boolean, default=False, comment="True if object has been redacted")
    mitigation = Column(String, default="", comment="ID of the mitigation which redacted this instance")
    ingestion_url = Column(String, default="", comment="GCS URL of the blob from which this instance was ingested. Does not apply if source='tcia'")
    source_file_hash = Column(String, default="", comment="md5 hash of the source file from which this instance was derived")

    seriess = relationship('Series',
                          secondary=series_instance,
                          back_populates='instances')

'''
---------------------------------------------------------------------------------------------------------------------
Table that includes all collections. A source is either an original_data_source or an analysis_result_source
This is a snapshot of what should be the current/next IDC version
'''
class Pre_Collection(Base):
    __tablename__ = 'pre_collection'

    collection_name = Column(String, primary_key=True, comment='collection name')
    collection_id = Column(String, unique=True, comment='collection ID')
    hash = Column(String, comment='Source hash')

    patients = relationship("Pre_Patient", back_populates="collection", order_by="Pre_Patient.patientid", cascade="all, delete")

'''
---------------------------------------------------------------------------------------------------------------------
Table of pre-ingestion patient metadata.
This is a snapshot of what should be in the current/next version
'''
class Pre_Patient(Base):
    __tablename__ = 'pre_patient'

    patientid = Column(String, primary_key=True, comment="Patient ID")
    collection_name = Column(String, primary_key=True, comment="Containing object")
    hash = Column(String, comment='Patient hash')

    # Composite Foreign Key Constraint pointing to pre_collection
    __table_args__ = (
        ForeignKeyConstraint(
            ['collection_name'],
            ['pre_collection.collection_name'],
            ondelete='CASCADE'
        ),
    )

    # Relationships
    collection = relationship("Pre_Collection", back_populates="patients")
    studies = relationship(
        "Pre_Study",
        back_populates="patient",
        order_by="Pre_Study.studyinstanceuid",
        cascade="all, delete"
    )

'''
---------------------------------------------------------------------------------------------------------------------
Table of pre-ingestion study metadata.
This is a snapshot of what should be the current/next version
'''
class Pre_Study(Base):
    __tablename__ = 'pre_study'

    studyinstanceuid = Column(String, primary_key=True, nullable=False)
    collection_name = Column(String,  nullable=False, comment="Containing object")
    patientid = Column(String, nullable=False, comment="Containing object")
    hash = Column(String, nullable=False, comment='Study hash')

    # Composite Foreign Key Constraint pointing to pre_patient's composite primary key
    __table_args__ = (
        ForeignKeyConstraint(
            ['collection_name', 'patientid'],
            ['pre_patient.collection_name', 'pre_patient.patientid'],
            ondelete='CASCADE'
        ),
    )

    # Relationships
    patient = relationship("Pre_Patient", back_populates="studies")
    seriess = relationship(
        "Pre_Series",
        back_populates="study",
        order_by="Pre_Series.seriesinstanceuid",
        cascade="all, delete"
    )
    # # collection = relationship("Pre_Patient", back_populates="collections")
    # patient = relationship("Pre_Patient", back_populates="studies")
    # seriess = relationship("Pre_Series", back_populates="study", order_by="Pre_Series.seriesinstanceuid", cascade="all, delete")

'''
---------------------------------------------------------------------------------------------------------------------
Table that pre-ingestion series metadata.
This is a snapshot of what should be the current/next version
'''
class Pre_Series(Base):
    __tablename__ = 'pre_series'
    seriesinstanceuid = Column(String, unique=True, primary_key=True, nullable=False)
    studyinstanceuid = Column(ForeignKey('pre_study.studyinstanceuid'), comment="Containing object")
    hash = Column(String, comment='Series hash')
    excluded = Column(Boolean, default=False, comment='True if this series should be excluded from ingestion')
    source_doi = Column(String, comment='Source DOI of this series\' wiki')
    source_url = Column(String, comment='Source URL of this series\' wiki')
    versioned_source_doi = Column(String, comment='Versioned source DOI of this series\' wiki')
    versioned_source_url = Column(String, comment='Versioned source URL of this series\' wiki')
    analysis_result = Column(Boolean, comment='True if this series is from an analysis result, else False')

    study = relationship("Pre_Study", back_populates="seriess")
    instances = relationship("Pre_Instance", back_populates="seriess", order_by="Pre_Instance.sopinstanceuid", cascade="all, delete")

'''
---------------------------------------------------------------------------------------------------------------------
Table that pre-ingestion instance metadata.
This is a snapshot of what should be the current/next version
'''
class Pre_Instance(Base):
    __tablename__ = 'pre_instance'
    sopinstanceuid = Column(String, primary_key=True, nullable=False)
    seriesinstanceuid = Column(ForeignKey('pre_series.seriesinstanceuid'), comment="Containing object")
    hash = Column(String, comment='Instance hash')
    ingestion_url = Column(String, comment='GCS URL of instance source')
    size = Column(BigInteger, comment='Instance size in bytes')
    excluded = Column(Boolean, default=False, comment='True if this instance should be excluded from ingestion')
    idc_version = Column(Integer, comment='IDC version when this instance was added/revised')
    mitigation = Column(String, default="", comment="ID of the mitigation which redacted this instance")
    source_file_hash = Column(String, default="", comment="md5 hash of the source file from which this instance was derived")

    seriess = relationship("Pre_Series", back_populates="instances")


class All_Collections(Base):
    __tablename__ = 'all_collections'
    collection_name = Column(String, primary_key=True, comment='Collection name')
    collection_id = Column(String, comment='Collection id')
    source_doi = Column(String)
    source_url = Column(String)
    source = Column(String)
    type = Column(String)
    access = Column(String)
    metadata_sunset = Column(Integer)
    dev_bucket = Column(String)
    pub_gcs_bucket = Column(String)
    pub_aws_bucket = Column(String)



# collection_id_map maps an idc_collection_id to one or more tcia_api_collection_ids.
# This mapping is meant to deal with the possibility that TCIA might rename a collection.
# In that case, the IDC generated idc_collection_id binds those tcia_api_collection_ids.
class Collection_id_map(Base):
    __tablename__ = 'collection_id_map'
    # tcia_api_collection_id = Column(String, primary_key=True, \
    #                 comment="Collection ID used by TCIA")
    idc_collection_uuid = Column(String, primary_key=True,
                   comment="IDC assigned collection ID (UUID4)")
    collection_id = Column(String, primary_key=True, \
                  comment="Collection ID used by IDC webapp")
    collection_name = Column(String, primary_key=True, \
                   comment="Collection ID used for ETL")

# This table is populated with metadata for collections that are not sourced from TCIA.
class Original_Collections_Metadata_IDC_Source(Base):
    __tablename__ = 'original_collections_metadata_idc_source'
    # tcia_api_collection_id = Column(String, comment='Collection ID used by TCIA APIs')
    # tcia_wiki_collection_id = Column(String, nullable=True, comment='TCIA Wiki page collection ID')
    collection_name = Column(String, comment='Public collection name')
    collection_id = Column(String,primary_key=True, nullable=False, comment='Collection ID used by IDC webapp')
    Status = Column(String, nullable=False, comment='Public or Limited')
    Updated = Column(String, comment='Date of last update')
    ImageTypes = Column(String, comment='List of image types')
    DOI = Column(String,comment='DOI of collection description page')
    URL = Column(String, comment='URL of collection description page')
    CancerType = Column(String, comment='Cancer type')
    SupportingData = Column(String, comment='Supporting data')
    Species = Column(String, comment='Species studies')
    Location = Column(String, comment='Cancer location ')
    license_url = Column(String, comment='URL of license description')
    license_long_name = Column(String, comment='Long name of license')
    license_short_name = Column(String, comment='Short name of license')
    Description = Column(String, comment='Description of collection')
    # version = Column(String, comment='Version of IDC-sourced subcollection as <original_idc_version>.<revised_idc_version>')

# This table is populated with metadata for collections that are not sourced from TCIA.
class Analysis_Results_Metadata_IDC_Source(Base):
    __tablename__ = 'analysis_results_metadata_idc_source'
    ID = Column(String, primary_key=True, comment='Results ID')
    Title = Column(String, comment='Descriptive title')
    Access = Column(String, comment='Limited or Public')
    DOI = Column(String,comment='DOI of collection description page')
    CancerType = Column(String, comment='Types of cancer analyzed')
    Location = Column(String, comment='Body location that was analyzed')
    Subjects = Column(String, comment='Number of subjects whose data was analyzed')
    Collections = Column(String, comment='idc_webapp_collection_ids of original data collections analyzed')
    AnalysisArtifacts = Column(String, comment='Types of analysis artifacts produced')
    Updated = Column(String, comment='Date of most recent update reported')
    license_url = Column(String, comment='URL of license description')
    license_long_name = Column(String, comment='Long name of license')
    license_short_name = Column(String, comment='Short name of license')
    URL = Column(String,comment='URL of collection description page')
    version = Column(String, comment='Version of analysis result as <original_idc_version>.<revised_idc_version>')

# This table is populated with a description of each analysis result.
class Analysis_Results_Descriptions(Base):
    __tablename__ = 'analysis_results_descriptions'
    id = Column(String, primary_key=True, comment='Analysis result id')
    description = Column(String, comment='Analysis result description')

# # This table is populated with IDC assigned UUID of each analysis result.
# class Analysis_Id_Map(Base):
#     __tablename__ = 'analysis_id_map'
#     collection_id = Column(String, comment='Analysis result ID')
#     idc_id = Column(String, primary_key=True, comment='IDC assigned UUID')

# This table gives the program to which collection, as identified by its
# tcia_wiki_collection_id, belongs.
class Program(Base):
    __tablename__ = 'program'
    tcia_wiki_collection_id = Column(String, primary_key=True, comment='Results ID')
    program = Column(String, comment='Descriptive title')

class DOI_To_Access(Base):
    __tablename__ = 'doi_to_access'
    source_doi = Column(String, primary_key=True, comment='Source DOI of collections or analysis result')
    type = Column(String, comment='One of "Cr", "Defaced", "Excluded", "Redacted"')
    access = Column(String, comment='One of "Public", "Excluded", "Limited" ')
    collection_id = Column(String, comment='Collection ID used by IDC webapp')

