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
    tcia = 0
    idc = 1
    all_sources = 2

Base = declarative_base()
# sql_engine = create_engine(sql_uri, echo=True)
# sql_engine = create_engine(sql_uri)

# These tables define the ETL database. There is a separate DB for each IDC version.
# Note that earlier IDC versions used a one-to-many schema.

# Flattened hierarchy. The underlying PSQL is a view.
class All_Joined(Base):
    __tablename__ = 'all_joined'
    idc_version = Column(Integer, nullable=False, comment="Target version of revision")
    previous_idc_version = Column(Integer, nullable=False, comment="ID of the previous version")
    v_hashes = Column(
        CompositeType(
            'hashes',
            [
                Column('tcia', String, default="", comment="Hash of tcia data"),
                Column('idc', String, default="", comment="Hash of idc data"),
                Column('all_sources', String, default="", comment="Hash of all data")
            ]
        ),
        comment="Source specific hierarchical hash"
    )
    v_sources = Column(
        CompositeType(
            'sources',
            [
                Column('tcia', Boolean, default=False),
                Column('idc', Boolean, default=False)
            ]
        ),
        nullable=True,
        comment="True if this objects includes instances from the corresponding source"
    )

    collection_id = Column(String, nullable=False, comment='TCIA/NBIA collection ID')
    idc_collection_id = Column(String, nullable=False, comment="IDC assigned collection ID")
    c_uuid = Column(String, nullable=False, comment="IDC assigned UUID of a version of this object")
    c_hashes = Column(
        CompositeType(
            'hashes',
            [
                Column('tcia', String, default="", comment="Hash of tcia data"),
                Column('idc', String, default="", comment="Hash of tcia idc data"),
                Column('all_sources', String, default="", comment="Hash of all data")
            ]
        ),
        comment="Source specific hierarchical hash"
    )
    c_sources = Column(
        CompositeType(
            'sources',
            [
                Column('tcia', Boolean, default=False),
                Column('idc', Boolean, default=False)
            ]
        ),
        nullable=True,
        comment="True if this objects includes instances from the corresponding source"
    )
    c_init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    c_rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    c_final_idc_version = Column(Integer, default=0, comment="Final IDC version of this version of this object")

    submitter_case_id = Column(String, nullable=False, comment="Submitter's patient ID")
    idc_case_id = Column(String, nullable=False, comment="IDC assigned patient ID")
    p_uuid = Column(String, nullable=False, comment="IDC assigned UUID of a version of this object")
    p_hashes = Column(
        CompositeType(
            'hashes',
            [
                Column('tcia', String, default="", comment="Hash of tcia  data"),
                Column('idc', String, default="", comment="Hash of idc data"),
                Column('all_sources', String, default="", comment="Hash of all data")
            ]
        ),
        comment="Source specific hierarchical hash"
    )
    p_sources = Column(
        CompositeType(
            'sources',
            [
                Column('tcia', Boolean, default=False),
                Column('idc', Boolean, default=False)
            ]
        ),
        nullable=True,
        comment="True if this objects includes instances from the corresponding source"
    )
    p_init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    p_rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    p_final_idc_version = Column(Integer, default=0, comment="Final IDC version of this version of this object")

    study_instance_uid = Column(String, nullable=False, comment="DICOM StudyInstanceUID")
    st_uuid = Column(String, nullable=False, comment="IDC assigned UUID of a version of this object")
    study_instances = Column(Integer, nullable=True, comment="Instances in this study")
    st_hashes = Column(
        CompositeType(
            'hashes',
            [
                Column('tcia', String, default="", comment="Hash of tcia data"),
                Column('idc', String, default="", comment="Hash of idc data"),
                Column('all_sources', String, default="", comment="Hash of all data")
            ]
        ),
        comment="Source specific hierarchical hash"
    )
    st_sources = Column(
        CompositeType(
            'sources',
            [
                Column('tcia', Boolean, default=False),
                Column('idc', Boolean, default=False)
            ]
        ),
        nullable=True,
        comment="True if this objects includes instances from the corresponding source"
    )
    st_init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    st_rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    st_final_idc_version = Column(Integer, default=0, comment="Final IDC version of this version of this object")

    series_instance_uid = Column(String, nullable=False, comment="DICOM SeriesInstanceUID")
    se_uuid = Column(String, nullable=False, comment="IDC assigned UUID of a version of this object")
    series_instances = Column(Integer, nullable=True, comment="Instances in this series")
    source_doi = Column(String, nullable=True, comment="A doi to the wiki page of this source of this series")
    source_url = Column(String, nullable=True, comment="A url to the wiki page of this source of this series")
    se_hashes = Column(
        CompositeType(
            'hashes',
            [
                Column('tcia', String, default="", comment="Hash of tcia  data"),
                Column('idc', String, default="", comment="Hash of idc data"),
                Column('all_sources', String, default="", comment="Hash of all data")
            ]
        ),
        comment="Source specific hierarchical hash"
    )
    se_sources = Column(
        CompositeType(
            'sources',
            [
                Column('tcia', Boolean, default=False),
                Column('idc', Boolean, default=False)
            ]
        ),
        nullable=True,
        comment="True if this objects includes instances from the corresponding source"
    )
    se_init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    se_rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    se_final_idc_version = Column(Integer, default=0, comment="Final IDC version of this version of this object")

    sop_instance_uid = Column(String, nullable=False, unique=False, comment='DICOM SOPInstanceUID')
    i_uuid = Column(String, primary_key=True, comment="IDC assigned UUID of a version of this object")
    i_hash = Column(String, nullable=True, comment="Hierarchical hex format MD5 hash of TCIA data at this level")
    i_size = Column(BigInteger, nullable=True, comment='Instance blob size (bytes)')
    i_excluded = Column(Boolean, default=False, comment="True if instance should be excluded from auxiliary_metacata, etc.")
    i_init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    i_rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    i_final_idc_version = Column(Integer, default=0, comment="Final IDC version of this version of this object")


version_collection = Table('version_collection', Base.metadata,
                           Column('version', ForeignKey('version.version'), primary_key=True),
                           Column('collection_uuid', ForeignKey('collection.uuid'), primary_key=True))

class Version(Base):
    __tablename__ = 'version'
    version = Column(Integer, primary_key=True, comment="Target version of revision")
    previous_version = Column(Integer, nullable=False, comment="ID of the previous version")
    hashes = Column(
        CompositeType(
            'hashes',
            [
                Column('tcia', String, default="", comment="Hash of tcia data"),
                Column('idc', String, default="", comment="Hash of idc data"),
                Column('all_sources', String, default="", comment="Hash of all data")
            ]
        ),
        comment="Source specific hierarchical hash"
    )
    sources = Column(
        CompositeType(
            'sources',
            [
                Column('tcia', Boolean, default=False),
                Column('idc', Boolean, default=False)
            ]
        ),
        nullable=True,
        comment="True if this objects includes instances from the corresponding source"
    )

    min_timestamp = Column(DateTime, nullable=True, comment="Time when building this object started")
    max_timestamp = Column(DateTime, nullable=True, comment="Time when building this object completed")
    # revised = Column(Boolean, default=False, comment="If True, this object is revised relative to the previous IDC version")
    done = Column(Boolean, default=False, comment="Set to True if this object has been processed")
    is_new = Column(Boolean, default=False, comment="True if this object is new in this version")
    expanded = Column(Boolean, default=False, comment="True if the next lower level has been populated")
    hashes = Column(
        CompositeType(
            'hashes',
            [
                Column('tcia', String, default="", comment="Hash of tcia data"),
                Column('idc', String, default="", comment="Hash of idc data"),
                Column('all_sources', String, default="", comment="Hash of all data")
            ]
        ),
        comment="Source specific hierarchical hash"
    )
    sources = Column(
        CompositeType(
            'sources',
            [
                Column('tcia', Boolean, default=False),
                Column('idc', Boolean, default=False)
            ]
        ),
        nullable=True,
        comment="True if this objects includes instances from the corresponding source"
    )
    revised = Column(
        CompositeType(
            'sources',
            [
                Column('tcia', Boolean, default=False, comment="True his object is revised relative to the previous IDC version in the corresponding source"),
                Column('idc', Boolean, default=False, comment="True his object is revised relative to the previous IDC version in the corresponding source")
            ]
        ),
        nullable=True,
    )

    collections = relationship('Collection',
                               secondary=version_collection,
                               back_populates='versions')

collection_patient = Table('collection_patient', Base.metadata,
                           Column('collection_uuid', ForeignKey('collection.uuid'), primary_key=True),
                           Column('patient_uuid', ForeignKey('patient.uuid'), primary_key=True))

class Collection(Base):
    __tablename__ = 'collection'
    collection_id = Column(String, nullable=False, comment='TCIA/NBIA collection ID')
    idc_collection_id = Column(String, nullable=False, comment="IDC assigned collection ID")
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
    sources = Column(
        CompositeType(
            'sources',
            [
                Column('tcia', Boolean, default=False),
                Column('idc', Boolean, default=False)
            ]
        ),
        nullable=True,
        comment="True if this objects includes instances from the corresponding source"
    )
    hashes = Column(
        CompositeType(
            'hashes',
            [
                Column('tcia', String, default="", comment="Hash of tcia data"),
                Column('idc', String, default="", comment="Hash of idc data"),
                Column('all_sources', String, default="", comment="Hash of all data")
            ]
        ),
        nullable=True,
        comment="Source specific hierarchical hash"
    )
    revised = Column(
        CompositeType(
            'sources',
            [
                Column('tcia', Boolean, default=False, comment="True his object is revised relative to the previous IDC version in the corresponding source"),
                Column('idc', Boolean, default=False, comment="True his object is revised relative to the previous IDC version in the corresponding source")
            ]
        ),
        nullable=True,
    )
    versions = relationship('Version',
                               secondary=version_collection,
                               back_populates='collections')

    patients = relationship('Patient',
                               secondary=collection_patient,
                               back_populates='collections')


patient_study = Table('patient_study', Base.metadata,
                           Column('patient_uuid', ForeignKey('patient.uuid'), primary_key=True),
                           Column('study_uuid', ForeignKey('study.uuid'), primary_key=True))

class Patient(Base):
    __tablename__ = 'patient'
    submitter_case_id = Column(String, nullable=False, comment="Submitter's patient ID")
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
    sources = Column(
        CompositeType(
            'sources',
            [
                Column('tcia', Boolean, default=False),
                Column('idc', Boolean, default=False)
            ]
        ),
        nullable=True,
        comment="True if this objects includes instances from the corresponding source"
    )
    hashes = Column(
        CompositeType(
            'hashes',
            [
                Column('tcia', String, default="", comment="Hash of tcia data"),
                Column('idc', String, default="", comment="Hash of tcia data"),
                Column('all_sources', String, default="", comment="Hash of all data")
            ]
        ),
        nullable=True,
        comment="Source specific hierarchical hash"
    )
    revised = Column(
        CompositeType(
            'sources',
            [
                Column('tcia', Boolean, default=False, comment="True his object is revised relative to the previous IDC version in the corresponding source"),
                Column('idc', Boolean, default=False, comment="True his object is revised relative to the previous IDC version in the corresponding source")
            ]
        ),
        nullable=True,
    )
    collections = relationship('Collection',
                               secondary=collection_patient,
                               back_populates='patients')
    studies = relationship('Study',
                            secondary=patient_study,
                            back_populates='patients')


study_series = Table('study_series', Base.metadata,
                      Column('study_uuid', ForeignKey('study.uuid'), primary_key=True),
                      Column('series_uuid', ForeignKey('series.uuid'), primary_key=True))


class Study(Base):
    __tablename__ = 'study'
    study_instance_uid = Column(String, nullable=False, comment="DICOM StudyInstanceUID")
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
    sources = Column(
        CompositeType(
            'sources',
            [
                Column('tcia', Boolean, default=False),
                Column('idc', Boolean, default=False)
            ]
        ),
        nullable=True,
        comment="True if this objects includes instances from the corresponding source"
    )
    hashes = Column(
        CompositeType(
            'hashes',
            [
                Column('tcia', String, default="", comment="Hash of tcia data"),
                Column('idc', String, default="", comment="Hash of idc data"),
                Column('all_sources', String, default="", comment="Hash of all data")
            ]
        ),
        nullable=True,
        comment="Source specific hierarchical hash"
    )
    revised = Column(
        CompositeType(
            'sources',
            [
                Column('tcia', Boolean, default=False, comment="True his object is revised relative to the previous IDC version in the corresponding source"),
                Column('idc', Boolean, default=False, comment="True his object is revised relative to the previous IDC version in the corresponding source")
            ]
        ),
        nullable=True,
    )
    patients = relationship('Patient',
                            secondary=patient_study,
                            back_populates='studies')
    seriess = relationship('Series',
                           secondary=study_series,
                           back_populates='studies')


series_instance = Table('series_instance', Base.metadata,
                     Column('series_uuid', ForeignKey('series.uuid'), primary_key=True),
                     Column('instance_uuid', ForeignKey('instance.uuid'), primary_key=True))

class Series(Base):
    __tablename__ = 'series'
    series_instance_uid = Column(String, nullable=False, comment="DICOM SeriesInstanceUID")
    uuid = Column(String, primary_key=True, comment="IDC assigned UUID of a version of this object")
    series_instances = Column(Integer, nullable=True, comment="Instances in this series")
    source_doi = Column(String, nullable=True, comment="A doi to the wiki page of this series")

    min_timestamp = Column(DateTime, nullable=True, comment="Time when building this object started")
    max_timestamp = Column(DateTime, nullable=True, comment="Time when building this object completed")
    init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    final_idc_version = Column(Integer, default=0, comment="Final IDC version of this version of this object")
    # revised = Column(Boolean, default=False, comment="If True, this object is revised relative to the previous IDC version")
    done = Column(Boolean, default=False, comment="Set to True if this object has been processed")
    is_new = Column(Boolean, default=False, comment="True if this object is new in this version")
    expanded = Column(Boolean, default=False, comment="True if the next lower level has been populated")
    sources = Column(
        CompositeType(
            'sources',
            [
                Column('tcia', Boolean, default=False),
                Column('idc', Boolean, default=False)
            ]
        ),
        nullable=True,
        comment="True if this objects includes instances from the corresponding source"
    )
    hashes = Column(
        CompositeType(
            'hashes',
            [
                Column('tcia', String, default="", comment="Hash of tcia data"),
                Column('idc', String, default="", comment="Hash of idc data"),
                Column('all_sources', String, default="", comment="Hash of all data")
            ]
        ),
        nullable=True,
        comment="Source specific hierarchical hash"
    )
    revised = Column(
        CompositeType(
            'sources',
            [
                Column('tcia', Boolean, default=False, comment="True his object is revised relative to the previous IDC version in the corresponding source"),
                Column('idc', Boolean, default=False, comment="True his object is revised relative to the previous IDC version in the corresponding source")
            ]
        ),
        nullable=True,
    )
    source_url = Column(String, nullable=True, comment="A url to the wiki page of this series")
    excluded = Column(Boolean, default=False, comment="True if object should be excluded from auxiliary_metadata, etc.")
    license_long_name = Column(String, comment="Long name of license.")
    license_url = Column(String, comment="License URL of this series.")
    license_short_name = Column(String, comment='Short name of license')

    studies = relationship('Study',
                           secondary=study_series,
                           back_populates='seriess')
    instances = relationship('Instance',
                          secondary=series_instance,
                          back_populates='seriess')

class Instance(Base):
    __tablename__ = 'instance'
    sop_instance_uid = Column(String, nullable=False, comment='DICOM SOPInstanceUID')
    uuid = Column(String, primary_key=True, comment="IDC assigned UUID of a version of this object")
    hash = Column(String, nullable=True, comment="Hierarchical hex format MD5 hash of TCIA data at this level")
    size = Column(BigInteger, nullable=True, comment='Instance blob size (bytes)')

    revised = Column(Boolean, default=False, comment="If True, this object is revised relative to the previous IDC version")
    done = Column(Boolean, default=False, comment="Set to True if this object has been processed")
    is_new = Column(Boolean, default=False, comment="True if this object is new in this version")
    expanded = Column(Boolean, default=False, comment="True if the next lower level has been populated")
    init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    final_idc_version = Column(Integer, default=0, comment="Final IDC version of this version of this object")
    source = Column(Enum(instance_source), nullable=True, comment='Source of this object; "tcia", "idc"')
    timestamp = Column(DateTime, nullable=True, comment="Time when this object was last built")
    # Excluded instances are somehow invalid, but are included in the DB to maintain the hash
    excluded = Column(Boolean, default=False, comment="True if object should be excluded from auxiliary_metadata, etc.")

    seriess = relationship('Series',
                          secondary=series_instance,
                          back_populates='instances')

# collection_id_map maps an idc_collection_id to one or more tcia_api_collection_ids.
# This mapping is meant to deal with the possibility that TCIA might rename a collection.
# In that case, the IDC generated idc_collection_id binds those tcia_api_collection_ids.
class Collection_id_map(Base):
    __tablename__ = 'collection_id_map'
    tcia_api_collection_id = Column(String, primary_key=True, \
                    comment="Collection ID used by TCIA")
    idc_collection_id = Column(String, primary_key=True,
                   comment="IDC assigned collection ID (UUID4)")
    idc_webapp_collection_id = Column(String, primary_key=True, \
                  comment="Collection ID used by IDC webapp")
    collection_id = Column(String, primary_key=True, \
                   comment="Collection ID used for ETL")

# Table that includes all IDC sourced collections.
# This is a snapshot of what should be the current/next IDC version
class IDC_Collection(Base):
    __tablename__ = 'idc_collection'
    collection_id = Column(String, unique=True, primary_key=True, comment='NBIA collection ID')
    hash = Column(String, comment='Collection hash')

    # vers = relationship("IDC_Version", back_populates="collections")
    patients = relationship("IDC_Patient", back_populates="collection", order_by="IDC_Patient.submitter_case_id", cascade="all, delete")


# Table that includes all IDC sourced patients.
# This is a snapshot of what should be the current/next IDC version
class IDC_Patient(Base):
    __tablename__ = 'idc_patient'
    submitter_case_id = Column(String, nullable=False, unique=True, primary_key=True, comment="Submitter's patient ID")
    collection_id = Column(ForeignKey('idc_collection.collection_id'), comment="Containing object")
    hash = Column(String, comment='Patient hash')

    collection = relationship("IDC_Collection", back_populates="patients")
    studies = relationship("IDC_Study", back_populates="patient", order_by="IDC_Study.study_instance_uid", cascade="all, delete")


# Table that includes all IDC sourced studies.
# This is a snapshot of what should be the current/next IDC version
class IDC_Study(Base):
    __tablename__ = 'idc_study'
    study_instance_uid = Column(String, unique=True, primary_key=True, nullable=False)
    submitter_case_id = Column(ForeignKey('idc_patient.submitter_case_id'), comment="Submitter's patient ID")
    hash = Column(String, comment='Study hash')

    patient = relationship("IDC_Patient", back_populates="studies")
    seriess = relationship("IDC_Series", back_populates="study", order_by="IDC_Series.series_instance_uid", cascade="all, delete")


# Table that includes all IDC sourced series.
# This is a snapshot of what should be the current/next IDC version
class IDC_Series(Base):
    __tablename__ = 'idc_series'
    series_instance_uid = Column(String, unique=True, primary_key=True, nullable=False)
    study_instance_uid = Column(ForeignKey('idc_study.study_instance_uid'), comment="Containing object")
    hash = Column(String, comment='Series hash')
    excluded = Column(Boolean, comment='True of this series should be excluded from ingestion')
    wiki_doi = Column(String, comment='Source DOI of this series\' wiki')
    wiki_url = Column(String, comment='Source URL of this series\' wiki')
    third_party = Column(Boolean, default=False, comment='True if from a third party analysis result')
    license_url = Column(String, comment='URL of license description')
    license_long_name = Column(String, comment='Long name of license')
    license_short_name = Column(String, comment='short name of license')

    study = relationship("IDC_Study", back_populates="seriess")
    instances = relationship("IDC_Instance", back_populates="seriess", order_by="IDC_Instance.sop_instance_uid", cascade="all, delete")


# Table that includes all IDC sourced instances.
# This is a snapshot of what should be the current/next IDC version
class IDC_Instance(Base):
    __tablename__ = 'idc_instance'
    sop_instance_uid = Column(String, primary_key=True, nullable=False)
    series_instance_uid = Column(ForeignKey('idc_series.series_instance_uid'), comment="Containing object")
    hash = Column(String, comment='Instance hash')
    gcs_url = Column(String, comment='GCS URL of instance')
    size = Column(BigInteger, comment='Instance size in bytes')
    idc_version = Column(Integer, comment='IDC version when this instance was added/revised')

    seriess = relationship("IDC_Series", back_populates="instances")

# class All_IDC_Joined(Base):
#     __tablename__ = "all_idc_joined"
#     version = Column(Integer, unique=True, primary_key=True, comment='NBIA collection ID')
#     v_hash = Column(String, comment='Version hash')
#     collection_id = Column(String, unique=True, primary_key=True, comment='NBIA collection ID')
#     c_hash = Column(String, comment='Collection hash')
#     submitter_case_id = Column(String, nullable=False, unique=True, primary_key=True, comment="Submitter's patient ID")
#     p_hash = Column(String, comment='Patient hash')
#     study_instance_uid = Column(String, unique=True, primary_key=True, nullable=False)
#     st_hash = Column(String, comment='Study hash')
#     series_instance_uid = Column(String, unique=True, primary_key=True, nullable=False)
#     se_hash = Column(String, comment='Series hash')
#     sop_instance_uid = Column(String, primary_key=True, nullable=False)
#     i_hash = Column(String, comment='Instance hash')
#     size = Column(Integer, comment='Instance size in bytes  ')
#     url = Column(String, comment='GCS URL of instance')

# A table of all collections having commercially restricted licenses
class CR_Collections(Base):
    __tablename__ = 'cr_collections'
    tcia_api_collection_id = Column(String, primary_key=True, comment='Collection ID')
    idc_collection_id = Column(String, comment="idc_collection_id of this collection")
    dev_tcia_url = Column(String, comment="Dev tcia bucket name")
    dev_idc_url = Column(String, comment="Dev idc bucket name")
    gcs_pub_tcia_url = Column(String, comment="Public gcs tcia bucket name")
    gcs_pub_idc_url = Column(String, comment="Public gcs idc bucket name")
    aws_pub_tcia_url = Column(String, comment="Public aws tcia bucket name")
    aws_pub_idc_url = Column(String, comment="Public aws idc bucket name")
    tcia_access = Column(String, comment="'Public', 'Limited', or 'Excluded'")
    idc_access = Column(String, comment="'Public', 'Limited', or 'Excluded'")

# A table of collections having radiology which might contain faces that will need masking
class Defaced_Collections(Base):
    __tablename__ = 'defaced_collections'
    tcia_api_collection_id = Column(String, primary_key=True, comment='Collection ID')
    idc_collection_id = Column(String, comment="idc_collection_id of this collection")
    dev_tcia_url = Column(String, comment="Dev tcia bucket name")
    dev_idc_url = Column(String, comment="Dev idc bucket name")
    gcs_pub_tcia_url = Column(String, comment="Public gcs tcia bucket name")
    gcs_pub_idc_url = Column(String, comment="Public gcs idc bucket name")
    aws_pub_tcia_url = Column(String, comment="Public aws tcia bucket name")
    aws_pub_idc_url = Column(String, comment="Public aws idc bucket name")
    tcia_access = Column(String, comment="'Public', 'Limited', or 'Excluded'")
    idc_access = Column(String, comment="'Public', 'Limited', or 'Excluded'")

# A table of collections that have been downloaded and included in the DB but are
# not public due to being judged of poor quality
class Excluded_Collections(Base):
    __tablename__ = 'excluded_collections'
    tcia_api_collection_id = Column(String, primary_key=True, comment='Collection ID')
    idc_collection_id = Column(String, comment="idc_collection_id of this collection")
    dev_tcia_url = Column(String, comment="Dev tcia bucket name")
    dev_idc_url = Column(String, comment="Dev idc bucket name")
    gcs_pub_tcia_url = Column(String, comment="Public gcs tcia bucket name")
    gcs_pub_idc_url = Column(String, comment="Public gcs idc bucket name")
    aws_pub_tcia_url = Column(String, comment="Public aws tcia bucket name")
    aws_pub_idc_url = Column(String, comment="Public aws idc bucket name")
    tcia_access = Column(String, comment="'Public', 'Limited', or 'Excluded'")
    idc_access = Column(String, comment="'Public', 'Limited', or 'Excluded'")

# A tableof collections having radiology data that has been redacted due to containing
# face scans
class Redacted_Collections(Base):
    __tablename__ = 'redacted_collections'
    tcia_api_collection_id = Column(String, primary_key=True, comment='Collection ID')
    idc_collection_id = Column(String, comment="idc_collection_id of this collection")
    dev_tcia_url = Column(String, comment="Dev tcia bucket name")
    dev_idc_url = Column(String, comment="Dev idc bucket name")
    gcs_pub_tcia_url = Column(String, comment="Public gcs tcia bucket name")
    gcs_pub_idc_url = Column(String, comment="Public gcs idc bucket name")
    aws_pub_tcia_url = Column(String, comment="Public aws tcia bucket name")
    aws_pub_idc_url = Column(String, comment="Public aws idc bucket name")
    tcia_access = Column(String, comment="'Public', 'Limited', or 'Excluded'")
    idc_access = Column(String, comment="'Public', 'Limited', or 'Excluded'")

# A table of all collections not in the previous four tables.
class Open_Collections(Base):
    __tablename__ = 'open_collections'
    tcia_api_collection_id = Column(String, primary_key=True, comment='Collection ID')
    idc_collection_id = Column(String, comment="idc_collection_id of this collection")
    dev_tcia_url = Column(String, comment="Dev tcia bucket name")
    dev_idc_url = Column(String, comment="Dev idc bucket name")
    gcs_pub_tcia_url = Column(String, comment="Public gcs tcia bucket name")
    gcs_pub_idc_url = Column(String, comment="Public gcs idc bucket name")
    aws_pub_tcia_url = Column(String, comment="Public aws tcia bucket name")
    aws_pub_idc_url = Column(String, comment="Public aws idc bucket name")
    tcia_access = Column(String, comment="'Public', 'Limited', or 'Excluded'")
    idc_access = Column(String, comment="'Public', 'Limited', or 'Excluded'")

# The table that is the union of the previous five tables
class All_Collections(Base):
    __tablename__ = 'all_collections'
    tcia_api_collection_id = Column(String, primary_key=True, comment='Collection ID')
    idc_collection_id = Column(String, comment="idc_collection_id of this collection")
    dev_tcia_url = Column(String, comment="Dev tcia bucket name")
    dev_idc_url = Column(String, comment="Dev idc bucket name")
    gcs_pub_tcia_url = Column(String, comment="Public gcs tcia bucket name")
    gcs_pub_idc_url = Column(String, comment="Public gcs idc bucket name")
    aws_pub_tcia_url = Column(String, comment="Public aws tcia bucket name")
    aws_pub_idc_url = Column(String, comment="Public aws idc bucket name")
    tcia_access = Column(String, comment="'Public', 'Limited', or 'Excluded'")
    idc_access = Column(String, comment="'Public', 'Limited', or 'Excluded'")

# A table that is the union of cr_collections, defaced_collections and open_collections.
# This table is probably not useful because it does not actually reflect all included data.
class All_Included_Collections(Base):
    __tablename__ = 'all_included_collections'
    tcia_api_collection_id = Column(String, primary_key=True, comment='Collection ID')
    idc_webapp_collection_id = Column(String)
    idc_collection_id = Column(String)
    dev_tcia_url = Column(String)
    dev_idc_url = Column(String)
    gcs_pub_tcia_url = Column(String, comment="Public gcs tcia bucket name")
    gcs_pub_idc_url = Column(String, comment="Public gcs idc bucket name")
    aws_pub_tcia_url = Column(String, comment="Public aws tcia bucket name")
    aws_pub_idc_url = Column(String, comment="Public aws idc bucket name")
    tcia_access = Column(String)
    idc_access = Column(String)

# This table is populated with metadata for collections that are not sourced from TCIA.
class Original_Collections_Metadata_IDC_Source(Base):
    __tablename__ = 'original_collections_metadata_idc_source'
    tcia_api_collection_id = Column(String, primary_key=True, comment='Collection ID used by TCIA APIs')
    tcia_wiki_collection_id = Column(String, nullable=False, comment='TCIA Wiki page collection ID')
    idc_webapp_collection_id = Column(String, nullable=False, comment='Collection ID used by IDC webapp')
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
    Description = Column(String, comment='Description of analysis result')







