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
# from idc.config import sql_uri


class instance_source(enum.Enum):
    tcia = 0
    path = 1
    all_sources = 2

Base = declarative_base()
# sql_engine = create_engine(sql_uri, echo=True)
# sql_engine = create_engine(sql_uri)

# These tables define the ETL database. There is a separate DB for each IDC version.
# Note that earlier IDC versions used a one-to-many schema.

version_collection = Table('version_collection', Base.metadata,
                           Column('version', ForeignKey('version.version'), primary_key=True),
                           Column('collection_uuid', ForeignKey('collection.uuid'), primary_key=True))

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
    hashes = Column(
        CompositeType(
            'hashes',
            [
                Column('tcia', String, default="", comment="Hash of tcia radiology data"),
                Column('path', String, default="", comment="Hash of tcia pathology data"),
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
                Column('path', Boolean, default=False)
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
                Column('path', Boolean, default=False, comment="True his object is revised relative to the previous IDC version in the corresponding source")
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
    collection_id = Column(String, nullable=False, unique=False, comment='TCIA/NBIA collection ID')
    idc_collection_id = Column(String, nullable=False, unique=False, comment="IDC assigned collection ID")
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
                Column('path', Boolean, default=False)
            ]
        ),
        nullable=True,
        comment="True if this objects includes instances from the corresponding source"
    )
    hashes = Column(
        CompositeType(
            'hashes',
            [
                Column('tcia', String, default="", comment="Hash of tcia radiology data"),
                Column('path', String, default="", comment="Hash of tcia pathology data"),
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
                Column('path', Boolean, default=False, comment="True his object is revised relative to the previous IDC version in the corresponding source")
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
    submitter_case_id = Column(String, nullable=False, unique=False, comment="Submitter's patient ID")
    idc_case_id = Column(String, nullable=False, unique=False, comment="IDC assigned patient ID")
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
                Column('path', Boolean, default=False)
            ]
        ),
        nullable=True,
        comment="True if this objects includes instances from the corresponding source"
    )
    hashes = Column(
        CompositeType(
            'hashes',
            [
                Column('tcia', String, default="", comment="Hash of tcia radiology data"),
                Column('path', String, default="", comment="Hash of tcia pathology data"),
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
                Column('path', Boolean, default=False, comment="True his object is revised relative to the previous IDC version in the corresponding source")
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
    study_instance_uid = Column(String, nullable=False, unique=False, comment="DICOM StudyInstanceUID")
    uuid = Column(String, nullable=False, unique=True, primary_key=True, comment="IDC assigned UUID of a version of this object")
    study_instances = Column(Integer, nullable=True, unique=False, comment="Instances in this study")

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
                Column('path', Boolean, default=False)
            ]
        ),
        nullable=True,
        comment="True if this objects includes instances from the corresponding source"
    )
    hashes = Column(
        CompositeType(
            'hashes',
            [
                Column('tcia', String, default="", comment="Hash of tcia radiology data"),
                Column('path', String, default="", comment="Hash of tcia pathology data"),
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
                Column('path', Boolean, default=False, comment="True his object is revised relative to the previous IDC version in the corresponding source")
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
    series_instance_uid = Column(String, unique=False, nullable=False, comment="DICOM SeriesInstanceUID")
    uuid = Column(String, primary_key=True, comment="IDC assigned UUID of a version of this object")
    series_instances = Column(Integer, nullable=True, comment="Instances in this series")
    source_doi = Column(String, nullable=True, comment="A doi to the wiki page of this source of this series")

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
                Column('path', Boolean, default=False)
            ]
        ),
        nullable=True,
        comment="True if this objects includes instances from the corresponding source"
    )
    hashes = Column(
        CompositeType(
            'hashes',
            [
                Column('tcia', String, default="", comment="Hash of tcia radiology data"),
                Column('path', String, default="", comment="Hash of tcia pathology data"),
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
                Column('path', Boolean, default=False, comment="True his object is revised relative to the previous IDC version in the corresponding source")
            ]
        ),
        nullable=True,
    )
    studies = relationship('Study',
                           secondary=study_series,
                           back_populates='seriess')
    instances = relationship('Instance',
                          secondary=series_instance,
                          back_populates='seriess')

class Instance(Base):
    __tablename__ = 'instance'
    sop_instance_uid = Column(String, nullable=False, unique=False, comment='DICOM SOPInstanceUID')
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
    source = Column(Enum(instance_source), nullable=True, comment='Source of this object; "tcia", "path"')
    timestamp = Column(DateTime, nullable=True, comment="Time when this object was last built")
    # Excluded instances are somehow invalid, but are included in the DB to maintain the hash
    excluded = Column(Boolean, default=False, comment="True if instance should be excluded from auxiliary_metacata, etc.")

    seriess = relationship('Series',
                          secondary=series_instance,
                          back_populates='instances')

class Collection_id_map(Base):
    __tablename__ = 'collection_id_map'
    collection_id = Column(String, primary_key=True)
    idc_collection_id = Column(String, nullable=False)


class WSI_Version(Base):
    __tablename__ = 'wsi_version'
    version = Column(Integer, unique=True, primary_key=True, comment='NBIA collection ID')
    hash = Column(String, comment='Version hash')

    collections = relationship("WSI_Collection", back_populates="vers", order_by="WSI_Collection.collection_id", cascade="all, delete")
    # patients = relationship("Patient", backref="the_collection")

class WSI_Collection(Base):
    __tablename__ = 'wsi_collection'
    collection_id = Column(String, unique=True, primary_key=True, comment='NBIA collection ID')
    version = Column(ForeignKey('wsi_version.version'), comment="Containing object")
    hash = Column(String, comment='Collection hash')

    vers = relationship("WSI_Version", back_populates="collections")
    patients = relationship("WSI_Patient", back_populates="collection", order_by="WSI_Patient.submitter_case_id", cascade="all, delete")
    # patients = relationship("Patient", backref="the_collection")

class WSI_Patient(Base):
    __tablename__ = 'wsi_patient'
    submitter_case_id = Column(String, nullable=False, unique=True, primary_key=True, comment="Submitter's patient ID")
    collection_id = Column(ForeignKey('wsi_collection.collection_id'), comment="Containing object")
    hash = Column(String, comment='Patient hash')

    collection = relationship("WSI_Collection", back_populates="patients")
    studies = relationship("WSI_Study", back_populates="patient", order_by="WSI_Study.study_instance_uid", cascade="all, delete")
    # studies = relationship("Study", backref="patient")

class WSI_Study(Base):
    __tablename__ = 'wsi_study'
    study_instance_uid = Column(String, unique=True, primary_key=True, nullable=False)
    submitter_case_id = Column(ForeignKey('wsi_patient.submitter_case_id'), comment="Submitter's patient ID")
    hash = Column(String, comment='Study hash')

    patient = relationship("WSI_Patient", back_populates="studies")
    seriess = relationship("WSI_Series", back_populates="study", order_by="WSI_Series.series_instance_uid", cascade="all, delete")
    # series = relationship("Study", backref="study")

class WSI_Series(Base):
    __tablename__ = 'wsi_series'
    series_instance_uid = Column(String, unique=True, primary_key=True, nullable=False)
    study_instance_uid = Column(ForeignKey('wsi_study.study_instance_uid'), comment="Containing object")
    hash = Column(String, comment='Series hash')

    study = relationship("WSI_Study", back_populates="seriess")
    instances = relationship("WSI_Instance", back_populates="seriess", order_by="WSI_Instance.sop_instance_uid", cascade="all, delete")
    # instances = relationship("Study", backref="series")

class WSI_Instance(Base):
    __tablename__ = 'wsi_instance'
    sop_instance_uid = Column(String, primary_key=True, nullable=False)
    series_instance_uid = Column(ForeignKey('wsi_series.series_instance_uid'), comment="Containing object")
    hash = Column(String, comment='Instance hash')
    url = Column(String, comment='GCS URL of instance')

    seriess = relationship("WSI_Series", back_populates="instances")

class CR_Collections(Base):
    __tablename__ = 'cr_collections'
    tcia_api_collection_id = Column(String, primary_key=True, comment='Collection ID')
    dev_url = Column(String, comment="Dev bucket name")
    pub_url = Column(String, comment="Public bucket name")
    access = Column(String, comment="Access: Public or Limited")
    v1 = Column(Boolean, comment='True if collection is in v1')
    v2 = Column(Boolean, comment='True if collection is in v2')
    idc_collection_id = Column(String, comment="idc_collection_id of this collection")

class Defaced_Collections(Base):
    __tablename__ = 'defaced_collections'
    tcia_api_collection_id = Column(String, primary_key=True, comment='Collection ID')
    dev_url = Column(String, comment="Dev bucket name")
    pub_url = Column(String, comment="Public bucket name")
    access = Column(String, comment="Access: Public or Limited")
    v1 = Column(Boolean, comment='True if collection is in v1')
    v2 = Column(Boolean, comment='True if collection is in v2')
    idc_collection_id = Column(String, comment="idc_collection_id of this collection")

class Excluded_Collections(Base):
    __tablename__ = 'excluded_collections'
    tcia_api_collection_id = Column(String, primary_key=True, comment='Collection ID')
    access = Column(String, comment="Access: Public or Limited")
    idc_collection_id = Column(String, comment="idc_collection_id of this collection")

class Open_Collections(Base):
    __tablename__ = 'open_collections'
    tcia_api_collection_id = Column(String, primary_key=True, comment='Collection ID')
    dev_url = Column(String, comment="Dev bucket name")
    pub_url = Column(String, comment="Public bucket name")
    access = Column(String, comment="Access: Public or Limited")
    idc_collection_id = Column(String, comment="idc_collection_id of this collection")

class Redacted_Collections(Base):
    __tablename__ = 'redacted_collections'
    tcia_api_collection_id = Column(String, primary_key=True, comment='Collection ID')
    dev_url = Column(String, comment="Dev bucket name")
    pub_url = Column(String, comment="Public bucket name")
    access = Column(String, comment="Access: Public or Limited")
    v1 = Column(Boolean, comment='True if collection is in v1')
    v2 = Column(Boolean, comment='True if collection is in v2')
    idc_collection_id = Column(String, comment="idc_collection_id of this collection")

