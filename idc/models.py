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

Base = declarative_base()
# sql_engine = create_engine(sql_uri, echo=True)
# sql_engine = create_engine(sql_uri)

# These tables define the ETL database. There is a separate DB for each IDC version.
# Note that earlier IDC versions used a one-to-many schema.

version_collection = Table('version_collection', Base.metadata,
                           Column('version', ForeignKey('version_mm.version'), primary_key=True),
                           Column('collection_uuid', ForeignKey('collection_mm.uuid'), primary_key=True))

class Version(Base):
    __tablename__ = 'version_mm'
    version = Column(Integer, primary_key=True, comment="Target version of revision")
    min_timestamp = Column(DateTime, nullable=True, comment="Time when building this object started")
    max_timestamp = Column(DateTime, nullable=True, comment="Time when building this object completed")
    revised = Column(Boolean, default=False, comment="If True, this object is revised relative to the previous IDC version")
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
    source_statuses = Column(
        CompositeType(
            'statuses',
            [
                Column('tcia',
                    CompositeType(
                        'status',
                        [
                            Column('min_timestamp',DateTime, nullable=True, comment="Time when building this object started"),
                            Column('max_timestamp', DateTime, nullable=True, comment="Time when building this object completed"),
                            Column('revised', Boolean, default=False, comment="If True, this object is revised relative to the previous IDC version"),
                            Column('done', Boolean, default=False, comment="Set to True if this object has been processed"),
                            Column('is_new', Boolean, default=False, comment="True if this object is new in this version"),
                            Column('expanded', Boolean, default=False, comment="True if the next lower level has been populated"),
                            Column('version', Integer, comment="Target version of source-specific revision")
                        ]
                    ),
                    comment="Revision status of tcia source"
                ),
                Column('path',
                    CompositeType(
                        'status',
                        [
                            Column('min_timestamp',DateTime, nullable=True, comment="Time when building this object started"),
                            Column('max_timestamp', DateTime, nullable=True, comment="Time when building this object completed"),
                            Column('revised', Boolean, default=False, comment="If True, this object is revised relative to the previous IDC version"),
                            Column('done', Boolean, default=False, comment="Set to True if this object has been processed"),
                            Column('is_new', Boolean, default=False, comment="True if this object is new in this version"),
                            Column('expanded', Boolean, default=False, comment="True if the next lower level has been populated"),
                            Column('version', Integer, comment="Target version of source-specific revision")
                        ]
                    ),
                    comment="Revision status of path source"
                )
            ]
        )
    )
    collections = relationship('Collection',
                               secondary=version_collection,
                               back_populates='versions')

collection_patient = Table('collection_patient', Base.metadata,
                           Column('collection_uuid', ForeignKey('collection_mm.uuid'), primary_key=True),
                           Column('patient_uuid', ForeignKey('patient_mm.uuid'), primary_key=True))

class Collection(Base):
    __tablename__ = 'collection_mm'
    collection_id = Column(String, nullable=False, unique=False, comment='TCIA/NBIA collection ID')
    idc_collection_id = Column(String, nullable=False, unique=False, comment="IDC assigned collection ID")
    uuid = Column(String, nullable=False, primary_key=True, comment="IDC assigned UUID of a version of this object")

    min_timestamp = Column(DateTime, nullable=True, comment="Time when building this object started")
    max_timestamp = Column(DateTime, nullable=True, comment="Time when building this object completed")
    init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    final_idc_version = Column(Integer, default=0, comment="Final IDC version of this version of this object")
    revised = Column(Boolean, default=False, comment="If True, this object is revised relative to the previous IDC version")
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
    versions = relationship('Version',
                               secondary=version_collection,
                               back_populates='collections')

    patients = relationship('Patient',
                               secondary=collection_patient,
                               back_populates='collections')


patient_study = Table('patient_study', Base.metadata,
                           Column('patient_uuid', ForeignKey('patient_mm.uuid'), primary_key=True),
                           Column('study_uuid', ForeignKey('study_mm.uuid'), primary_key=True))

class Patient(Base):
    __tablename__ = 'patient_mm'
    submitter_case_id = Column(String, nullable=False, unique=False, comment="Submitter's patient ID")
    idc_case_id = Column(String, nullable=False, unique=False, comment="IDC assigned patient ID")
    uuid = Column(String, nullable=False, primary_key=True, comment="IDC assigned UUID of a version of this object")

    min_timestamp = Column(DateTime, nullable=True, comment="Time when building this object started")
    max_timestamp = Column(DateTime, nullable=True, comment="Time when building this object completed")
    init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    final_idc_version = Column(Integer, default=0, comment="Final IDC version of this version of this object")
    revised = Column(Boolean, default=False, comment="If True, this object is revised relative to the previous IDC version")
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
    collections = relationship('Collection',
                               secondary=collection_patient,
                               back_populates='patients')
    studies = relationship('Study',
                            secondary=patient_study,
                            back_populates='patients')


study_series = Table('study_series', Base.metadata,
                      Column('study_uuid', ForeignKey('study_mm.uuid'), primary_key=True),
                      Column('series_uuid', ForeignKey('series_mm.uuid'), primary_key=True))


class Study(Base):
    __tablename__ = 'study_mm'
    study_instance_uid = Column(String, nullable=False, unique=False, comment="DICOM StudyInstanceUID")
    uuid = Column(String, nullable=False, unique=True, primary_key=True, comment="IDC assigned UUID of a version of this object")
    study_instances = Column(Integer, nullable=True, unique=False, comment="Instances in this study")

    min_timestamp = Column(DateTime, nullable=True, comment="Time when building this object started")
    max_timestamp = Column(DateTime, nullable=True, comment="Time when building this object completed")
    init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    final_idc_version = Column(Integer, default=0, comment="Final IDC version of this version of this object")
    revised = Column(Boolean, default=False, comment="If True, this object is revised relative to the previous IDC version")
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

    patients = relationship('Patient',
                            secondary=patient_study,
                            back_populates='studies')
    seriess = relationship('Series',
                           secondary=study_series,
                           back_populates='studies')


series_instance = Table('series_instance', Base.metadata,
                     Column('series_uuid', ForeignKey('series_mm.uuid'), primary_key=True),
                     Column('instance_uuid', ForeignKey('instance_mm.uuid'), primary_key=True))

class Series(Base):
    __tablename__ = 'series_mm'
    series_instance_uid = Column(String, unique=False, nullable=False, comment="DICOM SeriesInstanceUID")
    uuid = Column(String, primary_key=True, comment="IDC assigned UUID of a version of this object")
    series_instances = Column(Integer, nullable=True, comment="Instances in this series")
    source_doi = Column(String, nullable=True, comment="A doi to the wiki page of this source of this series")

    min_timestamp = Column(DateTime, nullable=True, comment="Time when building this object started")
    max_timestamp = Column(DateTime, nullable=True, comment="Time when building this object completed")
    init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    final_idc_version = Column(Integer, default=0, comment="Final IDC version of this version of this object")
    revised = Column(Boolean, default=False, comment="If True, this object is revised relative to the previous IDC version")
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

    studies = relationship('Study',
                           secondary=study_series,
                           back_populates='seriess')
    instances = relationship('Instance',
                          secondary=series_instance,
                          back_populates='seriess')

class Instance(Base):
    __tablename__ = 'instance_mm'
    sop_instance_uid = Column(String, nullable=False, unique=False, comment='DICOM SOPInstanceUID')
    uuid = Column(String, primary_key=True, comment="IDC assigned UUID of a version of this object")
    hash = Column(String, nullable=True, comment="Hierarchical hex format MD5 hash of TCIA data at this level")
    size = Column(Integer, nullable=True, comment='Instance blob size (bytes)')

    revised = Column(Boolean, default=False, comment="If True, this object is revised relative to the previous IDC version")
    done = Column(Boolean, default=False, comment="Set to True if this object has been processed")
    is_new = Column(Boolean, default=False, comment="True if this object is new in this version")
    expanded = Column(Boolean, default=False, comment="True if the next lower level has been populated")
    init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    final_idc_version = Column(Integer, default=0, comment="Final IDC version of this version of this object")
    source = Column(Enum(instance_source), nullable=True, comment='Source of this object; "tcia", "path"')
    timestamp = Column(DateTime, nullable=True, comment="Time when this object was last built")

    seriess = relationship('Series',
                          secondary=series_instance,
                          back_populates='instances')

# class Retired(Base):
#     __tablename__ = 'retired'
#     timestamp = Column(DateTime, nullable=True, comment="Time when this object was last updated by TCIA/NBIA")
#     sop_instance_uid = Column(String, primary_key=True, nullable=False)
#     source = Column(Enum(instance_source), nullable=False, comment='Source of this instance; "tcia", "path"')
#     hash = Column(String, nullable=False, comment="Hex format MD5 hash of this instance")
#     size = Column(Integer, nullable=False, comment='Instance blob size (bytes)')
#     init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
#     rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
#     study_instance_uid = Column(String, comment="Containing object")
#     series_instance_uid = Column(String, comment="Containing object")
#     submitter_case_id = Column(String, comment="Containing object")
#     collection_id = Column(String, comment="Containing object")
#     instance_uuid = Column(String, nullable=False)
#     series_uuid = Column(String, comment="Containing object")
#     study_uuid = Column(String, comment="Containing object")
#     idc_case_id = Column(String, comment="Containing object")
#     source_doi = Column(String, comment="Source DOI")

class WSI_metadata(Base):
    __tablename__ = 'wsi_metadata'
    collection_id = Column(String, nullable=False)
    submitter_case_id = Column(String, nullable=False)
    study_instance_uid = Column(String, nullable=False)
    series_instance_uid = Column(String, nullable=False)
    sop_instance_uid = Column(String, unique=True, primary_key=True, nullable=False)
    gcs_url = Column(String, nullable=False)
    hash = Column(String, comment="Hex format MD5 hash of this instance")
    size = Column(BigInteger, comment='Instance blob size (bytes)')

# Base.metadata.create_all(sql_engine)

