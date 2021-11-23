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
    Column, DateTime, ForeignKey, create_engine, MetaData, Table, ForeignKeyConstraint, Enum
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
# Note that IDC v2 used a somewhat different schema. That schema and all IDC v2 ETL code is
# in the idc_v2_final branch.
class Version(Base):
    __tablename__ = 'version'
    min_timestamp = Column(DateTime, nullable=True, comment="Time when building this object started")
    revised = Column(Boolean, default=True, comment="If True, this object is revised relative to the previous IDC version")
    done = Column(Boolean, default=True, comment="Set to True if this object has been processed")
    is_new = Column(Boolean, default=True, comment="True if this object is new in this version")
    expanded = Column(Boolean, default=False, comment="True if the next lower level has been populated")
    max_timestamp = Column(DateTime, nullable=True, comment="Time when building this object completed")
    hashes = Column(
        CompositeType(
            'hashes',
            [
                Column('tcia', String, comment="Hash of tcia radiology data"),
                Column('path', String, comment="Hash of tcia pathology data"),
                Column('all_sources', String, comment="Hash of all data")
            ]
        ),
        comment="Source specific hierarchical hash"
    )
    version = Column(Integer, primary_key=True, comment="Target version of revision")

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
                            Column('revised', Boolean, default=True, comment="If True, this object is revised relative to the previous IDC version"),
                            Column('done', Boolean, default=True, comment="Set to True if this object has been processed"),
                            Column('is_new', Boolean, default=True, comment="True if this object is new in this version"),
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
                            Column('revised', Boolean, default=True, comment="If True, this object is revised relative to the previous IDC version"),
                            Column('done', Boolean, default=True, comment="Set to True if this object has been processed"),
                            Column('is_new', Boolean, default=True, comment="True if this object is new in this version"),
                            Column('expanded', Boolean, default=False, comment="True if the next lower level has been populated"),
                            Column('version', Integer, comment="Target version of source-specific revision")
                        ]
                    ),
                    comment="Revision status of path source"
                )
            ]
        )
    )

class Collection(Base):
    __tablename__ = 'collection'
    min_timestamp = Column(DateTime, nullable=True, comment="Time when building this object started")
    collection_id = Column(String, unique=True, primary_key=True, comment='NBIA collection ID')
    revised = Column(Boolean, default=True, comment="If True, this object is revised relative to the previous IDC version")
    done = Column(Boolean, default=True, comment="Set to True if this object has been processed")
    is_new = Column(Boolean, default=True, comment="True if this object is new in this version")
    expanded = Column(Boolean, default=False, comment="True if the next lower level has been populated")
    init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    max_timestamp = Column(DateTime, nullable=True, comment="Time when building this object completed")
    sources = Column(
        CompositeType(
            'sources',
            [
                Column('tcia', Boolean),
                Column('path', Boolean)
            ]
        ),
        nullable=True,
        comment="True if this objects includes instances from the corresponding source"
    )
    hashes = Column(
        CompositeType(
            'hashes',
            [
                Column('tcia', String, comment="Hash of tcia radiology data"),
                Column('path', String, comment="Hash of tcia pathology data"),
                Column('all_sources', String, comment="Hash of all data")
            ]
        ),
        nullable=True,
        comment="Source specific hierarchical hash"
    )
    idc_collection_id = Column(String, nullable=False, unique=True, comment="IDC assigned collection ID")
    uuid = Column(String, nullable=False, unique=True, comment="IDC assigned UUID of a version of this object")

    patients = relationship("Patient", back_populates="collection", order_by="Patient.submitter_case_id", cascade="all, delete")
    # patients = relationship("Patient", backref="the_collection")

class Patient(Base):
    __tablename__ = 'patient'
    min_timestamp = Column(DateTime, nullable=True, comment="Time when building this object started")
    submitter_case_id = Column(String, nullable=False, unique=True, primary_key=True, comment="Submitter's patient ID")
    idc_case_id = Column(String, nullable=False, unique=True, comment="IDC assigned patient ID")
    revised = Column(Boolean, default=True, comment="If True, this object is revised relative to the previous IDC version")
    done = Column(Boolean, default=True, comment="True if this object has been processed")
    is_new = Column(Boolean, default=True, comment="True if this object is new in this version")
    expanded = Column(Boolean, default=False, comment="True if the next lower level has been populated")
    init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    collection_id = Column(ForeignKey('collection.collection_id'), comment="Containing object")
    max_timestamp = Column(DateTime, nullable=True, comment="Time when building this object completed")
    sources = Column(
        CompositeType(
            'sources',
            [
                Column('tcia', Boolean),
                Column('path', Boolean)
            ]
        ),
        nullable=True,
        comment="True if this objects includes instances from the corresponding source"
    )
    hashes = Column(
        CompositeType(
            'hashes',
            [
                Column('tcia', String, comment="Hash of tcia radiology data"),
                Column('path', String, comment="Hash of tcia pathology data"),
                Column('all_sources', String, comment="Hash of all data")
            ]
        ),
        nullable=True,
        comment="Source specific hierarchical hash"
    )
    uuid = Column(String, nullable=False, unique=True, comment="IDC assigned UUID of a version of this object")

    collection = relationship("Collection", back_populates="patients")
    studies = relationship("Study", back_populates="patient", order_by="Study.study_instance_uid", cascade="all, delete")
    # studies = relationship("Study", backref="patient")

class Study(Base):
    __tablename__ = 'study'
    min_timestamp = Column(DateTime, nullable=True, comment="Time when building this object started")
    study_instance_uid = Column(String, unique=True, primary_key=True, nullable=False)
    uuid = Column(String, nullable=False, unique=True, comment="IDC assigned UUID of a version of this object")
    study_instances = Column(Integer, nullable=False, comment="Instances in this study")
    revised = Column(Boolean, default=True, comment="If True, this object is revised relative to the previous IDC version")
    done = Column(Boolean, default=True, comment="True if this object has been processed")
    is_new = Column(Boolean, default=True, comment="True if this object is new in this version")
    expanded = Column(Boolean, default=False, comment="True if the next lower level has been populated")
    init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    submitter_case_id = Column(ForeignKey('patient.submitter_case_id'), comment="Submitter's patient ID")
    max_timestamp = Column(DateTime, nullable=True, comment="Time when building this object completed")
    sources = Column(
        CompositeType(
            'sources',
            [
                Column('tcia', Boolean),
                Column('path', Boolean)
            ]
        ),
        nullable=True,
        comment="True if this objects includes instances from the corresponding source"
    )
    hashes = Column(
        CompositeType(
            'hashes',
            [
                Column('tcia', String, comment="Hash of tcia radiology data"),
                Column('path', String, comment="Hash of tcia pathology data"),
                Column('all_sources', String, comment="Hash of all data")
            ]
        ),
        nullable=True,
        comment="Source specific hierarchical hash"
    )

    patient = relationship("Patient", back_populates="studies")
    seriess = relationship("Series", back_populates="study", order_by="Series.series_instance_uid", cascade="all, delete")
    # series = relationship("Study", backref="study")

class Series(Base):
    __tablename__ = 'series'
    min_timestamp = Column(DateTime, nullable=True, comment="Time when building this object started")
    series_instance_uid = Column(String, unique=True, primary_key=True, nullable=False)
    uuid = Column(String, nullable=False, unique=True, comment="IDC assigned UUID of a version of this object")
    series_instances = Column(Integer, nullable=False, comment="Instances in this series")
    source_doi = Column(String, nullable=True, comment="A doi to the wiki page of this source of this series")
    revised = Column(Boolean, default=True, comment="If True, this object is revised relative to the previous IDC version")
    done = Column(Boolean, default=True, comment="True if this object has been processed")
    is_new = Column(Boolean, default=True, comment="True if this object is new in this version")
    expanded = Column(Boolean, default=False, comment="True if the next lower level has been populated")
    init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    study_instance_uid = Column(ForeignKey('study.study_instance_uid'), comment="Containing object")
    max_timestamp = Column(DateTime, nullable=True, comment="Time when building this object completed")
    sources = Column(
        CompositeType(
            'sources',
            [
                Column('tcia', Boolean),
                Column('path', Boolean)
            ]
        ),
        nullable=True,
        comment="True if this objects includes instances from the corresponding source"
    )
    hashes = Column(
        CompositeType(
            'hashes',
            [
                Column('tcia', String, comment="Hash of tcia radiology data"),
                Column('path', String, comment="Hash of tcia pathology data"),
                Column('all_sources', String, comment="Hash of all data")
            ]
        ),
        nullable=True,
        comment="Source specific hierarchical hash"
    )

    study = relationship("Study", back_populates="seriess")
    instances = relationship("Instance", back_populates="series", order_by="Instance.sop_instance_uid", cascade="all, delete")
    # instances = relationship("Study", backref="series")

class Instance(Base):
    __tablename__ = 'instance'
    timestamp = Column(DateTime, nullable=True, comment="Time when this object was last built")
    sop_instance_uid = Column(String, primary_key=True, nullable=False)
    uuid = Column(String, nullable=False, unique=True, comment="IDC assigned UUID of a version of this object")
    hash = Column(String, nullable=True, comment="Hierarchical hex format MD5 hash of TCIA data at this level")
    size = Column(Integer, nullable=True, comment='Instance blob size (bytes)')
    revised = Column(Boolean, default=True, comment="If True, this object is revised relative to the previous IDC version")
    done = Column(Boolean, default=True, comment="True if this object has been processed")
    is_new = Column(Boolean, default=True, comment="True if this object is new in this version")
    expanded = Column(Boolean, default=False, comment="True if the next lower level has been populated")
    init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    series_instance_uid = Column(ForeignKey('series.series_instance_uid'), comment="Containing object")
    source = Column(Enum(instance_source), nullable=False, comment='Source of this object; "tcia", "path"')

    series = relationship("Series", back_populates="instances")

class Retired(Base):
    __tablename__ = 'retired'
    timestamp = Column(DateTime, nullable=True, comment="Time when this object was last updated by TCIA/NBIA")
    sop_instance_uid = Column(String, primary_key=True, nullable=False)
    source = Column(Enum(instance_source), nullable=False, comment='Source of this instance; "tcia", "path"')
    hash = Column(String, nullable=False, comment="Hex format MD5 hash of this instance")
    size = Column(Integer, nullable=False, comment='Instance blob size (bytes)')
    init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    study_instance_uid = Column(String, comment="Containing object")
    series_instance_uid = Column(String, comment="Containing object")
    submitter_case_id = Column(String, comment="Containing object")
    collection_id = Column(String, comment="Containing object")
    instance_uuid = Column(String, nullable=False)
    series_uuid = Column(String, comment="Containing object")
    study_uuid = Column(String, comment="Containing object")
    idc_case_id = Column(String, comment="Containing object")
    source_doi = Column(String, comment="Source DOI")

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

