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

from sqlalchemy import Integer, String, Boolean,\
    Column, DateTime, ForeignKey, create_engine, MetaData, Table, ForeignKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref
from idc.config import sql_uri

Base = declarative_base()
# sql_engine = create_engine(sql_uri, echo=True)
sql_engine = create_engine(sql_uri)

# These tables define the ETL database. There is a separate DB for each IDC version.
# Note that IDC v2 used a somewhat different schema. That schema and all IDC v2 ETL code is
# in the idc_v2_final branch.
class Collection(Base):
    __tablename__ = 'collection'
    timestamp = Column(DateTime, nullable=True, comment="Time when this object was last updated by TCIA/NBIA")
    collection_id = Column(String, unique=True, comment='NBIA collection ID')
    revised = Column(Boolean, default=True, comment="If True, this object is revised relative to the previous IDC version")
    done = Column(Boolean, default=True, comment="Set to True if this object has been processed")
    is_new = Column(Boolean, default=True, comment="True if this object is new in this version")
    expanded = Column(Boolean, default=False, comment="True if the next lower level has been populated")
    hash = Column(String, nullable=True, comment="Hierarchical hex format MD5 hash of this collection")
    init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")

class Patient(Base):
    __tablename__ = 'patient'
    timestamp = Column(DateTime, nullable=True, comment="Time when this object was last updated by TCIA/NBIA")
    submitter_case_id = Column(String, unique=True, nullable=False, comment="Submitter's patient ID")
    idc_case_id = Column(String, unique=True, nullable=True, comment="IDC assigned patient ID")
    revised = Column(Boolean, default=True, comment="If True, this object is revised relative to the previous IDC version")
    done = Column(Boolean, default=True, comment="True if this object has been processed")
    is_new = Column(Boolean, default=True, comment="True if this object is new in this version")
    expanded = Column(Boolean, default=False, comment="True if the next lower level has been populated")
    hash = Column(String, nullable=True, comment="Hierarchical hex format MD5 hash of this patient")
    init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    collection = Column(ForeignKey('collection.collection_id'), comment="Containing object")

class Study(Base):
    __tablename__ = 'study'
    timestamp = Column(DateTime, nullable=True, comment="Time when this object was last updated by TCIA/NBIA")
    study_instance_uid = Column(String, unique=True, nullable=False)
    uuid = Column(String, unique=True, nullable=False)
    study_instances = Column(Integer, nullable=False, comment="Instances in this study")
    revised = Column(Boolean, default=True, comment="If True, this object is revised relative to the previous IDC version")
    done = Column(Boolean, default=True, comment="True if this object has been processed")
    is_new = Column(Boolean, default=True, comment="True if this object is new in this version")
    expanded = Column(Boolean, default=False, comment="True if the next lower level has been populated")
    hash = Column(String, nullable=True, comment="Hierarchical hex format MD5 hash of this study")
    init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    submitter_case_id = Column(String, nullable=False, comment="Submitter's patient ID")
    collection_id = Column(String, nullable=False, comment="Containing object")
    ForeignKeyConstraint(['collection_id','submitter_case_id'], ['patient.collection','patient.submitter_case_id'])

class Series(Base):
    __tablename__ = 'series'
    timestamp = Column(DateTime, nullable=True, comment="Time when this object was last updated by TCIA/NBIA")
    series_instance_uid = Column(String, unique=True, nullable=False)
    uuid = Column(String, unique=True, nullable=False)
    series_instances = Column(Integer, nullable=False, comment="Instances in this series")
    source_doi = Column(String, nullable=True, comment="A doi to the wiki page of this source of this series")
    revised = Column(Boolean, default=True, comment="If True, this object is revised relative to the previous IDC version")
    done = Column(Boolean, default=True, comment="True if this object has been processed")
    is_new = Column(Boolean, default=True, comment="True if this object is new in this version")
    expanded = Column(Boolean, default=False, comment="True if the next lower level has been populated")
    hash = Column(String, nullable=True, comment="Hierarchical hex format MD5 hash of this series")
    init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    study_instance_uid = Column(ForeignKey('study.study_instance_uid'), comment="Containing object")

class Instance(Base):
    __tablename__ = 'instance'
    timestamp = Column(DateTime, nullable=True, comment="Time when this object was last updated by TCIA/NBIA")
    sop_instance_uid = Column(String, unique=True, nullable=False)
    uuid = Column(String, unique=True, nullable=False)
    hash = Column(String, nullable=False, comment="Hex format MD5 hash of this instance")
    instance_size = Column(Integer, nullable=False, comment='Instance blob size (bytes)')
    revised = Column(Boolean, default=True, comment="If True, this object is revised relative to the previous IDC version")
    done = Column(Boolean, default=True, comment="True if this object has been processed")
    is_new = Column(Boolean, default=True, comment="True if this object is new in this version")
    expanded = Column(Boolean, default=False, comment="True if the next lower level has been populated")
    init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    series_instance_uid = Column(ForeignKey('series.series_instance_uid'), comment="Containing object")

class Retired(Base):
    __tablename__ = 'retired'
    timestamp = Column(DateTime, nullable=True, comment="Time when this object was last updated by TCIA/NBIA")
    sop_instance_uid = Column(String, nullable=False)
    uuid = Column(String, nullable=False)
    hash = Column(String, nullable=False, comment="Hex format MD5 hash of this instance")
    instance_size = Column(Integer, nullable=False, comment='Instance blob size (bytes)')
    init_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this object")
    rev_idc_version = Column(Integer, nullable=False, comment="Initial IDC version of this version of this object")
    series_instance_uid = Column(String, comment="Containing object")

class UUIDS(base):
    __tablename__ = 'uuids'
    uuid = Column(String, unique=True, nullable=False)

Base.metadata.create_all(sql_engine)

