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
    Column, DateTime, ForeignKey, create_engine, MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref
from idc.config import sql_uri

Base = declarative_base()
# sql_engine = create_engine(sql_uri, echo=True)
sql_engine = create_engine(sql_uri)


class Version(Base):
    __tablename__ = 'version'
    id = Column(Integer, primary_key=True)
    idc_version_number = Column(Integer, nullable=False, unique=True, comment="IDC version number")
    idc_version_timestamp = Column(DateTime, nullable=False, comment='Time when this object was created')
    revised = Column(Boolean, default=True, comment="If True, this object is revised relative to the previous IDC version")
    done = Column(Boolean, default=True, comment="Set to True if this object has been processed")
    is_new = Column(Boolean, default=True, comment="True if this object is new in this version")
    expanded = Column(Boolean, default=False, comment="True if the next lower level has been populated")

    collections = relationship("Collection", back_populates='version', order_by="Collection.id", cascade="all, delete")

class Collection(Base):
    __tablename__ = 'collection'
    id = Column(Integer, primary_key=True, autoincrement=True)
    version_id = Column(ForeignKey('version.id'), comment="Containing object")
    idc_version_number = Column(Integer, nullable=False, comment="Containing object")
    collection_timestamp = Column(DateTime, nullable=True, comment="Time when this object was last updated by TCIA/NBIA")
    tcia_api_collection_id = Column(String, comment='NBIA collection ID')
    revised = Column(Boolean, default=True, comment="If True, this object is revised relative to the previous IDC version")
    done = Column(Boolean, default=True, comment="Set to True if this object has been processed")
    is_new = Column(Boolean, default=True, comment="True if this object is new in this version")
    expanded = Column(Boolean, default=False, comment="True if the next lower level has been populated")

    version = relationship("Version", back_populates="collections")
    patients = relationship("Patient", back_populates="collection", order_by="Patient.id", cascade="all, delete")

class Patient(Base):
    __tablename__ = 'patient'
    id = Column(Integer, primary_key=True, autoincrement=True)
    collection_id = Column(ForeignKey('collection.id'), comment="Containing object")
    idc_version_number = Column(Integer, nullable=False, comment="Containing object")
    patient_timestamp = Column(DateTime, nullable=True, comment="Time when this object was last updated by TCIA/NBIA")
    submitter_case_id = Column(String, nullable=False, comment="Submitter's patient ID")
    crdc_case_id = Column(String, nullable=True, comment="CRDC patient ID")
    revised = Column(Boolean, default=True, comment="If True, this object is revised relative to the previous IDC version")
    done = Column(Boolean, default=True, comment="True if this object has been processed")
    is_new = Column(Boolean, default=True, comment="True if this object is new in this version")
    expanded = Column(Boolean, default=False, comment="True if the next lower level has been populated")

    collection = relationship("Collection", back_populates="patients")
    studies = relationship("Study", back_populates="patient", order_by="Study.id", cascade="all, delete")

class Study(Base):
    __tablename__ = 'study'
    id = Column(Integer, primary_key=True, autoincrement=True)
    patient_id = Column(ForeignKey('patient.id'), comment="Containing object")
    idc_version_number = Column(Integer, nullable=False, comment="Containing object")
    study_timestamp = Column(DateTime, nullable=True, comment="Time when this object was last updated by TCIA/NBIA")
    study_instance_uid = Column(String, nullable=False)
    study_uuid = Column(String, nullable=False)
    study_instances = Column(Integer, nullable=False, comment="Instances in this study")
    revised = Column(Boolean, default=True, comment="If True, this object is revised relative to the previous IDC version")
    done = Column(Boolean, default=True, comment="True if this object has been processed")
    is_new = Column(Boolean, default=True, comment="True if this object is new in this version")
    expanded = Column(Boolean, default=False, comment="True if the next lower level has been populated")

    patient = relationship("Patient", back_populates="studies")
    seriess = relationship("Series", back_populates="study", order_by="Series.id", cascade="all, delete")

class Series(Base):
    __tablename__ = 'series'
    id = Column(Integer, primary_key=True, autoincrement=True)
    study_id = Column(ForeignKey('study.id'), comment="Containing object")
    idc_version_number = Column(Integer, nullable=False, comment="Containing object")
    series_timestamp = Column(DateTime, nullable=True, comment="Time when this object was last updated by TCIA/NBIA")
    series_instance_uid = Column(String, nullable=False)
    series_uuid = Column(String, nullable=False)
    series_instances = Column(Integer, nullable=False, comment="Instances in this series")
    source_doi = Column(String, nullable=True, comment="A doi to the wiki page of this source of this series")
    revised = Column(Boolean, default=True, comment="If True, this object is revised relative to the previous IDC version")
    done = Column(Boolean, default=True, comment="True if this object has been processed")
    is_new = Column(Boolean, default=True, comment="True if this object is new in this version")
    expanded = Column(Boolean, default=False, comment="True if the next lower level has been populated")

    study = relationship("Study", back_populates="seriess")
    instances = relationship("Instance", back_populates="series", order_by="Instance.id", cascade="all, delete")

class Instance(Base):
    __tablename__ = 'instance'
    id = Column(Integer, primary_key=True, autoincrement=True)
    series_id = Column(ForeignKey('series.id'), comment="Containing object")
    idc_version_number = Column(Integer, nullable=False, comment="Containing object")
    instance_timestamp = Column(DateTime, nullable=True, comment="Time when this object was last updated by TCIA/NBIA")
    sop_instance_uid = Column(String, nullable=False)
    instance_uuid = Column(String, nullable=False)
    gcs_url = Column(String, nullable=False, comment="GCS URL of this instance")
    instance_hash = Column(String, nullable=False, comment="Hex format MD5 hash of this instance")
    instance_size = Column(Integer, nullable=False, comment='Instance blob size (bytes)')
    revised = Column(Boolean, default=True, comment="If True, this object is revised relative to the previous IDC version")
    done = Column(Boolean, default=True, comment="True if this object has been processed")
    is_new = Column(Boolean, default=True, comment="True if this object is new in this version")
    expanded = Column(Boolean, default=False, comment="True if the next lower level has been populated")

    series = relationship("Series", back_populates="instances")

class Auxilliary_Metadata(Base):
    __tablename__ = 'auxilliary_metadata'
    id = Column(Integer, primary_key=True, autoincrement=True)
    idc_version_timestamp = Column(DateTime, nullable=False, comment="A timestamp identifying when an IDC version was created")
    idc_version_number = Column(Integer, nullable=False, comment="A number identifying an IDC version to which this instance belongs")
    collection_timestamp = Column(DateTime, nullable=False, comment="A timestamp identifying when a collection was created/revised")
    tcia_api_collection_id = Column(String, nullable=False, comment="The ID of this instance's TCIA data collection as expected by the TCIA API")
    patient_timestamp = Column(DateTime, nullable=False, comment="A timestamp identifying when a patient was created/revised")
    crdc_case_id = Column(String, nullable=True, comment="The CRDC case ID of this instance's patient")
    submitter_case_id = Column(String, nullable=False, comment="The submitter’s (of data to TCIA) ID of this instance's patient. This is the DICOM PatientID.")
    study_timestamp = Column(DateTime, nullable=False, comment="A timestamp identifying when a study was created/revised")
    study_uuid = Column(String, nullable=False, comment="A uuid identifying a version of a study. A study_uuid, when prefixed with 'dg.4DFC/', can be resolved to a GA4GH DRS bundle object of the study containing this instance")
    study_instance_uid = Column(String, nullable=False, comment="The StudyInstanceUID of the study containing this instance")
    study_instances = Column(Integer, nullable=False, comment="Instances in this study")
    series_timestamp = Column(DateTime, nullable=False, comment="A timestamp identifying when a series was created/revised")
    series_uuid = Column(String, nullable=False, comment="A uuid identifying a version of a series. A series_uuid, when prefixed with 'dg.4DFC/', can be resolved to a GA4GH DRS bundle object of the series containing this instance")
    series_instance_uid = Column(String, nullable=False, comment="The SOPInstanceUID of the series containing this instance")
    series_instances = Column(Integer, nullable=False, comment="Instances in this series")
    instance_timestamp = Column(DateTime, nullable=False, comment="A timestamp identifying when an instance was created/revised")
    instance_uuid = Column(String, nullable=False, comment="A uuid identifying a version of an instance. An instance_uuid, when prefixed with 'dg.4DFC/', can be resolved to a GA4GH DRS blob object of this instance")
    sop_instance_uid = Column(String, nullable=False, comment="The SOPInstanceUID of this instance")
    gcs_url = Column(String, nullable=False, comment="The URL of the GCS object containing this instance")
    instance_hash = Column(String, nullable=False, comment="The hex format md5 hash of this instance")
    instance_size = Column(Integer, nullable=False, comment="The size, in bytes, of this instance")

Base.metadata.create_all(sql_engine)

