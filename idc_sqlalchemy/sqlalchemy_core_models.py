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

from sqlalchemy import create_engine, MetaData, Table, Integer, String, Boolean,\
    Column, DateTime, ForeignKey, Numeric, CheckConstraint

from datetime import datetime

metadata = MetaData()

versions = Table('versions', metadata,
    Column('guid', String, primary_key=True),
    Column('version', String, nullable=False, comment="IDC version number"),
    Column('timestamp', String, nullable=False, comment='Time when this object was created'),
    Column("revised", Boolean, nullable=False, comment="If True, this object is revised relative to the previous IDC version"),
    Column("done", Boolean, nullable=False, comment="Set to True if this object has been processed"),
    )

collections = Table('collections', metadata,
    Column('guid', String, primary_key=True),
    Column('collection_id', String, nullable=False, comment='NBIA collection ID'),
    Column('timestamp', String, nullable=False, comment="Time when this collection was last updated by TCIA/NBIA"),
    Column('version', ForeignKey('versions.version'), comment="Containing object"),
    Column("DOI", String, nullable=True),
    Column("Status", String, nullable=False),
	Column("Access", String, nullable=False),
	Column("Updated", String, nullable=False),
	Column("ImageTypes", String, nullable=False),
	Column("CancerType", String, nullable=False),
	Column("SupportingData", String, nullable=False),
	Column("Location", String, nullable=False),
	Column("Description", String, nullable=False),
	Column("Species", String, nullable=False),
    Column("revised", Boolean, nullable=False, comment="If True, this object is revised relative to the previous IDC version"),
    Column("done", Boolean, nullable=False, comment="Set to True if this object has been processed"),
    )

patients = Table('patients', metadata,
    Column('guid', String, primary_key=True),
    Column('patient_id', String, nullable=False, comment="Submitter's patient ID"),
    Column('timestamp', String, nullable=False, comment="Time when this object was last updated by TCIA/NBIA"),
    Column('collection', ForeignKey("collections.guid"), comment="Containing object"),
    Column("revised", Boolean, nullable=False, comment="If True, this object is revised relative to the previous IDC version"),
    Column('done', Boolean, nullable=False, comment="True if this object has been processed"),
    )

studies = Table('studies', metadata,
    Column('guid', String, primary_key=True),
    Column('study_instance_uid', String, primary_key=True),
    Column('idc_version', ForeignKey("versions.version")),
    Column('timestamp', String, nullable=False, comment="Time when this object was last updated by TCIA/NBIA"),
    Column('patient', ForeignKey("patients.guid"), comment="Containing object"),
    Column("revised", Boolean, nullable=False, comment="If True, this object is revised relative to the previous IDC version"),
    Column('done', Boolean, nullable=False, comment="True if this object has been processed"),
    )

seriess = Table('seriess', metadata,
    Column('guid', String, primary_key=True),
    Column('series_instance_uid', String, primary_key=True),
    Column('timestamp', String, nullable=False, comment="Time when this object was last updated by TCIA/NBIA"),
    Column('study', ForeignKey("studies.guid"), comment="Containing object"),
    Column("revised", Boolean, nullable=False, comment="If True, this object is revised relative to the previous IDC version"),
    Column('done', Boolean, nullable=False, comment="True if this object has been processed"),
    )

instances = Table('instances', metadata,
    Column('guid', String, nullable=False),
    Column('sop_instance_uid', String, primary_key=True),
    Column('timestamp', String, nullable=False, comment="Time when this object was last updated by TCIA/NBIA"),
    Column('series', ForeignKey("seriess.guid"), comment="Containing object"),
    Column('url', String, nullable=False, comment="GCS URL of this instance"),
    Column('md5', String, nullable=False, comment="Hex format MD5 hash of this instance"),
    Column('size', Integer, nullable=False, comment='Instance blob size (bytes)'),
    Column("revised", Boolean, nullable=False, comment="If True, this object is revised relative to the previous IDC version"),
    Column('done', Boolean, nullable=False, comment="True if this object has been processed"),
    )