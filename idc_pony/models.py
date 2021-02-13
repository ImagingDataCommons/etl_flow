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

from datetime import datetime
from pony.orm import *
import settings as etl_settings
from python_settings import settings

settings.configure(etl_settings)
assert settings.configured

db = Database()

db.bind('postgres',
    database = settings.DATABASE_NAME,
    user=settings.DATABASE_USERNAME,
    password=settings.DATABASE_PASSWORD,
    host=settings.DATABASE_HOST,
    port=settings.DATABASE_PORT)

class Auxilliary_Metadata(db.Entity):
    crdc_case_id = Optional(str)
    gcs_url = Required(str)
    idc_version_number = Required(int)
    idc_version_timestamp = Required(datetime)
    instance_hash = Required(str)
    instance_size = Required(int)
    instance_uuid = PrimaryKey(str)
    series_instance_uid = Required(str)
    series_instances = Required(int)
    series_uuid = Required(str)
    sop_instance_uid = Required(str)
    study_instance_uid = Required(str)
    study_instances = Required(int)
    study_uuid = Required(str)
    submitter_case_id = Required(str)
    tcia_api_collection_id = Required(str)

class Version(db.Entity):
    idc_version_number = Required(int, unique=True)
    idc_version_timestamp = Required(datetime, unique=True)
    revised = Optional(bool, default= True)
    done = Optional(bool, default= True)
    collections = Set(lambda: Collection)

class Collection(db.Entity):
    idc_version_number = Required(int)
    idc_version_timestamp = Required(datetime)
    revised = Optional(bool, default= True)
    done = Optional(bool, default= True)
    version = Required(Version)
    patients = Set(lambda: Patient)
    tcia_api_collection_id = Required(str)

class Patient(db.Entity):
    idc_version_number = Required(int)
    idc_version_timestamp = Required(datetime)
    revised = Optional(bool, default= True)
    done = Optional(bool, default= True)
    collection = Required(Collection)
    studies = Set(lambda: Study)
    submitter_case_id = Required(str)
    crdc_case_id = Optional(str)


class Study(db.Entity):
    idc_version_number = Required(int)
    idc_version_timestamp = Required(datetime)
    revised = Optional(bool, default=True)
    done = Optional(bool, default=True)
    patient = Required(Patient)
    series = Set(lambda: Series)
    study_instance_uid = Required(str)
    study_uuid = Required(str)
    study_instances = Required(int)

class Series(db.Entity):
    idc_version_number = Required(int)
    idc_version_timestamp = Required(datetime)
    revised = Optional(bool, default=True)
    done = Optional(bool, default=True)
    study = Required(Study)
    instances = Set(lambda: Instance)
    series_instance_uid = Required(str)
    series_uuid = Required(str)
    series_instances = Required(int)

class Instance(db.Entity):
    idc_version_number = Required(int)
    idc_version_timestamp = Required(datetime)
    revised = Optional(bool, default=True)
    done = Optional(bool, default=True)
    series = Required(Series)
    instance_instance_uid = Required(str)
    instance_uuid = Required(str)
    gcs_url = Required(str)
    instance_hash = Required(str)
    instance_size = Required(int)

db.generate_mapping(create_tables=True)
sql_debug(True)
