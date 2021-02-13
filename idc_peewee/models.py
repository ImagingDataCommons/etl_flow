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

from peewee import *
import settings as etl_settings
from python_settings import settings

settings.configure(etl_settings)
assert settings.configured

# logger = logging.getLogger(settings.LOGGER_NAME)


database = PostgresqlDatabase(
    settings.DATABASE_NAME,
    user=settings.DATABASE_USERNAME,
    password=settings.DATABASE_PASSWORD,
    host=settings.DATABASE_HOST,
    port=settings.DATABASE_PORT,
    autoconnect=False
)

class UnknownField(object):
    def __init__(self, *_, **__): pass

class BaseModel(Model):
    class Meta:
        database = database

class AuxilliaryMetadata(BaseModel):
    crdc_case_id = CharField(null=True)
    gcs_url = CharField()
    idc_version_number = IntegerField()
    idc_version_timestamp = DateTimeField()
    instance_hash = CharField()
    instance_size = IntegerField()
    # instance_uuid = CharField(primary_key=True)
    instance_uuid = UUIDField(primary_key=True)
    series_instance_uid = CharField()
    series_instances = IntegerField()
    # series_uuid = CharField()
    series_uuid = UUIDField()
    sop_instance_uid = CharField()
    study_instance_uid = CharField()
    study_instances = IntegerField()
    # study_uuid = CharField()
    study_uuid = UUIDField()
    submitter_case_id = CharField()
    tcia_api_collection_id = CharField()

    class Meta:
        table_name = 'auxilliary_metadata'

# class AuxilliaryMetadataDup(BaseModel):
#     crdc_case_id = CharField()
#     gcs_url = CharField()
#     idc_version_number = IntegerField()
#     idc_version_timestamp = DateTimeField()
#     instance_hash = CharField()
#     instance_size = IntegerField()
#     instance_uuid = CharField(primary_key=True)
#     series_instance_uid = CharField()
#     series_instances = IntegerField()
#     series_uuid = CharField()
#     sop_instance_uid = CharField()
#     study_instance_uid = CharField()
#     study_instances = IntegerField()
#     study_uuid = CharField()
#     submitter_case_id = CharField()
#     tcia_api_collection_id = CharField()
#
#     class Meta:
#         table_name = 'auxilliary_metadata_dup'
#         primary_key = False
#
class Foo(BaseModel):
    foo_id = AutoField()
    field1 = CharField()
    field3 = BooleanField(default= True)

    class Meta:
        table_name = 'foo'

class Version(BaseModel):
    version_id = AutoField()
    idc_version_number = IntegerField(unique=True)
    idc_version_timestamp = DateTimeField(unique=True)
    revised = BooleanField(null=True)
    done = BooleanField(null=True)

    class Meta:
        table_name = 'version'

class Collection(BaseModel):
    collection_id = AutoField()
    version_id = ForeignKeyField(Version, backref='collections' )
    idc_version_number = IntegerField()
    idc_version_timestamp = DateTimeField()
    tcia_api_collection_id = CharField(unique=True)
    revised = BooleanField(null=True)
    done = BooleanField(null=True)

    class Meta:
        table_name = 'collection'

class Patient(BaseModel):
    patient_id = AutoField()
    collection_id = ForeignKeyField(Collection, backref='patients')
    idc_version_number = IntegerField()
    idc_version_timestamp = DateTimeField()
    crdc_case_id = CharField(null=True)
    submitter_case_id = CharField()
    revised = BooleanField(null=True)
    done = BooleanField(null=True)

    class Meta:
        table_name = 'patient'

class Study(BaseModel):
    study_id = AutoField()
    patient_id = ForeignKeyField(Patient, backref='studies')
    idc_version_number = IntegerField()
    idc_version_timestamp = DateTimeField()
    study_instance_uid = CharField(unique=True)
    study_uuid = UUIDField(unique=True)
    study_instances = IntegerField()
    revised = BooleanField(null=True)
    done = BooleanField(null=True)

    class Meta:
        table_name = 'study'

class Series(BaseModel):
    series_id = AutoField()
    study_id = ForeignKeyField(Study, backref="studies" )
    idc_version_number = IntegerField()
    idc_version_timestamp = DateTimeField()
    series_instance_uid = CharField(unique=True)
    series_uuid = UUIDField(unique=True)
    series_instances = IntegerField()
    revised = BooleanField(null=True)
    done = BooleanField(null=True)

    class Meta:
        table_name = 'series'

class Instance(BaseModel):
    instance_id = AutoField()
    series_id = ForeignKeyField(Series, backref='series')
    idc_version_number = IntegerField(unique=True)
    idc_version_timestamp = DateTimeField(unique=True)
    sop_instance_uid = CharField(unique=True)
    instance_uuid = UUIDField(unique=True)
    gcs_url = CharField(unique=True)
    instance_hash = CharField(unique=True)
    instance_size = IntegerField()
    revised = BooleanField(null=True)
    done = BooleanField(null=True)

    class Meta:
        table_name = 'instance'

# with database:
#     database.drop_tables([Foo, Version, Collection, Patient, Study, Series, Instance])

with database:database.create_tables([Foo, Version, Collection, Patient, Study, Series, Instance])