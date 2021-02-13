#
# Copyright 2020, Institute for Systems Biology
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

# This is the schema for the idc_tcia_DICOM_metadata BQ table
from google.cloud import bigquery

auxilliary_metadata_schema = [
    bigquery.SchemaField(
        name="idc_version_number",
        field_type="INTEGER",
        mode="REQUIRED",
        description="A number, corresponding to the version_uuid, identifying an IDC version"),
    bigquery.SchemaField(
        name="idc_version_timestamp",
        field_type="STRING",
        mode="REQUIRED",
        description="A timestamp identifying when an IDC version was created"),

    bigquery.SchemaField(
        name="TCIA_API_CollectionID",
        field_type="STRING",
        mode="REQUIRED",
        description="The collection ID of this instance's collection as expected by the TCIA API"),
    bigquery.SchemaField(
        name="IDC_Webapp_CollectionID",
        field_type="STRING",
        mode="REQUIRED",
        description="The collection ID of this instance's collection as used internally by the IDC web app"),

    bigquery.SchemaField(
        name="patient_ID",
        field_type="STRING",
        mode="REQUIRED",
        description="Submitter patient_ID"),

    bigquery.SchemaField(
        name="study_uuid",
        field_type="STRING",
        mode="REQUIRED",
        description="""
        A uuid identifying a version of a study. A study_uuid, when prefixed with 'dg.4DFC/', 
        can be resolved to a GA4GH DRS bundle object of the study containing this instance"""),
    bigquery.SchemaField(
        name="StudyInstanceUID",
        field_type="STRING",
        mode="REQUIRED",
        description="The StudyInstanceUID of the study containing this instance"),

    bigquery.SchemaField(
        name="series_uuid",
        field_type="STRING",
        mode="REQUIRED",
        description="""
        A uuid identifying a version of a series. A series_uuid, when prefixed with 'dg.4DFC/', 
        can be resolved to a GA4GH DRS bundle object of the study containing this instance"""),
    bigquery.SchemaField(
        name="SeriesInstanceUID",
        field_type="STRING",
        mode="REQUIRED",
        description="The SOPInstanceUID of the series containing this instance"),

    bigquery.SchemaField(
        name="instance_uuid",
        field_type="STRING",
        mode="REQUIRED",
        description="""
        A uuid identifying a version of an instance. An instance_uuid, when prefixed with 'dg.4DFC/', 
        can be resolved to a GA4GH DRS blob object of this instance"""),
    bigquery.SchemaField(
        name="SOPInstanceUID",
        field_type="STRING",
        mode="REQUIRED",
        description="The SOPInstanceUID of this instance"),
    bigquery.SchemaField(
        name="gcs_url",
        field_type="STRING",
        mode="REQUIRED",
        description="The URL of the GCS object containing this instance"),
    bigquery.SchemaField(
        name="md5_Hash", field_type="STRING",
        mode="REQUIRED",
        description="The hex format md5 hash of this instance"),
    bigquery.SchemaField(
        name="instance_size",
        field_type="INTEGER",
        mode="REQUIRED",
        description="The size, in bytes, of this instance"),
]
