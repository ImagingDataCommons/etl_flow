#
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

# This is the schema for the idc_tcia_collections_metadata BQ table
from google.cloud import bigquery

auxiliary_metadata_schema = [
    bigquery.SchemaField('collection_name', 'STRING', mode='NULLABLE', description='Collection name as used externally by IDC webapp'),
    bigquery.SchemaField('collection_id', 'STRING', mode='NULLABLE', description='Collection ID as used internally by IDC webapp and accepted by the IDC API'),
    bigquery.SchemaField('collection_timestamp', 'DATETIME', mode='NULLABLE', description='Revision timestamp'),
    bigquery.SchemaField('collection_hash', 'STRING', mode='NULLABLE', description='md5 hash of the of this version of the collection containing this instance'),
    bigquery.SchemaField('collection_init_idc_version', 'INTEGER', mode='NULLABLE', description='The IDC version in which the collection containing this instance first appeared'),
    bigquery.SchemaField('collection_revised_idc_version', 'INTEGER', mode='NULLABLE', description='The IDC version in which this version of the collection containing this instance first appeared'),
    bigquery.SchemaField('submitter_case_id', 'STRING', mode='NULLABLE', description='Patient ID assigned by submitter of this data'),
    bigquery.SchemaField('idc_case_id', 'STRING', mode='NULLABLE', description='IDC assigned UUID4 id of this version of the case/patient containing this instance'),
    bigquery.SchemaField('patient_hash', 'STRING', mode='NULLABLE', description='md5 hash of this version of the patient/case containing this instance'),
    bigquery.SchemaField('patient_init_idc_version', 'INTEGER', mode='NULLABLE', description='The IDC version in which the patient/case containing this instance first appeared'),
    bigquery.SchemaField('patient_revised_idc_version', 'INTEGER', mode='NULLABLE', description='The IDC version in which this version of the patient/case containing this instance first appeared'),
    bigquery.SchemaField('StudyInstanceUID', 'STRING', mode='NULLABLE', description='DICOM study containing this instance'),
    bigquery.SchemaField('study_uuid', 'STRING', mode='NULLABLE', description='UUID of this version of the study containing this instance'),
    bigquery.SchemaField('study_instances', 'INTEGER', mode='NULLABLE', description='Number of instances in the version of the study containing this instance'),
    bigquery.SchemaField('study_hash', 'STRING', mode='NULLABLE', description='md5 hash of the data in the this version of the study containing this instance'),
    bigquery.SchemaField('study_init_idc_version', 'INTEGER', mode='NULLABLE', description='The IDC version in which the study containing this instance first appeared'),
    bigquery.SchemaField('study_revised_idc_version', 'INTEGER', mode='NULLABLE', description='The IDC version in which this version of the study containing this instance first appeared'),
    bigquery.SchemaField('study_final_idc_version', 'INTEGER', mode='NULLABLE', description='The IDC version in which this version of the study containing this instance last appeared. If 0, thise is the current version.'),
    bigquery.SchemaField('SeriesInstanceUID', 'STRING', mode='NULLABLE', description='DICOM series containing this instance'),
    bigquery.SchemaField('series_uuid', 'STRING', mode='NULLABLE', description='UUID of this version of the series containing this instance'),
    bigquery.SchemaField('series_gcs_url', 'STRING', mode='NULLABLE', description='URL of the Google Cloud Storage (GCS) folder of the series containing this instance'),
    bigquery.SchemaField('series_aws_url', 'STRING', mode='NULLABLE', description='URL of the Amazon Web Services (AWS) folder of the series containing this instance'),
    bigquery.SchemaField('Source_DOI', 'STRING', mode='NULLABLE', description='The DOI of a wiki page that describes the original collection or analysis result that includes this instance'),
    bigquery.SchemaField('Source_URL', 'STRING', mode='NULLABLE', description='The URL of a wiki page that describes the original collection or analysis result that includes this instance'),
    bigquery.SchemaField('versioned_Source_DOI', 'STRING', mode='NULLABLE', description='If present, the DOI of a wiki page that describes the original collection or analysis result that includes this version of this instance'),
    bigquery.SchemaField('series_instances', 'INTEGER', mode='NULLABLE', description='Number of instances in the version of the study containing this instance'),
    bigquery.SchemaField('series_hash', 'STRING', mode='NULLABLE', description='md5 hash of the data in the this version of the series containing this instance'),
    bigquery.SchemaField('series_init_idc_version', 'INTEGER', mode='NULLABLE', description='The IDC version in which the series containing this instance first appeared'),
    bigquery.SchemaField('series_revised_idc_version', 'INTEGER', mode='NULLABLE', description='The IDC version in which this version of the series containing this instance first appeared'),
    bigquery.SchemaField('series_final_idc_version', 'INTEGER', mode='NULLABLE', description='The IDC version in which this version of the series containing this instance last appeared. If 0, thise is the current version.'),
    bigquery.SchemaField('SOPInstanceUID', 'STRING', mode='NULLABLE', description='DICOM instance containing this instance version'),
    bigquery.SchemaField('instance_uuid', 'STRING', mode='NULLABLE', description='UUID of this version of this instance'),
    bigquery.SchemaField('gcs_url', 'STRING', mode='NULLABLE', description='URL of the Google Cloud Storage (GCS) object containing the current version of this instance' ),
    bigquery.SchemaField('gcs_bucket', 'STRING', mode='NULLABLE', description='Name of the Google Cloud Storage (GCS) bucket containing the current version of this instance' ),
    bigquery.SchemaField('aws_url', 'STRING', mode='NULLABLE', description='URL to the Amazon Web Services (AWS) object containing the current version of this instance'),
    bigquery.SchemaField('aws_bucket', 'STRING', mode='NULLABLE', description='Name to the Amazon Web Services (AWS) bucket containing the current version of this instance'),
    bigquery.SchemaField('instance_size', 'INTEGER', mode='NULLABLE', description='Size in bytes of this version of this instance'),
    bigquery.SchemaField('instance_hash', 'STRING', mode='NULLABLE', description='md5 hash of the data in the this version of this instance'),
    # bigquery.SchemaField('instance_source', 'STRING', mode='NULLABLE', description='Source of the instance, either tcia or idc'),
    bigquery.SchemaField('instance_init_idc_version', 'INTEGER', mode='NULLABLE', description='The IDC version in which this instance first appeared'),
    bigquery.SchemaField('instance_revised_idc_version', 'INTEGER', mode='NULLABLE', description='The IDC version in which this instance first appeared'),
    bigquery.SchemaField('instance_final_idc_version', 'INTEGER', mode='NULLABLE', description='The IDC version in which this instance last appeared. If 0, thise is the current version.'),
    bigquery.SchemaField('access', 'STRING', mode='NULLABLE', description='Collection access status: Public or Limited'),
    bigquery.SchemaField('license_url', 'STRING', mode='NULLABLE', description='URL of license of this analysis result'),
    bigquery.SchemaField('license_long_name', 'STRING', mode='NULLABLE', description='Long name of license of this analysis result'),
    bigquery.SchemaField('license_short_name', 'STRING', mode='NULLABLE', description='Short name of license of this analysis result'),
    # bigquery.SchemaField('tcia_api_collection_id', 'STRING', mode='NULLABLE', description='DEPRECATED: Collection name as used externally by IDC webapp'),
    # bigquery.SchemaField('idc_webapp_collection_id', 'STRING', mode='NULLABLE', description='DEPRECATED: Collection ID as used internally by IDC webapp and accepted by the IDC API'),

]
