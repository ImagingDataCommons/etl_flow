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
    bigquery.SchemaField('tcia_api_collection_id', 'STRING', mode='NULLABLE', description='Collection ID as accepted by TCIA APIs'),
    bigquery.SchemaField('idc_webapp_collection_id', 'STRING', mode='NULLABLE', description='Collection ID as accepted by the IDC webapp'),
    bigquery.SchemaField('collection_id', 'STRING', mode='NULLABLE', description='Collection ID as accepted by the IDC webapp. Duplicate of idc_webapp_collection_id'),
    bigquery.SchemaField('collection_timestamp', mode='NULLABLE', description='Revision timestamp'),
    bigquery.SchemaField('collection_hash', mode='NULLABLE', description='md5 hash of the of this version of the collection containing this instance'),
    bigquery.SchemaField('collection_init_idc_version', mode='NULLABLE', description='The IDC version in which the collection containing this instance first appeared'),
    bigquery.SchemaField('collection_revised_idc_version', mode='NULLABLE', description='The IDC version in which this version of the collection containing this instance first appeared'),
    bigquery.SchemaField('submitter_case_id', mode='NULLABLE', description='Patient ID assigned by submitter of this data'),
    bigquery.SchemaField('idc_case_id', mode='NULLABLE', description='IDC assigned UUID4 id of this version of the case/patient containing this instance'),
    bigquery.SchemaField('patient_hash', mode='NULLABLE', description='md5 hash of this version of the patient/case containing this instance'),
    bigquery.SchemaField('patient_init_idc_version', mode='NULLABLE', description='The IDC version in which the patient/case containing this instance first appeared'),
    bigquery.SchemaField('patient_revised_idc_version', mode='NULLABLE', description='The IDC version in which this version of the patient/case containing this instance first appeared'),
    bigquery.SchemaField('StudyInstanceUID', mode='NULLABLE', description='DICOM study containing this instance'),
    bigquery.SchemaField('study_uuid', mode='NULLABLE', description='UUID of this version of the study containing this instance'),
    bigquery.SchemaField('study_instances', mode='NULLABLE', description='Number of instances in the version of the study containing this instance'),
    bigquery.SchemaField('study_hash', mode='NULLABLE', description='md5 hash of the data in the this version of the study containing this instance'),
    bigquery.SchemaField('study_init_idc_version', mode='NULLABLE', description='The IDC version in which the study containing this instance first appeared'),
    bigquery.SchemaField('study_revised_idc_version', mode='NULLABLE', description='The IDC version in which this version of the study containing this instance first appeared'),
    bigquery.SchemaField('SeriesInstanceUID', mode='NULLABLE', description='DICOM series containing this instance'),
    bigquery.SchemaField('series_uuid', mode='NULLABLE', description='UUID of this version of the series containing this instance'),
    bigquery.SchemaField('source_doi', mode='NULLABLE', description='The DOI of a wiki page that describes the original collection or analysis result that includes this instance'),
    bigquery.SchemaField('source_url', mode='NULLABLE', description='The URK of a wiki page that describes the original collection or analysis result that includes this instance'),
    bigquery.SchemaField('series_instances', mode='NULLABLE', description='Number of instances in the version of the study containing this instance'),
    bigquery.SchemaField('series_hash', mode='NULLABLE', description='md5 hash of the data in the this version of the series containing this instance'),
    bigquery.SchemaField('series_init_idc_version', mode='NULLABLE', description='The IDC version in which the series containing this instance first appeared'),
    bigquery.SchemaField('series_revised_idc_version', mode='NULLABLE', description='The IDC version in which this version of the series containing this instance first appeared'),
    bigquery.SchemaField('SOPInstanceUID', mode='NULLABLE', description='DICOM instance containing this instance version'),
    bigquery.SchemaField('instance_uuid', mode='NULLABLE', description='UUID of this version of this instance'),
    bigquery.SchemaField('gcs_url', mode='NULLABLE', description='URL to this object containing the current version of this instance in Google Cloud Storage (GCS)'),
    bigquery.SchemaField('instance_size', mode='NULLABLE', description='Size in bytes of this version of this instance'),
    bigquery.SchemaField('instance_hash', mode='NULLABLE', description='md5 hash of the data in the this version of this instance'),
    bigquery.SchemaField('instance_init_idc_version', mode='NULLABLE', description='The IDC version in which this instance first appeared'),
    bigquery.SchemaField('instance_revised_idc_version', mode='NULLABLE', description='The IDC version in which this instance first appeared'),
    bigquery.SchemaField('access', mode='NULLABLE', description='Collection access status: Public or Limited'),
    bigquery.SchemaField('license_url', 'STRING', mode='NULLABLE', description='URL of license of this analysis result'),
    bigquery.SchemaField('license_long_name', 'STRING', mode='NULLABLE', description='Long name of license of this analysis result'),
    bigquery.SchemaField('license_short_name', 'STRING', mode='NULLABLE', description='Short name of license of this analysis result'),
]
