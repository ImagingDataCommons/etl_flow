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

# This is the schema for the mutable_metadata BQ table
from google.cloud import bigquery

mutable_metadata_schema = [
    bigquery.SchemaField('crdc_study_uuid', 'STRING', mode='NULLABLE', description='UUID of this version of the study containing this instance'),
    bigquery.SchemaField('crdc_series_uuid', 'STRING', mode='NULLABLE', description='UUID of this version of the series containing this instance'),
    bigquery.SchemaField('crdc_instance_uuid', 'STRING', mode='NULLABLE', description='UUID of this version of this instance'),
    bigquery.SchemaField('gcs_url', 'STRING', mode='NULLABLE', description='URL to this object containing the current version of this instance in Google Cloud Storage (GCS)'),
    bigquery.SchemaField('aws_url', 'STRING', mode='NULLABLE', description='URL to this object containing the current version of this instance in Amazon Web Services (AWS)'),
    bigquery.SchemaField('access', 'STRING', mode='NULLABLE', description='Collection access status: Public or Limited'),
    bigquery.SchemaField('source_url', 'STRING', mode='NULLABLE', description='The URL of a wiki page that describes the original collection or analysis result that includes this instance'),
    bigquery.SchemaField('source_doi', 'STRING', mode='NULLABLE', description='The DOI of a wiki page that describes the original collection or analysis result that includes this instance'),
    bigquery.SchemaField('license_url', 'STRING', mode='NULLABLE', description='URL of license of this analysis result'),
    bigquery.SchemaField('license_long_name', 'STRING', mode='NULLABLE', description='Long name of license of this analysis result'),
    bigquery.SchemaField('license_short_name', 'STRING', mode='NULLABLE', description='Short name of license of this analysis result'),
]
