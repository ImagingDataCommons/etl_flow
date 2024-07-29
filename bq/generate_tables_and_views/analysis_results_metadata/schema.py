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

analysis_results_metadata_schema = [
    bigquery.SchemaField('ID', 'STRING', mode='REQUIRED', description='Results ID'),
    bigquery.SchemaField('Title', 'STRING', mode='REQUIRED', description='Descriptive title'),
    bigquery.SchemaField('Access', 'STRING', mode='REQUIRED', description='Limited or Public'),
    bigquery.SchemaField('source_doi','STRING', mode='NULLABLE', description='DOI that can be resolved at doi.org to a wiki page'),
    bigquery.SchemaField('source_url','STRING', mode='REQUIRED', description='URL of a wiki page'),
    bigquery.SchemaField('CancerTypes','STRING', mode='REQUIRED', description='Type(s) of cancer analyzed'),
    bigquery.SchemaField('TumorLocations', 'STRING', mode='REQUIRED', description='Body location that was analyzed'),
    bigquery.SchemaField('Subjects', 'INTEGER', mode='REQUIRED', description='Number of subjects whose data was analyzed'),
    bigquery.SchemaField('Collections', 'STRING', mode='REQUIRED', description='collection_names of original data collections analyzed'),
    bigquery.SchemaField('AnalysisArtifacts', 'STRING', mode='REQUIRED', description='Types of analysis artifacts produced'),
    bigquery.SchemaField('Updated', 'DATE', mode='REQUIRED', description='Most recent update reported by TCIA'),
    bigquery.SchemaField('license_url', 'STRING', mode='REQUIRED', description='URL of license of this analysis result'),
    bigquery.SchemaField('license_long_name', 'STRING', mode='REQUIRED', description='Long name of license of this analysis result'),
    bigquery.SchemaField('license_short_name', 'STRING', mode='REQUIRED', description='Short name of license of this analysis result'),
    bigquery.SchemaField('Description', 'STRING', mode='REQUIRED',
                         description='Analysis result description'),
    bigquery.SchemaField('Citation', 'STRING', mode='NULLABLE',
                         description='Citation to be used for this source'),
    bigquery.SchemaField('AnalysisArtifactsonTCIA', 'STRING', mode='REQUIRED',
                         description='DEPRECATED: Duplicate of AnalysisArtifacts'),
    bigquery.SchemaField('DOI', 'STRING', mode='REQUIRED',
                         description='DEPRECATED: Duplicate of source_doi'),
    bigquery.SchemaField('CancerType', 'STRING', mode='REQUIRED', description='DEPRECATED: Duplicate of CancerTypes'),
    bigquery.SchemaField('Location', 'STRING', mode='REQUIRED', description='DEPRECATED: Duplicate of PrimarySiteLocations'),

]
