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

# This is the schema for the collections_metadata BQ table
from google.cloud import bigquery

data_collections_metadata_schema = [
    bigquery.SchemaField('collection_name', 'STRING', mode='NULLABLE', description='Collection name as used externally by IDC webapp'),
    bigquery.SchemaField('collection_id', 'STRING', mode='NULLABLE', description='Collection ID as used internally by IDC webapp'),
    bigquery.SchemaField('Access', 'STRING', mode='REPEATED', description='Limited or Public'),
    bigquery.SchemaField('source_doi','STRING', mode='NULLABLE', description='DOI that can be resolved at doi.org to a wiki page'),
    bigquery.SchemaField('source_url','STRING', mode='NULLABLE', description='URL of collection information page'),
    bigquery.SchemaField('CancerTypes','STRING', mode='NULLABLE', description='Cancer type of this collection '),
    bigquery.SchemaField('TumorLocations','STRING', mode='NULLABLE', description='Body location that was studied'),
    bigquery.SchemaField('Subjects', 'INTEGER', mode='NULLABLE', description='Number of subjects in collection'),
    bigquery.SchemaField('Species', 'STRING', mode='NULLABLE', description="Species of collection subjects"),
    bigquery.SchemaField('ImageTypes', 'STRING', mode='NULLABLE', description='Enumeration of image type/modality'),
    bigquery.SchemaField('SupportingData', 'STRING', mode='NULLABLE', description='Type(s) of addional available data'),
    bigquery.SchemaField('Program', 'STRING', mode='NULLABLE', description='Program to which this collection belongs'),
    bigquery.SchemaField('Status', 'STRING', mode='NULLABLE', description='Collection status: Ongoing or Complete'),
    bigquery.SchemaField('Updated', 'DATE', mode='NULLABLE', description='Date of ost recent update'),
    bigquery.SchemaField(
        "licenses",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField('license_url', 'STRING', mode='NULLABLE',
                                 description='URL of license of this analysis result'),
            bigquery.SchemaField('license_long_name', 'STRING', mode='NULLABLE',
                                 description='Long name of license of this analysis result'),
            bigquery.SchemaField('license_short_name', 'STRING', mode='NULLABLE',
                                 description='Short name of license of this analysis result')
        ],
    ),
    bigquery.SchemaField('Description', 'STRING', mode='NULLABLE', description='Description of collection (HTML format)'),
    bigquery.SchemaField('DOI', 'STRING', mode='NULLABLE',
                         description='DEPRECATED: Duplicate of source_doi'),
    bigquery.SchemaField('URL', 'STRING', mode='NULLABLE', description='DEPRECATED: Duplicate of source_url'),
    bigquery.SchemaField('CancerType', 'STRING', mode='NULLABLE', description='DEPRECATED: Duplicate of CancerTypes '),
    bigquery.SchemaField('Location', 'STRING', mode='NULLABLE',
                         description='DEPRECATED: Duplicate of TumorLocations'),
]