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
    bigquery.SchemaField('collection_name', 'STRING', mode='REQUIRED', description='Collection name as used externally by IDC webapp'),
    bigquery.SchemaField('collection_id', 'STRING', mode='REQUIRED', description='Collection ID as used internally by IDC webapp'),
    bigquery.SchemaField('collection_title', 'STRING', mode='REQUIRED',
                         description='Descriptive title of this collection'),
    bigquery.SchemaField('cancer_types', 'STRING', mode='REQUIRED', description='Cancer types in this collection '),
    bigquery.SchemaField('tumor_locations', 'STRING', mode='REQUIRED',
                         description='Tumor locations in this collection'),
    bigquery.SchemaField('subjects', 'INTEGER', mode='REQUIRED', description='Number of subjects in this collection'),
    bigquery.SchemaField('species', 'STRING', mode='REQUIRED', description="Species of collection subjects"),
    bigquery.SchemaField(
        "sources",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField('source_id', 'STRING', mode='NULLABLE', description='collection_id or analysis_result_id of this source'),
            bigquery.SchemaField('source_type', 'STRING', mode='NULLABLE', description='"original collection" or "analysis result"'),
            bigquery.SchemaField('source_doi', 'STRING', mode='NULLABLE',
                                 description='DOI that can be resolved at doi.org to a information page of this source'),
            bigquery.SchemaField('source_url', 'STRING', mode='REQUIRED',
                                 description='URL of the information page of this sourc'),
            bigquery.SchemaField('modalities', 'STRING', mode='NULLABLE',
                                 description='URL of the information page of this source'),
            bigquery.SchemaField(
                "license",
                "RECORD",
                fields=[
                    bigquery.SchemaField('license_url', 'STRING', mode='REQUIRED',
                                         description='URL of license of this (sub)collection'),
                    bigquery.SchemaField('license_long_name', 'STRING', mode='REQUIRED',
                                         description='Long name of license of this (sub)collection'),
                    bigquery.SchemaField('license_short_name', 'STRING', mode='REQUIRED',
                                         description='Short name of license of this (sub)collection')
                ]
            ),
            bigquery.SchemaField('citation', 'STRING', mode='NULLABLE',
                                 description='Citation to be used for this source'),
            bigquery.SchemaField('access', 'STRING', mode='NULLABLE', description='DEPRECATED: All IDC data is public'),
            bigquery.SchemaField('ImageTypes', 'STRING', mode='NULLABLE',
                                 description='DEPRECATED: Duplicate of modalities'),
        ],
        description='Array of metadata for each source of instance data in this collection'
    ),
    bigquery.SchemaField('supporting_data', 'STRING', mode='NULLABLE', description='Type(s) of addional available data'),
    bigquery.SchemaField('program', 'STRING', mode='REQUIRED', description='Program to which this collection belongs'),
    bigquery.SchemaField('status', 'STRING', mode='NULLABLE', description='Collection status: Ongoing or Complete'),
    bigquery.SchemaField('updated', 'DATE', mode='NULLABLE', description='Date of most recent update'),
    bigquery.SchemaField('description', 'STRING', mode='REQUIRED', description='Description of collection (HTML format)'),
    # Deprecations
    bigquery.SchemaField('Title', 'STRING', mode='REQUIRED',
                         description='Deprecated: Duplicate of collection_title'),
    bigquery.SchemaField('CancerTypes', 'STRING', mode='REQUIRED', description='DEPRECATED: Duplicate of cancer_types'),
    bigquery.SchemaField('TumorLocations', 'STRING', mode='REQUIRED',
                         description='DEPRECATED: Duplicate of tumor_locations'),
    bigquery.SchemaField('SupportingData', 'STRING', mode='NULLABLE',
                         description='DEPRECATED: Duplicate of supporting_data'),
]