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

# Rename xxx_view views to xxx
import google.api_core.exceptions

import settings
import argparse
import json
import time
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import NotFound
from utilities.logging_config import successlogger, progresslogger, errlogger



def revise_dicom_all_view_schema(args, dones):
    # idc_v13 has both views and tables, so we don't rename xxx_view to xxx
    if int(args.dataset_version) <13:
        progresslogger.info(f'revise_dicom_all_view_schema_{args.trg_dataset}')
        return
    if f'revise_dicom_all_view_schema_{args.trg_dataset}' not in dones:
        client = bigquery.Client()
    view_id = 'dicom_all_view'
    try:
        view = client.get_table(f'{args.src_project}.{args.src_dataset}.{view_id}')
    except google.api_core.exceptions.NotFound:
        progresslogger.info(f'Skipping revise_dicom_all_view_schema{args.trg_dataset}_view_id')
        return
    new_view_id = f'{args.trg_project}.{args.trg_dataset}.{view_id}'

    new_view = bigquery.Table(new_view_id)
    # new_view.view_query = view.view_query.replace(args.src_dataset,args.trg_dataset) \
    #     .replace(args.src_project,args.trg_project)
    query = view.view_query
    query = query.replace(
        '    aux.aws_url as aws_url,\n',
        ''
    )
    query = query.replace(
        '    dcm.* except(PatientID, StudyInstanceUID, SeriesInstanceUID, SOPInstanceUID)\n',
        '    dcm.* except(PatientID, StudyInstanceUID, SeriesInstanceUID, SOPInstanceUID),\n    aux.aws_url as aws_url\n',
    )

    aws_schema = view.schema
    if not aws_schema[32].name == 'aws_url':
        errlogger.error('Did not find aws_url')
        return
    else:
        aws_schema.pop(32)
    aws_field = SchemaField(
            'aws_url',
            field_type='STRING',
            mode = 'NULLABLE',
            description = 'URL to this object containing the current version of this instance in Amazon Web Services (AWS)'
        )
    aws_schema.append(aws_field)

    new_view.view_query = query
    new_view.friendly_name = view.friendly_name
    new_view.description = view.description
    new_view.labels = view.labels
    # Delete the existing view

    client.delete_table(new_view_id, not_found_ok=True)
    installed_view = client.create_table(new_view)

    # For whatever reason, in at least one case, a field in the installed_view
    # schema is missing from the src_view schema. We add those missing fields.
    # installed_view.schema = add_missing_fields_to(view.schema, installed_view.schema)
    installed_view.schema = aws_schema

    # # Update the schema after creating the view
    # installed_view.schema = view.schema
    client.update_table(installed_view,['schema'])

    successlogger.info(f'revise_dicom_all_view_schema_{args.trg_dataset}')
    pass

# args.src_project: idc-pdp-staging
# args.src_dataset: idc_vX
# args.trg_project: idc_pdp_staging
# args.trg_dataset: idc_vX
# def revise_dicom_all_view_schema(args, dones ):
#     # idc_v13 has both views and tables, so we don't rename xxx_view to xxx
#     if int(args.dataset_version) <13:
#         progresslogger.info(f'revise_dicom_all_view_schema_{args.trg_dataset}')
#         return
#     if f'rename_views_{args.trg_dataset}' not in dones:
#         client = bigquery.Client()
#         # client = bigquery.Client(project=args.trg_project)
#         # src_dataset_ref = bigquery.DatasetReference(args.src_project, args.src_dataset)
#         # src_dataset = client.get_dataset(src_dataset_ref)
#
#         for table_id in [
#             'dicom_all_view',
#             'dicom_metadata_curated_view',
#             'measurement_groups_view',
#             'qualitative_measurements_view',
#             'quantitative_measurements_view',
#             'segmentations_view']:
#             rename_view(client, args, table_id)
#         successlogger.info(f'rename_views_{args.trg_dataset}')
#     else:
#         progresslogger.info(f'Skipping rename_views_{args.trg_dataset}')
