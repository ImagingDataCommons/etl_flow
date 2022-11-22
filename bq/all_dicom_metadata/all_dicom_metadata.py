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

# Generate a table of dicom metadata across all IDC versions
# Obsolete??
import argparse
import os
import json
from google.cloud import bigquery
import settings
from utilities.logging_config import successlogger, progresslogger, errlogger
from utilities.bq_helpers import query_BQ


def populate_current_version(args, write_disposition, schema_update_options=None):
    version = settings.CURRENT_VERSION
    pub_suffix = '_pub' if version >= 8 else ''
    client = bigquery.Client(project=settings.DEV_PROJECT)
    query = f"""
     WITH all_uuids AS (
     SELECT 
     c.uuid AS collection_uuid,
     c.rev_idc_version AS collection_rev_idc_version,
     {settings.CURRENT_VERSION} AS collection_final_idc_version,
     p.uuid AS patient_uuid,
     p.rev_idc_version AS patient_rev_idc_version,
     {settings.CURRENT_VERSION} AS patient_final_idc_version,
     st.uuid AS study_uuid,
     st.rev_idc_version AS study_rev_idc_version,
     {settings.CURRENT_VERSION} AS study_final_idc_version,
     se.uuid AS series_uuid,
     se.rev_idc_version AS series_rev_idc_version,
     {settings.CURRENT_VERSION} AS series_final_idc_version,
     i.uuid AS instance_uuid,
     i.rev_idc_version AS instance_rev_idc_version,
     {settings.CURRENT_VERSION} AS instance_final_idc_version,
     i.sop_instance_uid as SOPInstanceUID
    FROM `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.version` v
      JOIN `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.version_collection` vc ON v.version = vc.version
      JOIN `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.collection` c ON vc.collection_uuid = c.uuid
      JOIN `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.collection_patient` cp ON c.uuid = cp.collection_uuid
      JOIN `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.patient` p ON cp.patient_uuid = p.uuid
      JOIN `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.patient_study` ps ON p.uuid = ps.patient_uuid
      JOIN `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.study` st ON ps.study_uuid = st.uuid
      JOIN `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.study_series` ss ON st.uuid = ss.study_uuid
      JOIN `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.series` se ON ss.series_uuid = se.uuid
      JOIN `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.series_instance` si ON se.uuid = si.series_uuid
      JOIN `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.instance` i ON si.instance_uuid = i.uuid
      JOIN `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.all_included_collections` aic
      ON c.collection_id=aic.tcia_api_collection_id
      WHERE i.excluded=FALSE
      AND v.version = {settings.CURRENT_VERSION}
    )
    SELECT au.* except(SOPInstanceUID), dm.*
    FROM all_uuids au
    JOIN `idc-dev-etl.idc_v{version}{pub_suffix}.dicom_metadata` dm
    ON au.SOPInstanceUID = dm.SOPInstanceUID
    """

    result = query_BQ(client, args.dataset_name, args.table_name, query, write_disposition, schema_update_options)
    return result


def populate_version(args, version, write_disposition, schema_update_options=None):
    pub_suffix = '_pub' if version >= 8 else ''
    client = bigquery.Client(project=settings.DEV_PROJECT)
    query = f"""
     WITH all_uuids AS (
     SELECT 
     c.uuid AS collection_uuid,
     c.rev_idc_version AS collection_rev_idc_version,
     IF(c.final_idc_version != 0, c.final_idc_version, {settings.CURRENT_VERSION}) AS collection_final_idc_version,
     p.uuid AS patient_uuid,
     p.rev_idc_version AS patient_rev_idc_version,
     IF(p.final_idc_version != 0, p.final_idc_version, {settings.CURRENT_VERSION}) AS patient_final_idc_version,
     st.uuid AS study_uuid,
     st.rev_idc_version AS study_rev_idc_version,
     IF(st.final_idc_version != 0, st.final_idc_version, {settings.CURRENT_VERSION}) AS study_final_idc_version,
     se.uuid AS series_uuid,
     se.rev_idc_version AS series_rev_idc_version,
     IF(se.final_idc_version != 0, se.final_idc_version, {settings.CURRENT_VERSION}) AS series_final_idc_version,
     i.uuid AS instance_uuid,
     i.rev_idc_version AS instance_rev_idc_version,
     IF(i.final_idc_version != 0, i.final_idc_version, {settings.CURRENT_VERSION}) AS instance_final_idc_version,
     i.sop_instance_uid as SOPInstanceUID
    FROM `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.version` v
      JOIN `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.version_collection` vc ON v.version = vc.version
      JOIN `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.collection` c ON vc.collection_uuid = c.uuid
      JOIN `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.collection_patient` cp ON c.uuid = cp.collection_uuid
      JOIN `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.patient` p ON cp.patient_uuid = p.uuid
      JOIN `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.patient_study` ps ON p.uuid = ps.patient_uuid
      JOIN `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.study` st ON ps.study_uuid = st.uuid
      JOIN `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.study_series` ss ON st.uuid = ss.study_uuid
      JOIN `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.series` se ON ss.series_uuid = se.uuid
      JOIN `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.series_instance` si ON se.uuid = si.series_uuid
      JOIN `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.instance` i ON si.instance_uuid = i.uuid
      JOIN `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.all_included_collections` aic
      ON c.collection_id=aic.tcia_api_collection_id
      WHERE i.excluded=FALSE
      AND v.version = {version}
      AND ( 
        i.final_idc_version={version} OR 
        se.final_idc_version={version} OR
        st.final_idc_version={version} OR
        p.final_idc_version={version} OR
        c.final_idc_version={version})
    )
    SELECT au.* except(SOPInstanceUID), dm.*
    FROM all_uuids au
    JOIN `idc-dev-etl.idc_v{version}{pub_suffix}.dicom_metadata` dm
    ON au.SOPInstanceUID = dm.SOPInstanceUID
    """

    result = query_BQ(client, args.dataset_name, args.table_name, query, write_disposition, schema_update_options)
    return result



def gen_all_versions(args):
    # Start by creating a table with all the metadata in the latest IDC version
    populate_current_version(args, write_disposition= 'WRITE_TRUNCATE')

    # For each previous IDC version, add the metadata of instances that retired in that version
    for version in range(settings.CURRENT_VERSION-1,0,-1):
        # Get a list of the retired instances
        result = populate_version(args, version, write_disposition= 'WRITE_APPEND',
                                  schema_update_options='ALLOW_FIELD_ADDITION')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset_name', default='all_versions_whc')
    parser.add_argument('--table_name', default='all_dicom_metadata')
    args = parser.parse_args()
    print(f'args: {json.dumps(args.__dict__, indent=2)}')
    gen_all_versions(args)
