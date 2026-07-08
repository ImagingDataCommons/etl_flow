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

import sys
import argparse
from google.cloud  import bigquery
import settings


# This script generates a many-to-one DB table hierarchy. Similar to the idc_*
# hierarchy except:
# - It includes both TCIA and IDC sourced data


# # Created a flattened table from all_joined in which
# # each row was the name/ID of the corresponding analysis_results or original_data_source
# # Unlike all_joined_current_and_public, the table includes excluded instances but not redacted instances.
# def gen_all_pre_data(client):
#     query = f"""
# CREATE OR REPLACE TABLE `idc-dev-etl.idc_v0_dev.all_pre_data` AS
# SELECT
# *
# FROM `idc-dev-etl.idc_v0_dev.all_joined`
# WHERE Access="Public" AND idc_version=24 AND i_redacted=False AND metadata_sunset=0
# """
#     query_job = client.query(query)
#     query_job.result()
#     return

# Create a sequence of tables, each a subset of data from all_sources_data
# that is specific to a level of the hierarchy
def gen_all_sources_xxx_data(args, client):
    query = f"""
BEGIN
    CREATE OR REPLACE TABLE `idc-dev-etl.idc_v{args.version}_dev.pre_instance`
    AS
    SELECT DISTINCT
      SOPInstanceUID,
      SeriesInstanceUID,
      i_hash `hash`,
      ingestion_url,
      size size,
      i_excluded excluded,
      idc_version,
      mitigation,
      source_file_hash
    FROM `idc-dev-etl.idc_v{args.version}_dev.{args.data_snapshot}`
    ;
    
    CREATE OR REPLACE TABLE `idc-dev-etl.idc_v{args.version}_dev.pre_series` AS
    WITH series AS (
    SELECT DISTINCT
        SeriesInstanceUID,
        StudyInstanceUID,
        se_excluded excluded, 
        source_doi,
        source_url,
        versioned_source_doi,
        versioned_source_url,
        analysis_result
    FROM `idc-dev-etl.idc_v{args.version}_dev.all_data_snapshot`
    ),
    hashes AS (
    SELECT DISTINCT
        SeriesInstanceUID,
        `hash`
    FROM `idc-dev-etl.idc_v{args.version}_dev.pre_instance` i
    )
    SELECT DISTINCT
        series.*,
        TO_HEX(MD5(STRING_AGG(DISTINCT `hash`, '' ORDER BY `hash`))) `hash`,
    FROM series
    JOIN hashes
    ON series.seriesinstanceuid = hashes.seriesinstanceuid 
    GROUP BY 
        series.SeriesInstanceUID,
        StudyInstanceUID,
        excluded, 
        source_doi,
        source_url,
        versioned_source_doi,
        versioned_source_url,
        analysis_result
    ;
    
    CREATE OR REPLACE TABLE `idc-dev-etl.idc_v{args.version}_dev.pre_study` AS
    WITH studies AS (
    SELECT DISTINCT
        studyInstanceUID,
        collection_name,
        collection_id,
        patientID
    FROM `idc-dev-etl.idc_v{args.version}_dev.all_data_snapshot`
    ),
    hashes AS (
    SELECT DISTINCT
        StudyInstanceUID,
        `hash`
    FROM `idc-dev-etl.idc_v{args.version}_dev.pre_series` se
    )
    SELECT DISTINCT
        studies.*,
        TO_HEX(MD5(STRING_AGG(DISTINCT `hash`, '' ORDER BY `hash`))) `hash`
    FROM studies
    JOIN  hashes
    ON studies.studyinstanceuid = hashes.studyinstanceuid 
    GROUP BY 
        collection_name,
        collection_id,
        StudyInstanceUID,
        patientID
    ;
    
    CREATE OR REPLACE TABLE `idc-dev-etl.idc_v{args.version}_dev.pre_patient` AS
    WITH patients AS (
    SELECT DISTINCT
        collection_name,
        collection_id,
        patientID
    FROM `idc-dev-etl.idc_v{args.version}_dev.all_data_snapshot`
    ),
    hashes AS (
    SELECT DISTINCT
         collection_name,
         collection_id,
         patientID,
        `hash`
    FROM `idc-dev-etl.idc_v{args.version}_dev.pre_study`
    )
    SELECT DISTINCT
        patients.*,
        TO_HEX(MD5(STRING_AGG(DISTINCT `hash`, '' ORDER BY `hash`))) `hash`  
    FROM patients
    JOIN Hashes
    ON patients.patientID = hashes.patientID AND patients.collection_name = hashes.collection_name
    GROUP BY 
        patientID,
        collection_name,
        collection_id
   ;
    
    CREATE OR REPLACE TABLE `idc-dev-etl.idc_v{args.version}_dev.pre_collection` AS
    WITH collections AS (
    SELECT DISTINCT
        collection_name,
        collection_id
    FROM `idc-dev-etl.idc_v{args.version}_dev.all_data_snapshot`
    ),
    hashes AS (
    SELECT DISTINCT
        collection_name,
        collection_id,
        `hash`
    FROM `idc-dev-etl.idc_v{args.version}_dev.pre_patient` st
    )
   SELECT DISTINCT
        collections.*,
        TO_HEX(MD5(STRING_AGG(DISTINCT `hash`, '' ORDER BY `hash`))) `hash`
    FROM collections
    JOIN  hashes
    ON collections.collection_name = hashes.collection_name
    GROUP BY 
        collection_name,
        collection_id
   ;
END;
"""
    query_job = client.query(query)
    query_job.result()
    return

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--version", default=settings.CURRENT_VERSION)
    parser.add_argument('--data_snapshot', default='all_data_snapshot', help='BQ table, flattened snapshot hierarchy')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)

    client = bigquery.Client()
    gen_all_sources_xxx_data(args, client)
