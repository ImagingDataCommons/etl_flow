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

import pandas as pd
import argparse
import json
from google.cloud  import bigquery, storage
import settings
from utilities.logging_config import successlogger, progresslogger, errlogger

# *Obsolete*
# This script generates a many-to-one DB table hierarchy. Similar to the idc_*
# hierarchy except:
# - It includes both TCIA and IDC sourced data
# - The top level is 'source' rather than 'collection'. I.E. top level entities
#   are analysis_results and original_data.

# Created a flattened table from all_joined in which
# each row was the name/ID of the corresponding analysis_results or original_data_source
# Unlike all_joined_current_and_public, the table includes excluded instances but not redacted instances.
def gen_all_sources_data(client):
    query = f"""
CREATE OR REPLACE TABLE `idc-dev-etl.idc_v0_dev.all_sources_data` AS

WITH source_types as (
SELECT srcs.source_doi, IF(srcs.modalities LIKE '%SM%', "pathology", "radiology") source_type 
FROM `idc-dev-etl.idc_v24_pub.original_collections_metadata`, unnest(sources) srcs 
)

SELECT 
arm.analysis_result_name source_name, 
arm.analysis_result_id source_id, 
"analysis_result" source_type, 
ajpac.*
FROM `idc-dev-etl.idc_v0_dev.all_joined` ajpac
JOIN `idc-dev-etl.idc_v24_pub.analysis_results_metadata` arm
ON ajpac.source_doi = arm.source_doi
WHERE ajpac.Access="Public" AND idc_version=24 AND i_redacted=False AND metadata_sunset=0
UNION ALL
SELECT 
CONCAT(ajpac.collection_id, '_', source_types.source_type) source_name,  
REPLACE(REPLACE(LOWER(CONCAT(ajpac.collection_id, '_', source_types.source_type)), '-', '_'), ' ', '_') source_id, 
"original_data" source_type, 
ajpac.*
FROM `idc-dev-etl.idc_v0_dev.all_joined` ajpac
LEFT JOIN `idc-dev-etl.idc_v24_pub.analysis_results_metadata` arm
ON ajpac.source_doi = arm.source_doi
JOIN source_types
ON ajpac.source_doi = source_types.source_doi
WHERE ajpac.Access="Public" AND idc_version=24 AND i_redacted=False AND metadata_sunset=0 AND
    arm.source_doi IS NULL 
ORDER BY source_name
"""
    query_job = client.query(query)
    query_job.result()
    return

# Create a sequence of tables, each a subset of data from all_sources_data
# that is specific to a level of the hierarchy
def gen_all_sources_xxx_data(client):
    query = f"""
CREATE OR REPLACE TABLE `idc-dev-etl.idc_v0_dev.pre_instance`
AS
SELECT
  sop_instance_uid SOPInstanceUID,
  series_instance_uid SeriesInstanceUID,
  i_hash  `hash`,
  ingestion_url,
  i_size size,
  i_excluded excluded,
  i_init_idc_version idc_version,
  i_redacted redacted,
  mitigation,
FROM `idc-dev-etl.idc_v0_dev.all_sources_data`;

CREATE OR REPLACE TABLE `idc-dev-etl.idc_v0_dev.pre_series` AS
WITH distinct_data AS (
SELECT DISTINCT
source_name,
source_id,
series_instance_uid SeriesInstanceUID,
study_instance_uid StudyInstanceUID,
collection_id collection_name,
REPLACE(REPLACE(LOWER(collection_id),'-','_'),' ','_') collection_id,
se_excluded excluded, 
source_doi,
source_url,
analysis_result,
se_redacted redacted,
mitigation,
versioned_source_doi
FROM `idc-dev-etl.idc_v0_dev.all_sources_data`)
SELECT DISTINCT
dd.*,
TO_HEX(md5(string_agg(pre.hash, "" ORDER BY pre.hash))) `hash`
FROM distinct_data dd
JOIN `idc-dev-etl.idc_v0_dev.pre_instance` pre
ON dd.SeriesInstanceUID = pre.SeriesInstanceUID
GROUP BY source_name, source_id, SeriesInstanceUID, StudyInstanceUID, collection_name, collection_id, excluded, source_doi, source_url, analysis_result, redacted, mitigation, versioned_source_doi
;

CREATE OR REPLACE TABLE `idc-dev-etl.idc_v0_dev.pre_study` AS
WITH distinct_data AS (
SELECT DISTINCT
study_instance_uid StudyInstanceUID,
submitter_case_id patientID,
source_doi,
source_name,
source_id,
st_redacted redacted
FROM `idc-dev-etl.idc_v0_dev.all_sources_data`)
SELECT DISTINCT
dd.*,
TO_HEX(md5(string_agg(pre.hash, "" ORDER BY pre.hash))) `hash`
FROM distinct_data dd
JOIN `idc-dev-etl.idc_v0_dev.pre_series` pre
ON dd.StudyInstanceUID = pre.StudyInstanceUID AND dd.source_doi = pre.source_doi
GROUP BY source_name, source_id, StudyInstanceUID, patientID, redacted, source_doi
;

CREATE OR REPLACE TABLE `idc-dev-etl.idc_v0_dev.pre_patient` AS
WITH distinct_data AS (
SELECT DISTINCT
submitter_case_id patientID,
source_name,
source_id,
source_doi,
p_redacted redacted,
FROM `idc-dev-etl.idc_v0_dev.all_sources_data`)
SELECT DISTINCT
dd.*,
TO_HEX(md5(string_agg(pre.hash, "" ORDER BY pre.hash))) `hash`
FROM distinct_data dd
JOIN `idc-dev-etl.idc_v0_dev.pre_study` pre
ON dd.patientID = pre.patientID AND dd.source_doi = pre.source_doi
GROUP BY source_name, source_id, patientID, redacted, source_doi
;

CREATE OR REPLACE TABLE `idc-dev-etl.idc_v0_dev.pre_source` AS
WITH distinct_data AS (
SELECT DISTINCT
source_doi, 
source_name,
source_id, 
c_redacted redacted,
FROM `idc-dev-etl.idc_v0_dev.all_sources_data`)
SELECT DISTINCT
dd.*,
TO_HEX(md5(string_agg(pre.hash, "" ORDER BY pre.hash))) `hash`
FROM distinct_data dd
JOIN `idc-dev-etl.idc_v0_dev.pre_patient` pre
ON dd.source_doi = pre.source_doi
GROUP BY source_name, source_id, redacted, source_doi
;
"""
    query_job = client.query(query)
    query_job.result()
    return

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    client = bigquery.Client()
    gen_all_sources_data(client)
    gen_all_sources_xxx_data(client)
