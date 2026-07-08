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

# This script generates a many-to-one DB table hierarchy. Similar to the idc_*
# hierarchy except:
# - It includes both TCIA and IDC sourced data


# Created a flattened table from all_joined in which
# each row was the name/ID of the corresponding analysis_results or original_data_source
# Unlike all_joined_current_and_public, the table includes excluded instances but not redacted instances.
def gen_all_pre_data(client):
    query = f"""
CREATE OR REPLACE TABLE `idc-dev-etl.idc_v0_dev.all_pre_data` AS
SELECT 
*
FROM `idc-dev-etl.idc_v0_dev.all_joined`
WHERE Access="Public" AND idc_version=24 AND i_redacted=False AND metadata_sunset=0
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
  mitigation,
  ""  source_file_hash
FROM `idc-dev-etl.idc_v0_dev.all_pre_data`
;

CREATE OR REPLACE TABLE `idc-dev-etl.idc_v0_dev.pre_series` AS
SELECT DISTINCT
series_instance_uid SeriesInstanceUID,
study_instance_uid StudyInstanceUID,
se_hashes.all_hash `hash`,
se_excluded excluded, 
source_doi,
source_url,
analysis_result,
versioned_source_doi
FROM `idc-dev-etl.idc_v0_dev.all_pre_data`
;

CREATE OR REPLACE TABLE `idc-dev-etl.idc_v0_dev.pre_study` AS
SELECT DISTINCT
study_instance_uid StudyInstanceUID,
REPLACE(REPLACE(LOWER(collection_id),'-','_'),' ','_') collection_id,
submitter_case_id patientID,
st_hashes.all_hash `hash`,
FROM `idc-dev-etl.idc_v0_dev.all_pre_data`
;

CREATE OR REPLACE TABLE `idc-dev-etl.idc_v0_dev.pre_patient` AS
SELECT DISTINCT
REPLACE(REPLACE(LOWER(collection_id),'-','_'),' ','_') collection_id,
submitter_case_id patientID,
p_hashes.all_hash `hash`
FROM `idc-dev-etl.idc_v0_dev.all_pre_data`
;

CREATE OR REPLACE TABLE `idc-dev-etl.idc_v0_dev.pre_collection` AS
SELECT DISTINCT
collection_id collection_name,
REPLACE(REPLACE(LOWER(collection_id),'-','_'),' ','_') collection_id,
c_hashes.all_hash `hash`
FROM `idc-dev-etl.idc_v0_dev.all_pre_data`
;
"""
    query_job = client.query(query)
    query_job.result()
    return

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    client = bigquery.Client()
    gen_all_pre_data(client)
    gen_all_sources_xxx_data(client)
