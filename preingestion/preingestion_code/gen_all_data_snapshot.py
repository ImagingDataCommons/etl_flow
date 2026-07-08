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

# Generate the initial all_data_snapshot table.
from google.cloud import bigquery

query = """
    BEGIN
    
    CREATE TABLE `idc-dev-etl.idc_v0_dev.all_data_snapshot` AS (
    SELECT 
     collection_id collection_name, 
     REPLACE(REPLACE(LOWER(collection_id), '-', '_'), ' ', '_') collection_id, 
     c_hashes.all_hash c_hash,
     submitter_case_id patientID, 
     p_hashes.all_hash p_hash,
     study_instance_uid StudyInstanceUID, 
     st_hashes.all_hash st_hash,
     series_instance_uid SeriesInstanceUID, 
     se_hashes.all_hash se_hash, 
     se_excluded,
     source_doi, 
     source_url,
     versioned_source_doi,
     versioned_source_url,
     analysis_result, 
     sop_instance_uid SOPInstanceUID, 
     i_hash, 
     ingestion_url,
     i_size ,
     i_excluded, 
     idc_version,
     mitigation,
     "" source_file_hash
    FROM `idc-dev-etl.idc_v0_dev.all_joined` aj
    WHERE Access='Public'
    AND i_redacted=FALSE 
    AND idc_version=24
    AND metadata_sunset=0
    );
END;
    
    """
client = bigquery.Client()
query_job = client.query(query)
query_job.result()