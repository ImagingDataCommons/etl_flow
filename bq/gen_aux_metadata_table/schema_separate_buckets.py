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

# This is the schema for the idc_tcia_collections_metadata BQ table
from google.cloud import bigquery


auxiliary_metadata_schema = """
    SELECT
      {version} AS idc_version_number,
      v.max_timestamp as max_timestamp,
      v.min_timestamp as min_timestamp,
      v.hashes AS version_hashes,
      c.collection_id AS tcia_api_collection_id,
      REPLACE(REPLACE(LOWER(c.collection_id),'-','_'), ' ','_') AS idc_webapp_collection_id,
      se.source_doi AS source_doi,
      c.hashes AS collection_hashes,
      p.submitter_case_id AS submitter_case_id,
      p.idc_case_id AS idc_case_id,
      p.hashes AS patient_hashes,
      st.study_instance_uid AS StudyInstanceUID,
      st.uuid AS study_uuid,
      st.study_instances AS study_instances,
      st.hashes AS study_hashes,
      se.series_instance_uid AS SeriesInstanceUID,
      se.uuid AS series_uuid,
      se.series_instances AS series_instances,
      se.hashes AS series_hashes,
      i.sop_instance_uid AS SOPInstanceUID,
      i.uuid AS instance_uuid,
      CONCAT('gs://', 
      if (i.source is NULL, '{gcs_bucket}', CONCAT('idc_v3_', REPLACE(REPLACE(LOWER(c.collection_id),'-','_'), ' ','_'))), '/', i.uuid,'.dcm') AS gcs_url,
      if (i.source is NULL, '{gcs_bucket}', CONCAT('idc_v3_', REPLACE(REPLACE(LOWER(c.collection_id),'-','_'), ' ','_'))) AS gcs_bucket,
      i.size AS instance_size,
      i.hash AS instance_hash,
    FROM
      `{project}.{dataset}.version` AS v
    JOIN
      `{project}.{dataset}.collection` AS c
    ON
        1=1
    JOIN
      `{project}.{dataset}.patient` AS p
    ON
      c.collection_id = p.collection_id
    JOIN
      `{project}.{dataset}.study` AS st
    ON
      p.submitter_case_id = st.submitter_case_id
    JOIN
      `{project}.{dataset}.series` AS se
    ON
      st.study_instance_uid = se.study_instance_uid
    JOIN
      `{project}.{dataset}.instance` AS i
    ON
      se.series_instance_uid = i.series_instance_uid
    LEFT JOIN 
      `{project}.{dataset}.excluded_collections` as ex
    ON 
      LOWER(c.collection_id) = LOWER(ex.tcia_api_collection_id)
    WHERE ex.tcia_api_collection_id IS NULL"""