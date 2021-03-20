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


auxilliary_metadata_schema = """
    SELECT
      v.idc_version_number AS idc_version_number,
      v.idc_version_timestamp AS idc_version_timestamp,
      c.tcia_api_collection_id AS tcia_api_collection_id,
      REPLACE(REPLACE(LOWER(c.tcia_api_collection_id),'-','_'), ' ','_') AS idc_webapp_collection_id,
      se.source_doi AS source_doi,
      c.collection_timestamp AS collection_timestamp,
      p.submitter_case_id AS submitter_case_id,
      p.crdc_case_id AS crdc_case_id,
      p.patient_timestamp AS patient_timestamp,
      st.study_instance_uid AS StudyInstanceUID,
      st.study_uuid AS study_uuid,
      st.study_instances AS study_instances,
      st.study_timestamp AS study_timestamp,
      se.series_instance_uid AS SeriesInstanceUID,
      se.series_uuid AS series_uuid,
      se.series_instances AS series_instances,
      se.series_timestamp AS series_timestamp,
      i.sop_instance_uid AS SOPInstanceUID,
      i.instance_uuid AS instance_uuid,
      CONCAT(i.gcs_url,'.dcm') AS gcs_url,
      SPLIT(i.gcs_url,'/')[OFFSET(2)] AS gcs_bucket,
      i.instance_hash AS md5_hash,
      i.instance_size AS instance_size,
      i.instance_timestamp AS instance_timestamp
    FROM
      `{project}.{dataset}.version` AS v
    JOIN
      `{project}.{dataset}.collection` AS c
    ON
      v.id = c.version_id
    JOIN
      `{project}.{dataset}.patient` AS p
    ON
      c.id = p.collection_id
    JOIN
      `{project}.{dataset}.study` AS st
    ON
      p.id = st.patient_id
    JOIN
      `{project}.{dataset}.series` AS se
    ON
      st.id = se.study_id
    JOIN
      `{project}.{dataset}.instance` AS i
    ON
      se.id = i.series_id"""
