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
    WITH version_with_max_timestamp as (
    select v.id, v.idc_version_number, v.version_hash, max(c.collection_timestamp) as max_collection_timestamp
    FROM
      `{project}.{dataset}.version` AS v
    JOIN
      `{project}.{dataset}.collection` AS c
    ON
      v.id = c.version_id
    LEFT JOIN 
      `{project}.{dataset}.excluded_collections` as ex
    ON 
      LOWER(c.tcia_api_collection_id) = LOWER(ex.tcia_api_collection_id)
    WHERE ex.tcia_api_collection_id IS NULL
    GROUP BY v.id, v.idc_version_number, v.version_hash
    ),
    coll_stat AS (
    SELECT o.tcia_api_collection_id, o.{target}_url as url, o.access
    FROM
    `idc-dev-etl.idc_v{version}.open_collections` as o
    UNION ALL
    SELECT cr.tcia_api_collection_id, cr.{target}_url as url, cr.access
    FROM
    `idc-dev-etl.idc_v{version}.cr_collections` as cr
    UNION ALL
    SELECT r.tcia_api_collection_id, r.{target}_url as url, r.access
    FROM
    `idc-dev-etl.idc_v{version}.redacted_collections` as r)

    SELECT
      v.idc_version_number AS idc_version_number,
      v.max_collection_timestamp as idc_version_timestamp,
      v.version_hash AS version_hash,
      c.tcia_api_collection_id AS tcia_api_collection_id,
      REPLACE(REPLACE(LOWER(c.tcia_api_collection_id),'-','_'), ' ','_') AS idc_webapp_collection_id,
      se.source_doi AS source_doi,
--      c.collection_timestamp AS collection_timestamp,
      c.collection_hash AS collection_hash,
--      c.collection_initial_idc_version AS collection_init_idc_version,
      coll_stat.access AS access,
      p.submitter_case_id AS submitter_case_id,
      p.idc_case_id AS idc_case_id,
--      p.patient_timestamp AS patient_timestamp,
      p.patient_hash AS patient_hash,
--      p.patient_initial_idc_version AS patient_init_idc_version,
      st.study_instance_uid AS StudyInstanceUID,
      st.study_uuid AS study_uuid,
--      st.study_instances AS study_instances,
--      st.study_timestamp AS study_timestamp,
      st.study_hash AS study_hash,
--      st.study_initial_idc_version AS study_init_idc_version,
      se.series_instance_uid AS SeriesInstanceUID,
      se.series_uuid AS series_uuid,
--      se.series_instances AS series_instances,
--      se.series_timestamp AS series_timestamp,
      se.series_hash AS series_hash,
--      se.series_initial_idc_version AS series_init_idc_version,
      i.sop_instance_uid AS SOPInstanceUID,
      i.instance_uuid AS instance_uuid,
      CONCAT('gs://', coll_stat.url, '/', i.instance_uuid, '.dcm') as gcs_url,
      coll_stat.url as gcs_bucket,
--      CONCAT('gs://','{gcs_bucket}','/', i.instance_uuid,'.dcm') AS gcs_url,
--      '{gcs_bucket}' AS gcs_bucket,
      i.instance_size AS instance_size,
--      i.instance_timestamp AS instance_timestamp,
      i.instance_hash AS instance_hash,
--      i.instance_initial_idc_version AS instance_init_idc_version
    FROM
--      `{project}.{dataset}.version` AS v
      version_with_max_timestamp as v
    JOIN
      `{project}.{dataset}.collection` AS c
    ON
      v.id = c.version_id
    JOIN
      `{project}.{dataset}.patient` AS p
    ON
      c.id = p.collection_id
    JOIN
      coll_stat
    ON
      lower(c.tcia_api_collection_id) = lower(coll_stat.tcia_api_collection_id)
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
      se.id = i.series_id
    LEFT JOIN 
      `{project}.{dataset}.excluded_collections` as ex
    ON 
      LOWER(c.tcia_api_collection_id) = LOWER(ex.tcia_api_collection_id)
    WHERE ex.tcia_api_collection_id IS NULL"""