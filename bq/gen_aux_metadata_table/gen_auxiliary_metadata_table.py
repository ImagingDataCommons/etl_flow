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

# This script generates the BQ auxiliary_metadata table. It basically joins the BQ version, collection,
# patient, study, series, and instance tables. Typically these are uploaded from PostgreSQL to BQ using
# the upload_psql_to_bq.py script
import argparse
import sys
from google.cloud import bigquery
from utilities.bq_helpers import load_BQ_from_json, query_BQ

def gen_aux_table(args):

    query = f"""
WITH
  collection_access AS (
  SELECT o.tcia_api_collection_id, o.premerge_tcia_url, o.premerge_path_url, o.{args.target}_url as url, o.access
  FROM
    `idc-dev-etl.{args.dev_bqdataset_name}.open_collections` as o
  UNION ALL
  SELECT cr.tcia_api_collection_id, cr.premerge_tcia_url, cr.premerge_path_url, cr.{args.target}_url as url, cr.access
  FROM
    `idc-dev-etl.{args.dev_bqdataset_name}.cr_collections` as cr
  UNION ALL
  SELECT r.tcia_api_collection_id, r.premerge_tcia_url, r.premerge_path_url, r.{args.target}_url as url, r.access
  FROM
    `idc-dev-etl.{args.dev_bqdataset_name}.redacted_collections` as r
  UNION ALL
  SELECT d.tcia_api_collection_id, d.premerge_tcia_url, d.premerge_path_url, d.{args.target}_url as url, d.access
  FROM
    `idc-dev-etl.{args.dev_bqdataset_name}.defaced_collections` as d),
  license_info AS (
  SELECT
    DOI,
    license_url,
    license_long_name,
    license_short_name
  FROM
    `idc-dev-etl.{args.pub_bqdataset_name}.original_collections_metadata`
  UNION ALL
  SELECT
    DOI,
    license_url,
    license_long_name,
    license_short_name
  FROM
    `idc-dev-etl.{args.pub_bqdataset_name}.analysis_results_metadata` )
SELECT
  c.collection_id AS tcia_api_collection_id,
  REPLACE(REPLACE(LOWER(c.collection_id),'-','_'), ' ','_') AS idc_webapp_collection_id,
  c.min_timestamp as collection_timestamp,
  c.hashes.all_hash AS collection_hash,
  c.init_idc_version AS collection_init_idc_version,
  c.rev_idc_version AS collection_revised_idc_version,
--   c.Access As access,
  collection_access.access AS access,
--
  p.submitter_case_id AS submitter_case_id,
  p.idc_case_id AS idc_case_id,
  p.hashes.all_hash AS patient_hash,
  p.init_idc_version AS patient_init_idc_version,
  p.rev_idc_version AS patient_revised_idc_version,
--
  st.study_instance_uid AS StudyInstanceUID,
  st.uuid AS study_uuid,
  st.study_instances AS study_instances,
  st.hashes.all_hash AS study_hash,
  st.init_idc_version AS study_init_idc_version,
  st.rev_idc_version AS study_revised_idc_version,
--
  se.series_instance_uid AS SeriesInstanceUID,
  se.uuid AS series_uuid,
  IF(c.collection_id='APOLLO', '', se.source_doi) AS source_doi,
  se.series_instances AS series_instances,
  se.hashes.all_hash AS series_hash,
  se.init_idc_version AS series_init_idc_version,
  se.rev_idc_version AS series_revised_idc_version,
--
  i.sop_instance_uid AS SOPInstanceUID,
  i.uuid AS instance_uuid,
  i.size AS instance_size,
  i.hash AS instance_hash,
  i.init_idc_version AS instance_init_idc_version,
  i.rev_idc_version AS instance_revised_idc_version,
  li.license_url AS license_url,
  li.license_long_name AS license_long_name,
  li.license_short_name AS license_short_name,
  CONCAT('gs://', if(i.rev_idc_version = {args.version}, if(i.source = 'tcia', collection_access.premerge_tcia_url, collection_access.premerge_path_url), collection_access.url), '/', i.uuid, '.dcm') as gcs_url,

  FROM
    `{args.src_project}.{args.dev_bqdataset_name}.version` AS v
  JOIN
    `{args.src_project}.{args.dev_bqdataset_name}.version_collection` AS vc
  ON
    v.version = vc.version
  JOIN
    `{args.src_project}.{args.dev_bqdataset_name}.collection` AS c
  ON
    vc.collection_uuid = c.uuid
  JOIN
    `{args.src_project}.{args.dev_bqdataset_name}.collection_patient` AS cp
  ON
    c.uuid = cp.collection_uuid
  JOIN
    `{args.src_project}.{args.dev_bqdataset_name}.patient` AS p
  ON
    cp.patient_uuid = p.uuid
  JOIN
    `{args.src_project}.{args.dev_bqdataset_name}.patient_study` AS ps
  ON
    p.uuid = ps.patient_uuid
  JOIN
    `{args.src_project}.{args.dev_bqdataset_name}.study` AS st
  ON
    ps.study_uuid = st.uuid
  JOIN
    `{args.src_project}.{args.dev_bqdataset_name}.study_series` AS ss
  ON
    st.uuid = ss.study_uuid
  JOIN
    `{args.src_project}.{args.dev_bqdataset_name}.series` AS se
  ON
    ss.series_uuid = se.uuid
  JOIN
    `{args.src_project}.{args.dev_bqdataset_name}.series_instance` si
  ON
    se.uuid = si.series_uuid
  JOIN
    `{args.src_project}.{args.dev_bqdataset_name}.instance` i
  ON
    si.instance_uuid = i.uuid
  LEFT JOIN
    `{args.src_project}.{args.dev_bqdataset_name}.excluded_collections` ex
  ON
    c.collection_id = ex.tcia_api_collection_id
  JOIN
    collection_access
  ON
    c.collection_id = collection_access.tcia_api_collection_id
  JOIN
    license_info AS li
  ON
    se.source_doi = li.DOI
  WHERE
    ex.tcia_api_collection_id IS NULL
  AND
    v.version = {args.version}
  ORDER BY
    tcia_api_collection_id, submitter_case_id, StudyInstanceUID, SeriesInstanceUID, SOPInstanceUID
"""


    client = bigquery.Client(project=args.dst_project)
    result=query_BQ(client, args.pub_bqdataset_name, args.bqtable_name, query, write_disposition='WRITE_TRUNCATE')
