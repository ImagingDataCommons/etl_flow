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
import json
import sys
from google.cloud import bigquery

from python_settings import settings
from time import sleep
from utilities.bq_helpers import load_BQ_from_json, query_BQ
from bq.utils.gen_license_table import get_original_collection_licenses


def create_original_collections_licenses_table(args):
    BQ_client = bigquery.Client()
    args.gen_excluded = False
    licenses = get_original_collection_licenses(args)
    flattened_licenses = []
    for collection_id, license_info in licenses.items():
        for source, license in license_info.items():
            flattened_licenses.append(
                json.dumps(
                    {
                        "idc_webapp_collection_id": collection_id,
                        "source": source,
                        "license_url": license['license_url'],
                        "license_long_name": license['license_long_name'],
                        "license_short_name": license['license_short_name']
                    }
                )
            )
    json_licenses = '\n'.join(flattened_licenses)
    job = load_BQ_from_json(BQ_client,
                settings.DEV_PROJECT,
                settings.BQ_DEV_INT_DATASET, args.temp_license_table_name, json_licenses,
                            aschema=None, write_disposition='WRITE_TRUNCATE')
    while not job.state == 'DONE':
        print('Status: {}'.format(job.state))
        sleep(args.period * 60)

def build_table(args):
    query = f"""
WITH
  collection_access AS (
--   SELECT DISTINCT m.idc_collection_id, o.premerge_tcia_url, o.premerge_path_url, o.{args.target}_url as url, o.access
  SELECT DISTINCT g.tcia_api_collection_id, g.idc_collection_id, g.dev_tcia_url, g.dev_path_url, g.pub_tcia_url, g.pub_path_url, g.tcia_access	path_access
  FROM
    `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.version` as v
  JOIN 
    `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.version_collection` vc
  ON
    v.version = vc.version
  JOIN
    `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.collection` c
  ON
    vc.collection_uuid = c.uuid
  JOIN
    `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.open_collections` as g
  ON
--     REPLACE(REPLACE(LOWER(c.collection_id),' ','_'),'-','_') = REPLACE(REPLACE(LOWER(g.tcia_api_collection_id ),' ','_'),'-','_') 
    c.collection_id = g.tcia_api_collection_id
  # JOIN
  #   `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.collection_id_map` as m
  # ON REPLACE(REPLACE(LOWER(o.tcia_api_collection_id),'-','_'), ' ','_') = REPLACE(REPLACE(LOWER(m.idc_webapp_collection_id),'-','_'), ' ','_')
  WHERE v.version = {settings.CURRENT_VERSION}

  UNION ALL

--   SELECT DISTINCT m.idc_collection_id, cr.premerge_tcia_url, cr.premerge_path_url, cr.{args.target}_url as url, cr.access
  SELECT DISTINCT g.tcia_api_collection_id, g.idc_collection_id, g.dev_tcia_url, g.dev_path_url, g.pub_tcia_url, g.pub_path_url, g.tcia_access	path_access
  FROM
    `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.version` as v
  JOIN 
    `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.version_collection` vc
  ON
    v.version = vc.version
  JOIN
    `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.collection` c
  ON
    vc.collection_uuid = c.uuid
  JOIN
    `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.cr_collections` as g
  ON
--     REPLACE(REPLACE(LOWER(c.collection_id),' ','_'),'-','_') = REPLACE(REPLACE(LOWER(g.tcia_api_collection_id ),' ','_'),'-','_') 
    c.collection_id = g.tcia_api_collection_id
  # JOIN
  #   `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.collection_id_map` as m
  # ON REPLACE(REPLACE(LOWER(cr.tcia_api_collection_id),'-','_'), ' ','_') = REPLACE(REPLACE(LOWER(m.idc_webapp_collection_id),'-','_'), ' ','_')
  WHERE v.version = {settings.CURRENT_VERSION}

  UNION ALL

--   SELECT DISTINCT m.idc_collection_id, r.premerge_tcia_url, r.premerge_path_url, r.{args.target}_url as url, r.access
  SELECT DISTINCT g.tcia_api_collection_id, g.idc_collection_id, g.dev_tcia_url, g.dev_path_url, g.pub_tcia_url, g.pub_path_url, g.tcia_access	path_access
  FROM
    `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.version` as v
  JOIN 
    `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.version_collection` vc
  ON
    v.version = vc.version
  JOIN
    `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.collection` c
  ON
    vc.collection_uuid = c.uuid
  JOIN
    `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.redacted_collections` as g
  ON
--     REPLACE(REPLACE(LOWER(c.collection_id),' ','_'),'-','_') = REPLACE(REPLACE(LOWER(g.tcia_api_collection_id ),' ','_'),'-','_') 
    c.collection_id = g.tcia_api_collection_id
  # JOIN
  #   `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.collection_id_map` as m
  # ON REPLACE(REPLACE(LOWER(r.tcia_api_collection_id),'-','_'), ' ','_') = REPLACE(REPLACE(LOWER(m.idc_webapp_collection_id),'-','_'), ' ','_')
  WHERE v.version = {settings.CURRENT_VERSION}
 
  UNION ALL

--   SELECT DISTINCT m.idc_collection_id, d.premerge_tcia_url, d.premerge_path_url, d.{args.target}_url as url, d.access
  SELECT DISTINCT g.tcia_api_collection_id, g.idc_collection_id, g.dev_tcia_url, g.dev_path_url, g.pub_tcia_url, g.pub_path_url, g.tcia_access	path_access
  FROM
    `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.version` as v
  JOIN 
    `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.version_collection` vc
  ON
    v.version = vc.version
  JOIN
    `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.collection` c
  ON
    vc.collection_uuid = c.uuid
  JOIN
    `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.defaced_collections` as g
  ON
--     REPLACE(REPLACE(LOWER(c.collection_id),' ','_'),'-','_') = REPLACE(REPLACE(LOWER(g.tcia_api_collection_id ),' ','_'),'-','_') 
    c.collection_id = g.tcia_api_collection_id
  # JOIN
  #   `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.collection_id_map` as m
  # ON REPLACE(REPLACE(LOWER(d.tcia_api_collection_id),'-','_'), ' ','_') = REPLACE(REPLACE(LOWER(m.idc_webapp_collection_id),'-','_'), ' ','_')
  WHERE v.version = {settings.CURRENT_VERSION}
  ),
--
  tcia_licenses AS (
  SELECT
    oc.DOI, oc.URL,
    tl.license_url,
    tl.license_long_name,
    tl.license_short_name,
    IF(tl.license_short_name='TCIA' or tl.license_short_name='TCIA NC', 'Limited', 'Public') AS access

  FROM
    `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.{args.temp_license_table_name}` tl
  JOIN
    `idc-dev-etl.{settings.BQ_DEV_EXT_DATASET}.original_collections_metadata` oc
  ON tl.idc_webapp_collection_id = oc.idc_webapp_collection_id
  WHERE tl.source = 'tcia'),
--
  path_licenses AS (
  SELECT
    oc.DOI, oc.URL,
    tl.license_url,
    tl.license_long_name,
    tl.license_short_name,
    IF(tl.license_short_name='TCIA' or tl.license_short_name='TCIA NC', 'Limited', 'Public') AS access

  FROM
    `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.{args.temp_license_table_name}` tl
  JOIN
    `idc-dev-etl.{settings.BQ_DEV_EXT_DATASET}.original_collections_metadata` oc
  ON tl.idc_webapp_collection_id = oc.idc_webapp_collection_id
  WHERE tl.source = 'path'),
--
  analysis_licenses AS (
  SELECT
    DOI, "" AS URL,
    license_url,
    license_long_name,
    license_short_name,
    access

  FROM
    `idc-dev-etl.{settings.BQ_DEV_EXT_DATASET}.analysis_results_metadata` ),
--
pre_licensed as (
SELECT
  c.collection_id AS tcia_api_collection_id,
  REPLACE(REPLACE(LOWER(c.collection_id),'-','_'), ' ','_') AS idc_webapp_collection_id,
  REPLACE(REPLACE(LOWER(c.collection_id),'-','_'), ' ','_') AS collection_id,
  c.min_timestamp as collection_timestamp,
  c.hashes.all_hash AS collection_hash,
  c.init_idc_version AS collection_init_idc_version,
  c.rev_idc_version AS collection_revised_idc_version,
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
  IF(se.source_url is Null , "", se.source_url) AS source_url,
  se.series_instances AS series_instances,
  se.hashes.all_hash AS series_hash,
  se.init_idc_version AS series_init_idc_version,
  se.rev_idc_version AS series_revised_idc_version,
--
  i.sop_instance_uid AS SOPInstanceUID,
  i.uuid AS instance_uuid,
  CONCAT('gs://',
    # If we are generating gcs_url for the public auxiliary_metadata table 
    if('{args.target}' = 'pub', 
        if( i.source='tcia', collection_access.pub_tcia_url, collection_access.pub_path_url), 
    #else 
        # We are generating the dev auxiliary_metadata
        # If this instance is new in this version and we 
        # have not merged new instances into dev buckets
        if(i.rev_idc_version = {settings.CURRENT_VERSION} and not {args.merged},
            # We use the premerge url prefix
            CONCAT('idc_v', {settings.CURRENT_VERSION}, 
                '_',
                i.source,
                '_',
                REPLACE(REPLACE(LOWER(c.collection_id),'-','_'), ' ','_')
                ),

        #else
            # This instance is not new so use the staging bucket prefix
             if( i.source='tcia', collection_access.dev_tcia_url, collection_access.dev_path_url)
            )
        ), 
    '/', i.uuid, '.dcm') as gcs_url,
  i.size AS instance_size,
  i.hash AS instance_hash,
  i.init_idc_version AS instance_init_idc_version,
  i.rev_idc_version AS instance_revised_idc_version,
  i.source AS i_source

  FROM
    `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.version` AS v
  JOIN
    `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.version_collection` AS vc
  ON
    v.version = vc.version
  JOIN
    `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.collection` AS c
  ON
    vc.collection_uuid = c.uuid
  JOIN
    `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.collection_patient` AS cp
  ON
    c.uuid = cp.collection_uuid
  JOIN
    `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.patient` AS p
  ON
    cp.patient_uuid = p.uuid
  JOIN
    `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.patient_study` AS ps
  ON
    p.uuid = ps.patient_uuid
  JOIN
    `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.study` AS st
  ON
    ps.study_uuid = st.uuid
  JOIN
    `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.study_series` AS ss
  ON
    st.uuid = ss.study_uuid
  JOIN
    `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.series` AS se
  ON
    ss.series_uuid = se.uuid
  JOIN
    `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.series_instance` si
  ON
    se.uuid = si.series_uuid
  JOIN
    `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.instance` i
  ON
    si.instance_uuid = i.uuid
  LEFT JOIN
    `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.excluded_collections` ex
  ON
    c.collection_id = ex.tcia_api_collection_id
  JOIN
    collection_access
  ON
    c.idc_collection_id = collection_access.idc_collection_id
  WHERE
    ex.tcia_api_collection_id IS NULL 
  AND 
    i.excluded is False
  AND
    v.version = {args.version}
    ),
licensed as (
  SELECT 
    p_l.*, 
    li.access,
    li.license_url,
    li.license_long_name,
    li.license_short_name
  FROM pre_licensed p_l
  JOIN tcia_licenses li
  ON IFNULL(p_l.source_doi,'') = IFNULL(li.DOI,'') 
  AND IFNULL(p_l.source_url,'') = IFNULL(li.URL,'') AND p_l.i_source='tcia'

  UNION ALL
  SELECT 
    p_l.*, 
    li.access,
    li.license_url,
    li.license_long_name,
    li.license_short_name
  FROM pre_licensed p_l
  JOIN path_licenses li
  ON IFNULL(p_l.source_doi,'') = IFNULL(li.DOI,'') 
  AND IFNULL(p_l.source_url,'') = IFNULL(li.URL,'') AND p_l.i_source='path'

  UNION ALL
  SELECT 
    p_l.*, 
    li.access,
    li.license_url,
    li.license_long_name,
    li.license_short_name
  FROM pre_licensed p_l
  JOIN analysis_licenses li
  ON IFNULL(p_l.source_doi,'') = IFNULL(li.DOI,'') 
  AND IFNULL(p_l.source_url,'') = IFNULL(li.URL,'')
  )
--
  SELECT * EXCEPT(i_source)
  FROM licensed
  ORDER BY
    tcia_api_collection_id, submitter_case_id
"""

    client = bigquery.Client(project=args.dst_project)
    result=query_BQ(client, args.trg_bqdataset_name, args.bqtable_name, query, write_disposition='WRITE_TRUNCATE')

def gen_aux_table(args):
    create_original_collections_licenses_table(args)
    build_table(args)