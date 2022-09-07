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
# Create two BQ views, all_joined and all_joined_included. These are the
# same SQL from version to version, but can't be easily copied from
# Cloud SQL.

import settings
import argparse
import json
from google.cloud import bigquery

client = bigquery.Client()

def create_all_joined():
    view_id = f"{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined"
    view = bigquery.Table(view_id)
    view.view_query = f"""
   SELECT v.version AS idc_version,
    v.previous_version AS previous_idc_version,
    v.hashes AS v_hashes,
    v.sources AS v_sources,
    c.collection_id,
    c.idc_collection_id,
    c.uuid AS c_uuid,
    c.hashes AS c_hashes,
    c.sources AS c_sources,
    c.init_idc_version AS c_init_idc_version,
    c.rev_idc_version AS c_rev_idc_version,
    c.final_idc_version AS c_final_idc_version,
    p.submitter_case_id,
    p.idc_case_id,
    p.uuid AS p_uuid,
    p.hashes AS p_hashes,
    p.sources AS p_sources,
    p.init_idc_version AS p_init_idc_version,
    p.rev_idc_version AS p_rev_idc_version,
    p.final_idc_version AS p_final_idc_version,
    st.study_instance_uid,
    st.uuid AS st_uuid,
    st.study_instances,
    st.hashes AS st_hashes,
    st.sources AS st_sources,
    st.init_idc_version AS st_init_idc_version,
    st.rev_idc_version AS st_rev_idc_version,
    st.final_idc_version AS st_final_idc_version,
    se.series_instance_uid,
    se.uuid AS se_uuid,
    se.series_instances,
    se.source_doi,
    se.source_url,
    se.hashes AS se_hashes,
    se.sources AS se_sources,
    se.init_idc_version AS se_init_idc_version,
    se.rev_idc_version AS se_rev_idc_version,
    se.final_idc_version AS se_final_idc_version,
    i.sop_instance_uid,
    i.uuid AS i_uuid,
    i.hash AS i_hash,
    i.source AS i_source,
    i.size AS i_size,
    i.excluded AS i_excluded,
    i.init_idc_version AS i_init_idc_version,
    i.rev_idc_version AS i_rev_idc_version,
    i.final_idc_version AS i_final_idc_version
   FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.version` v
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.version_collection` vc ON v.version = vc.version
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.collection` c ON vc.collection_uuid = c.uuid
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.collection_patient` cp ON c.uuid = cp.collection_uuid
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.patient` p ON cp.patient_uuid = p.uuid
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.patient_study` ps ON p.uuid = ps.patient_uuid
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.study` st ON ps.study_uuid = st.uuid
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.study_series` ss ON st.uuid = ss.study_uuid
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.series` se ON ss.series_uuid = se.uuid
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.series_instance` si ON se.uuid = si.series_uuid
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.instance` i ON si.instance_uuid = i.uuid
  ORDER BY v.version, c.collection_id, p.submitter_case_id, st.study_instance_uid, se.series_instance_uid, i.sop_instance_uid
  """
    # Make an API request to create the view.
    view = client.create_table(view)
    print(f"Created {view.table_type}: {str(view.reference)}")
    return

def create_all_joined_included():
    view_id = f"{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_included"
    view = bigquery.Table(view_id)
    view.view_query = f"""
   SELECT v.version AS idc_version,
    v.previous_version AS previous_idc_version,
    v.hashes AS v_hashes,
    v.sources AS v_sources,
    c.collection_id,
    c.idc_collection_id,
    c.uuid AS c_uuid,
    c.hashes AS c_hashes,
    c.sources AS c_sources,
    c.init_idc_version AS c_init_idc_version,
    c.rev_idc_version AS c_rev_idc_version,
    c.final_idc_version AS c_final_idc_version,
    p.submitter_case_id,
    p.idc_case_id,
    p.uuid AS p_uuid,
    p.hashes AS p_hashes,
    p.sources AS p_sources,
    p.init_idc_version AS p_init_idc_version,
    p.rev_idc_version AS p_rev_idc_version,
    p.final_idc_version AS p_final_idc_version,
    st.study_instance_uid,
    st.uuid AS st_uuid,
    st.study_instances,
    st.hashes AS st_hashes,
    st.sources AS st_sources,
    st.init_idc_version AS st_init_idc_version,
    st.rev_idc_version AS st_rev_idc_version,
    st.final_idc_version AS st_final_idc_version,
    se.series_instance_uid,
    se.uuid AS se_uuid,
    se.series_instances,
    se.source_doi,
    se.source_url,
    se.hashes AS se_hashes,
    se.sources AS se_sources,
    se.init_idc_version AS se_init_idc_version,
    se.rev_idc_version AS se_rev_idc_version,
    se.final_idc_version AS se_final_idc_version,
    i.sop_instance_uid,
    i.uuid AS i_uuid,
    i.hash AS i_hash,
    i.source AS i_source,
    i.size AS i_size,
    i.excluded AS i_excluded,
    i.init_idc_version AS i_init_idc_version,
    i.rev_idc_version AS i_rev_idc_version,
    i.final_idc_version AS i_final_idc_version
   FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.version` v
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.version_collection` vc ON v.version = vc.version
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.collection` c ON vc.collection_uuid = c.uuid
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.collection_patient` cp ON c.uuid = cp.collection_uuid
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.patient` p ON cp.patient_uuid = p.uuid
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.patient_study` ps ON p.uuid = ps.patient_uuid
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.study` st ON ps.study_uuid = st.uuid
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.study_series` ss ON st.uuid = ss.study_uuid
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.series` se ON ss.series_uuid = se.uuid
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.series_instance` si ON se.uuid = si.series_uuid
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.instance` i ON si.instance_uuid = i.uuid
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_included_collections` aic ON c.collection_id = aic.tcia_api_collection_id

  ORDER BY v.version, c.collection_id, p.submitter_case_id, st.study_instance_uid, se.series_instance_uid, i.sop_instance_uid
  """
    # Make an API request to create the view.
    view = client.create_table(view)
    print(f"Created {view.table_type}: {str(view.reference)}")

if __name__ == '__main__':
    create_all_joined()
    create_all_joined_included()
