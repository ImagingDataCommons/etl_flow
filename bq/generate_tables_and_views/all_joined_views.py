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

# Upload DB tables used in to generated subsequent BQ tables.
# Beginning with idc v8, the per version BQ datasets are split
# into idc_v<version>_dev and idc_v<version>_pub. These tables
# go into the former, generated tables into the latter.

import argparse
import settings
from google.cloud import bigquery
from utilities.bq_helpers import create_BQ_dataset

def create_all_joined(client):
    view_id = f"{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined"
    view = bigquery.Table(view_id)
    view.view_query = f"""
   SELECT v.version AS idc_version,
    v.previous_version AS previous_idc_version,
    v.min_timestamp AS v_min_timestamp,
    v.max_timestamp AS v_max_timestamp,
    v.hashes AS v_hashes,
    v.sources AS v_sources,
    c.collection_id,
    c.idc_collection_id,
    c.uuid AS c_uuid,
    c.min_timestamp AS c_min_timestamp,
    c.max_timestamp AS c_max_timestamp,
    c.hashes AS c_hashes,
    c.sources AS c_sources,
    c.init_idc_version AS c_init_idc_version,
    c.rev_idc_version AS c_rev_idc_version,
    c.final_idc_version AS c_final_idc_version,
    c.redacted AS c_redacted,
    p.submitter_case_id,
    p.idc_case_id,
    p.uuid AS p_uuid,
    p.min_timestamp AS p_min_timestamp,
    p.max_timestamp AS p_max_timestamp,
    p.hashes AS p_hashes,
    p.sources AS p_sources,
    p.init_idc_version AS p_init_idc_version,
    p.rev_idc_version AS p_rev_idc_version,
    p.final_idc_version AS p_final_idc_version,
    p.redacted AS p_redacted,
    st.study_instance_uid,
    st.uuid AS st_uuid,
    st.min_timestamp AS st_min_timestamp,
    st.max_timestamp AS st_max_timestamp,
    st.study_instances,
    st.hashes AS st_hashes,
    st.sources AS st_sources,
    st.init_idc_version AS st_init_idc_version,
    st.rev_idc_version AS st_rev_idc_version,
    st.final_idc_version AS st_final_idc_version,
    st.redacted AS st_redacted,
    se.series_instance_uid,
    se.uuid AS se_uuid,
    se.min_timestamp AS se_min_timestamp,
    se.max_timestamp AS se_max_timestamp,
    se.series_instances,
    se.source_doi,
    se.source_url,
    se.versioned_source_doi,
    se.third_party,
    se.hashes AS se_hashes,
    se.sources AS se_sources,
    se.init_idc_version AS se_init_idc_version,
    se.rev_idc_version AS se_rev_idc_version,
    se.final_idc_version AS se_final_idc_version,
    se.license_url,
    se.license_long_name,
    se.license_short_name,
    se.excluded,
    se.redacted AS se_redacted,
    i.sop_instance_uid,
    i.uuid AS i_uuid,
    i.timestamp as i_timestamp,
    i.hash AS i_hash,
    i.source AS i_source,
    i.size AS i_size,
    i.excluded AS i_excluded,
    i.init_idc_version AS i_init_idc_version,
    i.rev_idc_version AS i_rev_idc_version,
    i.final_idc_version AS i_final_idc_version,
    i.redacted AS i_redacted,
    i.mitigation as mitigation,
    i.ingestion_url as ingestion_url,
    ac.dev_tcia_url,
    ac.dev_idc_url,
    ac.pub_gcs_tcia_url,
    ac.pub_gcs_idc_url,
    ac.pub_aws_tcia_url,
    ac.pub_aws_idc_url,
    ac.tcia_access,
    ac.idc_access,
    ac.tcia_metadata_sunset,
    ac.idc_metadata_sunset

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
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_collections` ac ON collection_id = ac.tcia_api_collection_id

  """
    # Make an API request to create the view.
    client.delete_table(view_id, not_found_ok=True)
    view = client.create_table(view, exists_ok=True)
    print(f"Created {view.table_type}: {str(view.reference)}")
    return view

# All instances that are public and in the current version
def create_all_joined_public_and_current(client):
    view_id = f"{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_public_and_current"
    view = bigquery.Table(view_id)
    # view.view_query = f"""
    # SELECT aj.* from `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined` aj
    # JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_collections` ac
    # ON aj.collection_id = ac.tcia_api_collection_id
    # WHERE ((aj.i_source='tcia' AND ac.tcia_access='Public') OR (aj.i_source='idc' AND ac.idc_access='Public'))
    # AND ((aj.i_source='tcia' and ac.tcia_metadata_sunset=0) OR (aj.i_source='idc' and ac.idc_metadata_sunset=0))
    # AND idc_version={settings.CURRENT_VERSION} and aj.i_excluded=False
    # """
    view.view_query = f"""
    SELECT * from `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined`
    WHERE ((i_source='tcia' AND tcia_access='Public') OR (i_source='idc' AND idc_access='Public'))
    AND ((i_source='tcia' and tcia_metadata_sunset=0) OR (i_source='idc' and idc_metadata_sunset=0))
    AND idc_version={settings.CURRENT_VERSION} AND i_excluded=False AND i_redacted=FALSE 
    """
    # Make an API request to create the view.
    client.delete_table(view_id, not_found_ok=True)
    view = client.create_table(view, exists_ok=True)
    print(f"Created {view.table_type}: {str(view.reference)}")
    return view

# All instances that are public across all IDC versions
def create_all_joined_public(client):
    view_id = f"{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_public"
    view = bigquery.Table(view_id)
    view.view_query = f"""
    SELECT * from `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined` aj
    WHERE (i_source='tcia' AND tcia_access='Public') OR (i_source='idc' AND idc_access='Public' AND i_redacted=FALSE )
    """
    # Make an API request to create the view.
    client.delete_table(view_id, not_found_ok=True)
    view = client.create_table(view, exists_ok=True)
    print(f"Created {view.table_type}: {str(view.reference)}")
    return view


# All instances having limited access (redacted), across all IDC versions
def create_all_joined_limited(client):
    view_id = f"{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_limited"
    view = bigquery.Table(view_id)
    view.view_query = f"""
    SELECT * from `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined` aj
    WHERE (i_source='tcia' AND tcia_access='Limited') OR (i_source='idc' AND idc_access='Limited')
    """
    # Make an API request to create the view.
    client.delete_table(view_id, not_found_ok=True)
    view = client.create_table(view, exists_ok=True)
    print(f"Created {view.table_type}: {str(view.reference)}")
    return view


# All instances that are in excluded collections, across all IDC versions
def create_all_joined_excluded(client):
    view_id = f"{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_excluded"
    view = bigquery.Table(view_id)
    view.view_query = f"""
    SELECT * from `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined` aj
    WHERE (i_source='tcia' AND tcia_access='Excluded') OR (i_source='idc' AND idc_access='Excluded' AND i_redacted=FALSE)
    """
    # Make an API request to create the view.
    client.delete_table(view_id, not_found_ok=True)
    view = client.create_table(view, exists_ok=True)
    print(f"Created {view.table_type}: {str(view.reference)}")
    return view

def create_idc_all_joined(client):
    view_id = f"{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.idc_all_joined"
    view = bigquery.Table(view_id)
    view.view_query = f"""
    SELECT 
     c.collection_id, 
     c.hash c_hash,
     c.redacted c_redacted,
     p.submitter_case_id, 
     p.hash p_hash,
     p.redacted p_redacted,
     st.study_instance_uid, 
     st.hash st_hash,
     st.redacted st_redacted,
     se.series_instance_uid, 
     se.hash se_hash, 
     se.excluded se_excluded,
     se.redacted se_redacted, 
     source_doi, 
     source_url,
     versioned_source_doi,
     versioned_source_url,
     third_party, 
     license_long_name,
     license_short_name, 
     license_url,
     sop_instance_uid, 
     i.hash i_hash, 
     gcs_url,
     size,
     i.excluded i_excluded, 
     idc_version,
     i.redacted i_redacted,
     mitigation
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.idc_collection` c
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.idc_patient` p
     ON c.collection_id = p.collection_id
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.idc_study` st
     ON p.submitter_case_id = st.submitter_case_id
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.idc_series` se
     ON st.study_instance_uid = se.study_instance_uid
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.idc_instance` i
     ON se.series_instance_uid = i.series_instance_uid
   """
    # Make an API request to create the view.
    view = client.create_table(view, exists_ok=True)
    print(f"Created {view.table_type}: {str(view.reference)}")
    return view



if __name__ == '__main__':
    # Create BQ datasets.
    BQ_client = bigquery.Client(project=settings.DEV_PROJECT)
    try:
        dataset = create_BQ_dataset(BQ_client, settings.BQ_DEV_INT_DATASET)
    except:
        # Presume the dataset already exists
        pass

    create_all_joined(BQ_client)
    create_all_joined_public(BQ_client)
    create_all_joined_limited(BQ_client)
    create_all_joined_excluded(BQ_client)
    create_all_joined_public_and_current(BQ_client)
    create_idc_all_joined(BQ_client)




