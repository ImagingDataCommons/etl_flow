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

import sys
import argparse
import settings
from google.cloud import bigquery
from utilities.bq_helpers import create_BQ_dataset
from utilities.tcia_helpers import get_tcia_collection_manager_data
import pandas as pd
from bq.bq_utilities import create_temp_table_from_df
from bq.bq_utilities import get_github_directory_contents_from_comet, \
    get_data_from_comet

# Flatten the version/collection/... hierarchy
# Note that we no longer include license here as the license can change over time.
def create_all_flattened(client):
    view_id = f"{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_flattened"
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
    CONCAT('https://doi.org/', se.versioned_source_doi) versioned_source_url,
    se.hashes AS se_hashes,
    se.sources AS se_sources,
    se.init_idc_version AS se_init_idc_version,
    se.rev_idc_version AS se_rev_idc_version,
    se.final_idc_version AS se_final_idc_version,
    se.excluded se_excluded,
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

  """
    # Make an API request to create the view.
    client.delete_table(view_id, not_found_ok=True)
    view = client.create_table(view, exists_ok=True)
    print(f"Created {view.table_type}: {str(view.reference)}")
    return view


def create_all_sources(client):
    # Create a temporary table of names of all analysis results metadata
    analysis_result_dois = []
    analysis_files = get_github_directory_contents_from_comet("collections/analysis", args.comet_branch)
    for analysis_file in analysis_files:
        data = get_data_from_comet(f"collections/analysis/{analysis_file}", branch=args.comet_branch)
        analysis_result_dois.append((data['source_doi'], data['analysis_result_id'].lower().replace('-', '_').replace(' ', '_')))
    df = pd.DataFrame(analysis_result_dois, columns=['source_doi', 'analysis_result_id'])
    table_id = f"{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.analysis_result_dois"
    schema = [
        bigquery.SchemaField("source_doi", "STRING")
    ]
    create_temp_table_from_df(client, table_id, schema, df, 60)

    query = f"""
with basics as (
  SELECT distinct 
  af.collection_id collection_name,
  REPLACE( REPLACE( LOWER(af.collection_id), '-', '_'), ' ', '_') collection_id, 
  af.source_doi, 
  af.source_url, 
  i_source source,
  if(not dtc.type is null, dtc.type, 'Open') Type,
  if(not dtc.access is NULL, dtc.access, 'Public') Access,
  if(ms.metadata_sunset is NULL, 0, CAST(ms.metadata_sunset AS INT64)) metadata_sunset
FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_flattened` af
LEFT JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.doi_to_access` dtc
ON af.source_doi = dtc.source_doi
LEFT JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.metadata_sunset` ms
ON af.source_doi = ms.source_doi
)
SELECT 
  basics.*,
  if(ard.source_doi IS NULL, False, True) analysis_result,
  if(ard.source_doi IS NULL, "", ard.analysis_result_id) analysis_result_id,
  if(Type='Open', 'idc-arch-open', if(Type='Cr', 'idc-arch-cr', if(Type='Defaced', 'idc-arch-defaced', if(Type='Redacted','idc-arch-redacted','idc-arch-excluded')))) dev_bucket,
  if(Type='Open', 'idc-open-data', if(Type='Cr', 'idc-open-cr', if(Type='Defaced', 'idc-open-idc1', NULL))) pub_gcs_bucket,
  if(Type='Open', 'idc-open-data', if(Type='Cr', 'idc-open-data-cr', if(Type='Defaced', 'idc-open-data-two', NULL))) pub_aws_bucket,
FROM basics
-- ORDER by collection_id, source_doi, dev_bucket, pub_gcs_bucket, pub_aws_bucket
LEFT JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.analysis_result_dois` ard
ON basics.source_doi = ard.source_doi
ORDER by collection_id, basics.source_doi, pub_gcs_bucket, pub_aws_bucket
"""
    # Make an API request to create the view.
    table_id = f"{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_sources"
    client.delete_table(table_id, not_found_ok=True)
    job_config = bigquery.QueryJobConfig(destination=table_id)
    query_job = client.query(query,job_config=job_config)
    query_job.result()
    print(f"Created TABLE: {str(table_id)}")


def create_all_joined(client):
    view_id = f"{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined"
    view = bigquery.Table(view_id)
    view.view_query = f"""
-- SELECT af.*, ac.source, ac.Class, ac.Access, ac.metadata_sunset, ac.dev_bucket, ac.pub_gcs_bucket, ac.pub_aws_bucket
SELECT af.*, ac.source, ac.Type, ac.Access, ac.metadata_sunset, ac.analysis_result, ac.dev_bucket, ac.pub_gcs_bucket, ac.pub_aws_bucket
FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_flattened` af
JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_sources` ac
ON af.source_doi = ac.source_doi 
WHERE af.collection_id=ac.collection_name

  """
    # Make an API request to create the view.
    client.delete_table(view_id, not_found_ok=True)
    view = client.create_table(view, exists_ok=True)
    print(f"Created {view.table_type}: {str(view.reference)}")
    return view


def create_all_joined_public(client):
    view_id = f"{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_public"
    view = bigquery.Table(view_id)
    view.view_query = f"""
    SELECT 
        aj.*, 
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined` aj
    WHERE Access='Public'
    AND i_excluded=FALSE AND i_redacted=FALSE
    """
    # Make an API request to create the view.
    client.delete_table(view_id, not_found_ok=True)
    view = client.create_table(view, exists_ok=True)
    print(f"Created {view.table_type}: {str(view.reference)}")
    return view


def create_all_joined_limited(client):
    view_id = f"{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_limited"
    view = bigquery.Table(view_id)
    view.view_query = f"""
    SELECT * from `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined` aj
    WHERE Access='Limited'
    """
    # Make an API request to create the view.
    client.delete_table(view_id, not_found_ok=True)
    view = client.create_table(view, exists_ok=True)
    print(f"Created {view.table_type}: {str(view.reference)}")
    return view


# All instances that are in excluded collections, across all IDC versions
# This does not include instances that are individually excluded,
# but are from collections that are otherwise included
def create_all_joined_excluded(client):
    view_id = f"{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_excluded"
    view = bigquery.Table(view_id)
    view.view_query = f"""
    SELECT * from `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined` aj
    WHERE Access='Excluded'
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
     CONCAT('https://doi.org/', versioned_source_doi) versioned_source_url,
     analysis_result, 
     sop_instance_uid, 
     i.hash i_hash, 
     ingestion_url,
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
    client.delete_table(view_id, not_found_ok=True)
    view = client.create_table(view, exists_ok=True)
    print(f"Created {view.table_type}: {str(view.reference)}")
    return view


def create_pre_all_joined_collections(client):
    view_id = f"{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.pre_all_joined_collections"
    view = bigquery.Table(view_id)
    view.view_query = f"""
    SELECT 
     c.collection_name,
     c.collection_id,
     c.hash c_hash,
     p.patientID, 
     p.hash p_hash,
     st.StudyInstanceUID, 
     st.hash st_hash,
     se.SeriesInstanceUID, 
     se.hash se_hash, 
     se.excluded se_excluded,
     source_doi,
     source_url,
     versioned_source_doi,
     CONCAT('https://doi.org/', versioned_source_doi) versioned_source_url,
     analysis_result, 
     SOPInstanceUID, 
     i.hash i_hash, 
     ingestion_url,
     size,
     i.excluded i_excluded, 
     idc_version,
     mitigation,
     source_file_hash
    FROM `idc-dev-etl.idc_v0_dev.pre_collection` c
     JOIN `idc-dev-etl.idc_v0_dev.pre_patient` p
     ON c.collection_id = p.collection_id
     JOIN `idc-dev-etl.idc_v0_dev.pre_study` st
     ON p.collection_id = st.collection_id AND p.patientID = st.patientID
     JOIN `idc-dev-etl.idc_v0_dev.pre_series` se
     ON st.StudyInstanceUID = se.StudyInstanceUID
     JOIN `idc-dev-etl.idc_v0_dev.pre_instance` i
     ON se.SeriesInstanceUID = i.SeriesInstanceUID
   """
    # Make an API request to create the view.
    client.delete_table(view_id, not_found_ok=True)
    view = client.create_table(view, exists_ok=True)
    print(f"Created {view.table_type}: {str(view.reference)}")
    return view


def create_all_joined_public_and_current(client):
    view_id = f"{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_public_and_current"
    view = bigquery.Table(view_id)

    view.view_query = f"""
    SELECT 
        aj.*
--         license_url,
--         license_long_name,
--         license_short_name
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_public` aj
--     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.licenses` li
--     ON aj.source_doi = li.source_doi
    WHERE idc_version={settings.CURRENT_VERSION} 
    AND metadata_sunset = 0
    """
    # Make an API request to create the view.
    client.delete_table(view_id, not_found_ok=True)
    view = client.create_table(view, exists_ok=True)
    print(f"Created {view.table_type}: {str(view.reference)}")
    return view


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--comet_branch", default='release/v24')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)

    # Create BQ datasets.
    BQ_client = bigquery.Client(project=settings.DEV_PROJECT)
    try:
        dataset = create_BQ_dataset(BQ_client, settings.BQ_DEV_INT_DATASET)
    except:
        # Presume the dataset already exists
        pass

    # create_all_flattened(BQ_client)
    # create_all_sources(BQ_client)
    # create_all_joined(BQ_client)
    # create_all_joined_public(BQ_client)
    # create_all_joined_public_and_current(BQ_client)
    # create_all_joined_limited(BQ_client)
    # create_all_joined_excluded(BQ_client)
    # create_idc_all_joined(BQ_client)
    # create_pre_all_joined_sources(BQ_client)

    create_pre_all_joined_collections(BQ_client)



