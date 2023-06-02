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

# Upload tables from Cloud SQL to BQ
import google.cloud.bigquery
from google.cloud import bigquery
from utilities.bq_helpers import BQ_table_exists, delete_BQ_Table, query_BQ
from utilities.logging_config import successlogger, errlogger
from time import time, sleep
from python_settings import settings

def create_idc_all_joined(client, args, table, order_by):
    view_id = f"{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.idc_all_joined"
    view = bigquery.Table(view_id)
    view.view_query = f"""
    SELECT 
     c.collection_id, 
     c.hash 
     c_hash,
     p.submitter_case_id, 
     p.hash p_hash,
     st.study_instance_uid, 
     st.hash st_hash,
     se.series_instance_uid, 
     se.hash se_hash, 
     se.excluded se_excluded, 
     wiki_doi, 
     wiki_url, 
     third_party, 
     license_long_name,
     license_short_name, 
     license_url,
     sop_instance_uid, 
     i.hash i_hash, 
     gcs_url, 
     size,
     i.excluded i_excluded, 
     idc_version
    FROM idc_collection c
     JOIN idc_patient p
     ON c.collection_id = p.collection_id
     JOIN idc_study st
     ON p.submitter_case_id = st.submitter_case_id
     JOIN idc_series se
     ON st.study_instance_uid = se.study_instance_uid
     JOIN idc_instance i
     ON se.series_instance_uid = i.series_instance_uid
   """
    # Make an API request to create the view.
    view = client.create_table(view, exists_ok=True)
    print(f"Created {view.table_type}: {str(view.reference)}")
    return view


def create_all_joined(client, args, table, order_by):
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
    c.min_timestamp AS c_min_timestamp,
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
    se.third_party,
    se.hashes AS se_hashes,
    se.sources AS se_sources,
    se.init_idc_version AS se_init_idc_version,
    se.rev_idc_version AS se_rev_idc_version,
    se.final_idc_version AS se_final_idc_version,
    se.license_url,
    se.license_long_name,
    se.license_short_name,
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
     WHERE i.excluded=False
  """
    # Make an API request to create the view.
    view = client.create_table(view, exists_ok=True)
    print(f"Created {view.table_type}: {str(view.reference)}")
    return view


def upload_version(client, args, table, order_by):
    sql = f"""
    SELECT
      CAST(version AS INT) AS version,
      CAST(previous_version AS INT) AS previous_version,
      min_timestamp,
      max_timestamp,
      done,
      is_new,
      expanded,
      STRUCT(tcia,
        idc,
        all_sources) AS hashes,
      STRUCT(tcia_src AS tcia,
        idc_src AS idc) AS sources,
      STRUCT(tcia_revised AS tcia,
        idc_revised AS idc) AS revised
    FROM
      EXTERNAL_QUERY ( '{args.federated_query}',
        '''SELECT version, previous_version, min_timestamp, max_timestamp, done, 
            is_new, expanded, (hashes).tcia, (hashes).idc, (hashes).all_sources, 
            (sources).tcia AS tcia_src, (sources).idc AS idc_src, (revised).tcia AS tcia_revised, 
            (revised).idc AS idc_revised 
        FROM {table}''')
    ORDER BY {order_by}
    """
    result=query_BQ(client, settings.BQ_DEV_INT_DATASET, table, sql, write_disposition='WRITE_TRUNCATE')
    return result


def upload_collection(client, args, table, order_by):
    sql = f"""
    SELECT
      collection_id,
      idc_collection_id,
      uuid,
      min_timestamp,
      max_timestamp,
      CAST(init_idc_version AS INT) AS init_idc_version,
      CAST(rev_idc_version AS INT) AS rev_idc_version,
      CAST(final_idc_version AS INT) AS final_idc_version,
      done,
      is_new,
      expanded,
      STRUCT(tcia_hash,
        idc_hash,
        all_hash) AS hashes,
      STRUCT(tcia_src AS tcia,
        idc_src AS idc) AS sources,
      STRUCT(tcia_rev AS tcia,
        idc_rev AS idc) AS revised
    FROM
      EXTERNAL_QUERY ( '{args.federated_query}',
        '''SELECT collection_id, idc_collection_id, uuid, min_timestamp, max_timestamp, 
            init_idc_version, rev_idc_version, final_idc_version, done, is_new, expanded, 
            (hashes).tcia AS tcia_hash, (hashes).idc AS idc_hash, (hashes).all_sources AS all_hash, 
            (sources).tcia AS tcia_src, (sources).idc AS idc_src, (revised).tcia AS tcia_rev, 
            (revised).idc AS idc_rev 
        FROM {table}''')
    ORDER BY {order_by}
    """
    result=query_BQ(client, settings.BQ_DEV_INT_DATASET, table, sql, write_disposition='WRITE_TRUNCATE')
    return result

def upload_patient(client, args, table, order_by):

    sql = f"""
    SELECT
      submitter_case_id,
      idc_case_id,
      uuid,
      min_timestamp,
      max_timestamp,
      CAST(init_idc_version AS INT) AS init_idc_version,
      CAST(rev_idc_version AS INT) AS rev_idc_version,
      CAST(final_idc_version AS INT) AS final_idc_version,
      done,
      is_new,
      expanded,
      STRUCT(tcia_hash,
        idc_hash,
        all_hash) AS hashes,
      STRUCT(tcia_src AS tcia,
        idc_src AS idc) AS sources,
      STRUCT(tcia_rev AS tcia,
        idc_rev AS idc) AS revised
    FROM
      EXTERNAL_QUERY ( '{args.federated_query}',
        '''SELECT submitter_case_id, idc_case_id, uuid, min_timestamp, max_timestamp, 
            init_idc_version, rev_idc_version, final_idc_version, done, is_new, expanded, 
            (hashes).tcia AS tcia_hash, (hashes).idc AS idc_hash, (hashes).all_sources AS all_hash, 
            (sources).tcia AS tcia_src, (sources).idc AS idc_src, (revised).tcia AS tcia_rev, 
            (revised).idc AS idc_rev 
        FROM {table}''')
    ORDER BY {order_by}
    """
    result=query_BQ(client, settings.BQ_DEV_INT_DATASET, table, sql, write_disposition='WRITE_TRUNCATE')
    return result

def upload_study(client, args, table, order_by):
    sql = f"""
    SELECT
      study_instance_uid,
      uuid,
      CAST(study_instances AS INT) AS study_instances,
      min_timestamp,
      max_timestamp,
      CAST(init_idc_version AS INT) AS init_idc_version,
      CAST(rev_idc_version AS INT) AS rev_idc_version,
      CAST(final_idc_version AS INT) AS final_idc_version,
      done,
      is_new,
      expanded,
      STRUCT(tcia_hash,
        idc_hash,
        all_hash) AS hashes,
      STRUCT(tcia_src AS tcia,
        idc_src AS idc) AS sources,
      STRUCT(tcia_rev AS tcia,
        idc_rev AS idc) AS revised
    FROM
      EXTERNAL_QUERY ( '{args.federated_query}',
        '''SELECT study_instance_uid, uuid, study_instances, min_timestamp, 
            max_timestamp, init_idc_version, rev_idc_version, final_idc_version, 
            done, is_new, expanded, (hashes).tcia AS tcia_hash, (hashes).idc AS idc_hash, 
            (hashes).all_sources AS all_hash, (sources).tcia AS tcia_src, 
            (sources).idc AS idc_src, (revised).tcia AS tcia_rev, (revised).idc AS idc_rev 
        FROM {table}''')
    ORDER BY {order_by}
    """
    result=query_BQ(client, settings.BQ_DEV_INT_DATASET, table, sql, write_disposition='WRITE_TRUNCATE')
    return result


def upload_series(client, args, table, order_by):
    sql = f"""
    SELECT
      series_instance_uid,
      uuid,
      CAST(series_instances AS INT) AS series_instances,
      source_doi,
      min_timestamp,
      max_timestamp,
      CAST(init_idc_version AS INT) AS init_idc_version,
      CAST(rev_idc_version AS INT) AS rev_idc_version,
      CAST(final_idc_version AS INT) AS final_idc_version,
      done,
      is_new,
      expanded,
      STRUCT(tcia_hash,
        idc_hash,
        all_hash) AS hashes,
      STRUCT(tcia_src AS tcia,
        idc_src AS idc) AS sources,
      STRUCT(tcia_rev AS tcia,
        idc_rev AS idc) AS revised,
      source_url,
      excluded,
      license_long_name,
      license_url,
      license_short_name,
      third_party
      
    FROM
      EXTERNAL_QUERY ( '{args.federated_query}',
        '''SELECT series_instance_uid, uuid, series_instances, source_doi, 
            min_timestamp, max_timestamp, init_idc_version, rev_idc_version, 
            final_idc_version, done, is_new, expanded, (hashes).tcia AS tcia_hash, 
            (hashes).idc AS idc_hash, (hashes).all_sources AS all_hash, 
            (sources).tcia AS tcia_src, (sources).idc AS idc_src, 
            (revised).tcia AS tcia_rev, (revised).idc AS idc_rev,
            source_url, excluded, license_long_name, license_url,
            license_short_name, third_party
        FROM {table}''')
    ORDER BY {order_by}
    """
    result=query_BQ(client, settings.BQ_DEV_INT_DATASET, table, sql, write_disposition='WRITE_TRUNCATE')
    return result


def upload_instance(client, args, table, order_by):
    sql = f"""SELECT
      sop_instance_uid,
      uuid,
      `hash`,  
      CAST(size AS INT) AS size,
      revised,
      done,
      is_new,
      expanded,
      CAST(init_idc_version AS INT) AS init_idc_version,
      CAST(rev_idc_version AS INT) AS rev_idc_version,
      CAST(final_idc_version AS INT) AS final_idc_version,
      source,
      timestamp,
      excluded
    FROM
      EXTERNAL_QUERY ( '{args.federated_query}',
        '''SELECT sop_instance_uid, uuid, hash, size, revised, done, is_new, 
            expanded, init_idc_version, rev_idc_version, final_idc_version, 
            cast(source AS varchar) AS source, timestamp, excluded
        FROM {table}''')
    ORDER BY {order_by}
    """
    result=query_BQ(client, settings.BQ_DEV_INT_DATASET, table, sql, write_disposition='WRITE_TRUNCATE')
    return result


def upload_table(client, args, table, order_by):
    sql = f"""
    SELECT
        *
    FROM
      EXTERNAL_QUERY ( '{args.federated_query}',
        '''SELECT * from {table}''')
    ORDER BY {order_by}
    """
    result=query_BQ(client, settings.BQ_DEV_INT_DATASET, table, sql, write_disposition='WRITE_TRUNCATE')
    return result


def upload_to_bq(args, tables):
    client = bigquery.Client(project=settings.DEV_PROJECT)
    for table in args.upload:
        successlogger.info(f'Uploading table {table}')
        b = time()

        if BQ_table_exists(client, settings.DEV_PROJECT, settings.BQ_DEV_INT_DATASET, table):
            delete_BQ_Table(client, settings.DEV_PROJECT, settings.BQ_DEV_INT_DATASET, table)
        result = tables[table]['func'](client, args, table, tables[table]['order_by'])
        if type(result) != google.cloud.bigquery.Table:
            job_id = result.path.split('/')[-1]
            job = client.get_job(job_id, location='US')
            while job.state != 'DONE':
                successlogger.info('Waiting...')
                sleep(15)
                job = client.get_job(job_id, location='US')
            if not job.error_result==None:
                errlogger.error(f'{table} upload failed')
            else:
                successlogger.info(f'{table} upload completed in {time()-b:.2f}s')
        else:
            successlogger.info(f'{table} upload completed in {time() - b:.2f}s')





