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

from google.cloud import bigquery
from utilities.bq_helpers import BQ_table_exists, delete_BQ_Table, query_BQ
from utilities.logging_config import successlogger, errlogger
from time import time, sleep
from python_settings import settings

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
        path,
        all_sources) AS hashes,
      STRUCT(tcia_src AS tcia,
        path_src AS path) AS sources,
      STRUCT(tcia_revised AS tcia,
        path_revised AS path) AS revised
    FROM
      EXTERNAL_QUERY ( '{args.federated_query}',
        '''SELECT version, previous_version, min_timestamp, max_timestamp, done, 
            is_new, expanded, (hashes).tcia, (hashes).path, (hashes).all_sources, 
            (sources).tcia AS tcia_src, (sources).path AS path_src, (revised).tcia AS tcia_revised, 
            (revised).path AS path_revised 
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
        path_hash,
        all_hash) AS hashes,
      STRUCT(tcia_src AS tcia,
        path_src AS path) AS sources,
      STRUCT(tcia_rev AS tcia,
        path_rev AS path) AS revised
    FROM
      EXTERNAL_QUERY ( '{args.federated_query}',
        '''SELECT collection_id, idc_collection_id, uuid, min_timestamp, max_timestamp, 
            init_idc_version, rev_idc_version, final_idc_version, done, is_new, expanded, 
            (hashes).tcia AS tcia_hash, (hashes).path AS path_hash, (hashes).all_sources AS all_hash, 
            (sources).tcia AS tcia_src, (sources).path AS path_src, (revised).tcia AS tcia_rev, 
            (revised).path AS path_rev 
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
        path_hash,
        all_hash) AS hashes,
      STRUCT(tcia_src AS tcia,
        path_src AS path) AS sources,
      STRUCT(tcia_rev AS tcia,
        path_rev AS path) AS revised
    FROM
      EXTERNAL_QUERY ( '{args.federated_query}',
        '''SELECT submitter_case_id, idc_case_id, uuid, min_timestamp, max_timestamp, 
            init_idc_version, rev_idc_version, final_idc_version, done, is_new, expanded, 
            (hashes).tcia AS tcia_hash, (hashes).path AS path_hash, (hashes).all_sources AS all_hash, 
            (sources).tcia AS tcia_src, (sources).path AS path_src, (revised).tcia AS tcia_rev, 
            (revised).path AS path_rev 
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
        path_hash,
        all_hash) AS hashes,
      STRUCT(tcia_src AS tcia,
        path_src AS path) AS sources,
      STRUCT(tcia_rev AS tcia,
        path_rev AS path) AS revised
    FROM
      EXTERNAL_QUERY ( '{args.federated_query}',
        '''SELECT study_instance_uid, uuid, study_instances, min_timestamp, 
            max_timestamp, init_idc_version, rev_idc_version, final_idc_version, 
            done, is_new, expanded, (hashes).tcia AS tcia_hash, (hashes).path AS path_hash, 
            (hashes).all_sources AS all_hash, (sources).tcia AS tcia_src, 
            (sources).path AS path_src, (revised).tcia AS tcia_rev, (revised).path AS path_rev 
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
        path_hash,
        all_hash) AS hashes,
      STRUCT(tcia_src AS tcia,
        path_src AS path) AS sources,
      STRUCT(tcia_rev AS tcia,
        path_rev AS path) AS revised,
      source_url,
      excluded
    FROM
      EXTERNAL_QUERY ( '{args.federated_query}',
        '''SELECT series_instance_uid, uuid, series_instances, source_doi, 
            min_timestamp, max_timestamp, init_idc_version, rev_idc_version, 
            final_idc_version, done, is_new, expanded, (hashes).tcia AS tcia_hash, 
            (hashes).path AS path_hash, (hashes).all_sources AS all_hash, 
            (sources).tcia AS tcia_src, (sources).path AS path_src, 
            (revised).tcia AS tcia_rev, (revised).path AS path_rev,
            source_url, excluded
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




