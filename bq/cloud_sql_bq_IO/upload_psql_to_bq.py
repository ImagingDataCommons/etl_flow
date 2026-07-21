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
      `hash`,
      CAST(previous_version AS INT) AS previous_version,
      min_timestamp,
      max_timestamp,
      done,
      is_new,
      expanded,
      revised
    FROM
      EXTERNAL_QUERY ( '{args.federated_query}',
        '''SELECT version, previous_version, min_timestamp, max_timestamp, done, hash,
            is_new, expanded, revised
        FROM {table}''')
    ORDER BY {order_by}
    """
    result=query_BQ(client, settings.BQ_DEV_INT_DATASET, table, sql, write_disposition='WRITE_TRUNCATE')
    return result


def upload_collection(client, args, table, order_by):
    sql = f"""
    SELECT
      collection_name,
      idc_collection_uuid,
      uuid,
      min_timestamp,
      max_timestamp,
      CAST(init_idc_version AS INT) AS init_idc_version,
      CAST(rev_idc_version AS INT) AS rev_idc_version,
      CAST(final_idc_version AS INT) AS final_idc_version,
      done,
      is_new,
      expanded,
      `hash`,
      revised,
      redacted,
      mitigation
    FROM
      EXTERNAL_QUERY ( '{args.federated_query}',
        '''SELECT collection_name, idc_collection_uuid, uuid, min_timestamp, max_timestamp, 
            init_idc_version, rev_idc_version, final_idc_version, done, is_new, expanded, 
            hash,
            revised, redacted, mitigation
        FROM {table}''')
    ORDER BY {order_by}
    """
    result=query_BQ(client, settings.BQ_DEV_INT_DATASET, table, sql, write_disposition='WRITE_TRUNCATE')
    return result

def upload_patient(client, args, table, order_by):

    sql = f"""
    SELECT
      patientid,
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
      `hash`,
      revised,
      redacted,
      mitigation
    FROM
      EXTERNAL_QUERY ( '{args.federated_query}',
        '''SELECT patientid, idc_case_id, uuid, min_timestamp, max_timestamp, 
            init_idc_version, rev_idc_version, final_idc_version, done, is_new, expanded, 
            hash,
            revised, redacted, mitigation
        FROM {table}''')
    ORDER BY {order_by}
    """
    result=query_BQ(client, settings.BQ_DEV_INT_DATASET, table, sql, write_disposition='WRITE_TRUNCATE')
    return result

def upload_study(client, args, table, order_by):
    sql = f"""
    SELECT
      studyinstanceuid,
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
      `hash`,
      revised,
      redacted,
      mitigation
    FROM
      EXTERNAL_QUERY ( '{args.federated_query}',
        '''SELECT studyinstanceuid, uuid, study_instances, min_timestamp, 
            max_timestamp, init_idc_version, rev_idc_version, final_idc_version, 
            done, is_new, expanded, hash, 
            revised,
            redacted, mitigation
        FROM {table}''')
    ORDER BY {order_by}
    """
    result=query_BQ(client, settings.BQ_DEV_INT_DATASET, table, sql, write_disposition='WRITE_TRUNCATE')
    return result


def upload_series(client, args, table, order_by):
    sql = f"""
    SELECT
      seriesinstanceuid,
      uuid,
      CAST(series_instances AS INT) AS series_instances,
      source_doi,
      source_url,
      versioned_source_doi,
      min_timestamp,
      max_timestamp,
      CAST(init_idc_version AS INT) AS init_idc_version,
      CAST(rev_idc_version AS INT) AS rev_idc_version,
      CAST(final_idc_version AS INT) AS final_idc_version,
      done,
      is_new,
      expanded,
      `hash`,
      revised,
      excluded,
      collection_type source_type,
      redacted, 
      mitigation
      
    FROM
      EXTERNAL_QUERY ( '{args.federated_query}',
        '''SELECT seriesinstanceuid, uuid, series_instances, source_doi, source_url, versioned_source_doi, 
            min_timestamp, max_timestamp, init_idc_version, rev_idc_version, 
            final_idc_version, done, is_new, expanded, hash, 
            revised,
            excluded, 
            collection_type,
            redacted, mitigation
        FROM {table}''')
    ORDER BY {order_by}
    """
    result=query_BQ(client, settings.BQ_DEV_INT_DATASET, table, sql, write_disposition='WRITE_TRUNCATE')
    return result


def upload_instance(client, args, table, order_by):
    sql = f"""SELECT
      sopinstanceuid,
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
      timestamp,
      excluded,
      redacted,
      mitigation,
      ingestion_url, 
      source_file_hash
    FROM
      EXTERNAL_QUERY ( '{args.federated_query}',
        '''SELECT sopinstanceuid, uuid, hash, size, revised, done, is_new, 
            expanded, init_idc_version, rev_idc_version, final_idc_version, 
            timestamp, excluded, redacted,
            mitigation, ingestion_url, source_file_hash
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
        if type(result) != bigquery.Table:
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