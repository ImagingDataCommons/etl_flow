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

# Upload Cloud SQL tables to BQ tables. Add tables not originally uploaded.
# Specifically upload tables documenting IDC sourced table.


import argparse
import settings
from google.cloud import bigquery
from utilities.bq_helpers import BQ_table_exists, delete_BQ_Table, query_BQ
from utilities.logging_config import successlogger, errlogger
from time import time, sleep

def upload_table(client, args, dataset, table, order_by):
    sql = f"""
    SELECT
        *
    FROM
      EXTERNAL_QUERY ( '{args.federated_query}',
        '''SELECT * from {table}''')
    ORDER BY {order_by}
    """
    result=query_BQ(client, dataset, table, sql, write_disposition='WRITE_TRUNCATE')
    return result


def upload_to_bq(args, tables, project, dataset):
    client = bigquery.Client(project=project)
    for table in args.upload:
        successlogger.info(f'Uploading table {table}')
        b = time()

        if BQ_table_exists(client, project, dataset, table):
            delete_BQ_Table(client, project, dataset, table)
        result = tables[table]['func'](client, args, dataset, table, tables[table]['order_by'])
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

tables = {
        'idc_collection': {"func": upload_table, "order_by": "collection_id"},
        'idc_instance': {"func": upload_table, "order_by": "sop_instance_uid"},
        'idc_patient': {"func": upload_table, "order_by": "submitter_case_id"},
        'idc_series': {"func": upload_table, "order_by": "series_instance_uid"},
        'idc_study': {"func": upload_table, "order_by": "study_instance_uid"},
        'wsi_collection': {"func": upload_table, "order_by": "collection_id"},
        'wsi_instance': {"func": upload_table, "order_by": "sop_instance_uid"},
        'wsi_patient': {"func": upload_table, "order_by": "submitter_case_id"},
        'wsi_series': {"func": upload_table, "order_by": "series_instance_uid"},
        'wsi_study': {"func": upload_table, "order_by": "study_instance_uid"},
        'wsi_metadata': {"func": upload_table, "order_by": "sop_instance_uid"},

}

if __name__ == '__main__':
    version = 12
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=version)
    parser.add_argument('--federated_query', default=f'idc-dev-etl.us.etl_federated_query_idc_v{version}')
    parser.add_argument('--upload', nargs='*', default= [
        # 'idc_collection',
        # 'idc_instance',
        # 'idc_patient',
        # 'idc_series',
        # 'idc_study',
        'wsi_collection',
        'wsi_instance',
        'wsi_patient',
        'wsi_series',
        'wsi_study',
        # 'wsi_metadata',
    ], help="Tables to upload")
    args = parser.parse_args()
    print('args: {}'.format(args))

    upload_to_bq(args, tables, settings.DEV_PROJECT, f'idc_v{args.version}_dev')





