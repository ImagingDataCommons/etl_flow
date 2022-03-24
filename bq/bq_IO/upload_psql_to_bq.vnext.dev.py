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

import os
import logging
from logging import INFO
import argparse
from python_settings import settings
from bq.bq_IO.upload_psql_to_bq import upload_to_bq, upload_version, upload_collection, upload_patient, upload_study, \
    upload_series, upload_instance, upload_table


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=9, help='Version to upload')
    args = parser.parse_args()
    parser.add_argument('--db', default=f'idc_v{args.version}', help="Database to access")
    parser.add_argument('--project', default='idc-dev-etl')
    args = parser.parse_args()
    # parser.add_argument('--bqdataset_name', default=f"idc_v{args.version}_dev", help="BQ dataset of table")
    parser.add_argument('--bqdataset_name', default=f"whc_dev", help="BQ dataset of table")
    parser.add_argument('--federated_query', default=f'idc-dev-etl.us.etl_federated_query_idc_v{args.version}')
    parser.add_argument('--tables', default= {
        # 'analysis_id_map': {"func": upload_table, "order_by": "collection_id"},
        # 'collection_id_map': {"func": upload_table, "order_by": "idc_webapp_collection_id"},
        # 'version': {"func":upload_version, "order_by":"version"},
        # 'version_collection': {"func": upload_table, "order_by": "version"},
        # 'collection': {"func":upload_collection, "order_by":"collection_id"},
        # 'collection_patient': {"func": upload_table, "order_by": "collection_uuid"},
        # 'patient': {"func":upload_patient, "order_by":"submitter_case_id"},
        # 'patient_study': {"func": upload_table, "order_by": "patient_uuid"},
        # 'study': {"func":upload_study, "order_by":"study_instance_uid"},
        # 'study_series': {"func": upload_table, "order_by": "study_uuid"},
        # 'series': {"func":upload_series, "order_by":"series_instance_uid"},
        # 'series_instance': {"func": upload_table, "order_by": "series_uuid"},
        # 'instance': {"func":upload_instance, "order_by":"sop_instance_uid"},
        # 'cr_collections': {"func": upload_table, "order_by": "tcia_api_collection_id"},
        # 'defaced_collections': {"func": upload_table, "order_by": "tcia_api_collection_id"},
        # 'excluded_collections': {"func": upload_table, "order_by": "tcia_api_collection_id"},
        # 'open_collections': {"func": upload_table, "order_by": "tcia_api_collection_id"},
        # 'redacted_collections': {"func": upload_table, "order_by": "tcia_api_collection_id"},
        # 'program': {"func": upload_table, "order_by": "tcia_wiki_collection_id"},
        'non_tcia_collection_metadata':{"func": upload_table, "order_by": "idc_webapp_collection_id"},
    }, help="Tables to upload")
    parser.add_argument('--server', default='CLOUD')
    parser.add_argument('--user', default=settings.CLOUD_USERNAME)
    parser.add_argument('--password', default=settings.CLOUD_PASSWORD)
    parser.add_argument('--host', default=settings.CLOUD_HOST)
    parser.add_argument('--port', default=settings.CLOUD_PORT)
    args = parser.parse_args()

    print('args: {}'.format(args))


    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/copy_staging_log.log'.format(os.environ['PWD']))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/copy_staging_err.log'.format(os.environ['PWD']))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    upload_to_bq(args)





