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

# Upload DB tables used to generate subsequent BQ tables.
# Beginning with idc v8, the per version BQ datasets are split
# into idc_v<version>_dev and idc_v<version>_pub. These tables
# go into the former, generated tables into the latter.

import argparse
import settings
from upload_psql_to_bq import upload_to_bq, upload_version, upload_collection, upload_patient, upload_study, \
    upload_series, upload_instance, upload_table
from google.cloud import bigquery
from utilities.bq_helpers import create_BQ_dataset

tables = {
        'collection': {"func":upload_collection, "order_by":"collection_id"},
        'collection_id_map': {"func": upload_table, "order_by": "idc_webapp_collection_id"},
        'collection_patient': {"func": upload_table, "order_by": "collection_uuid"},
        'idc_collection': {"func": upload_table, "order_by": "collection_id"},
        'idc_instance': {"func": upload_table, "order_by": "sop_instance_uid"},
        'idc_patient': {"func": upload_table, "order_by": "submitter_case_id"},
        'idc_series': {"func": upload_table, "order_by": "series_instance_uid"},
        'idc_study': {"func": upload_table, "order_by": "study_instance_uid"},
        'instance': {"func":upload_instance, "order_by":"sop_instance_uid"},
        'patient': {"func":upload_patient, "order_by":"submitter_case_id"},
        'patient_study': {"func": upload_table, "order_by": "patient_uuid"},
        'series': {"func":upload_series, "order_by":"series_instance_uid"},
        'series_instance': {"func": upload_table, "order_by": "series_uuid"},
        'study': {"func":upload_study, "order_by":"study_instance_uid"},
        'study_series': {"func": upload_table, "order_by": "study_uuid"},
        'version': {"func":upload_version, "order_by":"version"},
        'version_collection': {"func": upload_table, "order_by": "version"},
}

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--federated_query', default=f'idc-dev-etl.us.etl_federated_query_idc_v{settings.CURRENT_VERSION}')
    parser.add_argument('--upload', nargs='*', default= [
        # 'collection',
        # 'collection_id_map',
        # 'collection_patient',
        # 'idc_collection',
        # 'idc_instance',
        # 'idc_patient',
        # 'idc_series',
        # 'idc_study',
        # 'instance',
        # 'patient',
        # 'patient_study',
        'series',
        # 'series_instance',
        # 'study',
        # 'study_series',
        # 'version',
        # 'version_collection',
    ], help="Tables to upload")
    args = parser.parse_args()
    print('args: {}'.format(args))

    # Create BQ datasets.
    BQ_client = bigquery.Client(project=settings.DEV_PROJECT)
    try:
        dataset = create_BQ_dataset(BQ_client, settings.BQ_DEV_INT_DATASET)
    except:
        # Presume the dataset already exists
        pass

    upload_to_bq(args, tables)





