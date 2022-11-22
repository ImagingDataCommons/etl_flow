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

# One time script to delete CPTAC pathology from the idc-open-idc1 bucket
# It was previously moved from the idc-dev-defaced bucket to idc-dev-open
# and idc-open-pdpn-staging.
import argparse
from gcs.misc.delete_list_of_blobs.delete_list_of_blobs import del_all_instances
from google.cloud import bigquery
from utilities.logging_config import successlogger, progresslogger, errlogger
from python_settings import settings

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', default='idc-dev-defaced')
    parser.add_argument('--processes', default=16, help="Number of concurrent processes")
    parser.add_argument('--batch', default=100, help='Size of batch assigned to each process')
    parser.add_argument('--project', default='canceridc-data')
    args = parser.parse_args()

    client = bigquery.Client()
    query = f"""
    SELECT distinct i_uuid FROM `idc-dev-etl.idc_v12_dev.all_joined_included` 
    where collection_id in ('CPTAC-CM', 'CPTAC-LSCC') 
    and i_source='idc'
    and i_rev_idc_version<10
    order by i_uuid
    """
    instances = [f'{row.i_uuid}.dcm' for row in client.query(query)]
    del_all_instances  (args, instances)
