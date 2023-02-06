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

"""
Add an aws_url column to auxiliary_metadata.
"""

import settings
import argparse
import json
import time
from google.cloud import bigquery
from utilities.logging_config import successlogger, progresslogger, errlogger




def revise_gcs_urls(args):
    client = bigquery.Client()
    table_schema = client.get_table(f'{args.project}.{args.dataset}.auxiliary_metadata').schema  # Make an API request.
    # Determine whether this version of auxiliary_metadata has a gcs_bucket column
    gcs_bucket = next((item for item in table_schema if item.name == 'gcs_bucket'),-1) != -1
    if gcs_bucket:
        query = f"""
        UPDATE `{args.project}.{args.dataset}.auxiliary_metadata` am
        SET 
        am.gcs_url = IF('{args.dev_or_pub}'='dev', uum.dev_gcs_url, uum.pub_gcs_url), 
        am.aws_url = IF('{args.dev_or_pub}'='dev', uum.dev_aws_url, uum.pub_aws_url), 
        am.gcs_bucket = IF('{args.dev_or_pub}'='dev', uum.dev_gcs_bucket, uum.pub_gcs_bucket)
        FROM `idc-dev-etl.idc_v14_dev.uuid_url_map` uum
        WHERE am.instance_uuid = uum.uuid
        AND am.series_uuid = uum.se_uuid
        """
    else:
        query = f"""
        UPDATE `{args.project}.{args.dataset}.auxiliary_metadata` am
        SET 
        am.gcs_url = IF('{args.dev_or_pub}'='dev', uum.dev_gcs_url, uum.pub_gcs_url), 
        am.aws_url = IF('{args.dev_or_pub}'='dev', uum.dev_aws_url, uum.pub_aws_url)
        FROM `idc-dev-etl.idc_v14_dev.uuid_url_map` uum
        WHERE am.instance_uuid = uum.uuid
        AND am.series_uuid = uum.se_uuid
         """

    job = client.query(query)
    while not job.done():
        print('Waiting for job done. Status: {}'.format(job.state))
        time.sleep(5)
    progresslogger.info(f'Job error result: {job.error_result}')
    return


def revise_table(args):
    add_aws_url_column(args)
    revise_gcs_urls(args)

if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()

    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version number')
    parser.add_argument('--project', default="idc-dev-etl", help='Project in which tables live')
    # parser.add_argument('--dataset', default=f"idc_v{settings.CURRENT_VERSION}_pub", help="BQ dataset")
    parser.add_argument('--dataset', default=f"whc_dev_idc_v13_pub", help="BQ dataset")
    parser.add_argument('--uuid_url_map', default="idc-dev-etl.idc_v14_dev.uuid_url_map",
                        help="Table that maps instance uuids to URLS")
    parser.add_argument('--dev_or_pub', default='pub', help='Revising the dev or pub version of auxiliary_metadata')
    args = parser.parse_args()

    print(f'args: {json.dumps(args.__dict__, indent=2)}')

    revise_table(args)
