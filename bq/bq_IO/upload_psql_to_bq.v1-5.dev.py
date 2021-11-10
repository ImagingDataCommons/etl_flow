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

# Upload certain tables to all BQ dataset versions.
# These tables define how collections are distributed among
# various buckets.
# The open_collections view defines the collections that are not
# listed in any of these tables. It is populated into the BQ
# datasets separately.

import os
import logging
from logging import INFO
import argparse
from python_settings import settings
from bq.bq_IO.upload_psql_to_bq import upload_to_bq


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', default='idc-dev-etl')
    parser.add_argument('--tables', default= [
                    # 'excluded_collections',
                    # 'redacted_collections',
                    # 'cr_collections',
                    'defaced_collections',
                    # 'open_collections'
                    ], help="Tables to upload")
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

    for version in range(1,6):
        print(f"Version {version}")
        args.version=version
        args.db = 'idc_v5' # Currently these tables are only in idc_v5
        args.bqdataset_name = f'idc_v{version}'
        upload_to_bq(args)





