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
Script to empty the PDP staging bucket, which is a "delta" bucket
containing only the instances that are new to a version. Therefore
it must be emptied before the instances for the next version are
copied to it.
"""

import argparse
import os
import logging
# from logging import INFO
from gcs.empty_bucket_mp.empty_bucket_mp import pre_delete
# proglogger = logging.getLogger('root.prog')
# successlogger = logging.getLogger('root.success')
# errlogger = logging.getLogger('root.err')


from python_settings import settings
import settings as etl_settings

if not settings.configured:
    settings.configure(etl_settings)
assert settings.configured


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', default='idc-open-pdp-staging')
    parser.add_argument('--processes', default=32, help="Number of concurrent processes")
    parser.add_argument('--batch', default=1000, help='Size of batch assigned to each process')
    parser.add_argument('--project', default='idc-pdp-staging')
    parser.add_argument('--log_dir', default=f'/mnt/disks/idc-etl/logs/empty_pdp_staging_bucket_mp')

    args = parser.parse_args()

    # # if not os.path.exists('{}'.format(args.log_dir)):
    # #     os.mkdir('{}'.format(args.log_dir))
    #
    # proglogger = logging.getLogger('root.prog')
    # prog_fh = logging.FileHandler(f'{os.environ["PWD"]}/logs/bucket.log')
    # progformatter = logging.Formatter('%(levelname)s:prog:%(message)s')
    # proglogger.addHandler(prog_fh)
    # prog_fh.setFormatter(progformatter)
    # proglogger.setLevel(INFO)
    #
    # successlogger = logging.getLogger('root.success')
    # successlogger.setLevel(INFO)
    #
    # errlogger = logging.getLogger('root.err')

    pre_delete(args)
