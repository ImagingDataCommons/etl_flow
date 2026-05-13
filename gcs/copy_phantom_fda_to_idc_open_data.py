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

# One time use script to copy Phantom FDA blobs to the


import  json
from gcs_utilities.copy_blobs_defined_by_bq_query_mp import copy_all_blobs
import argparse
import settings
from utilities.logging_config import successlogger, progresslogger, errlogger

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='Version to work on')
    parser.add_argument("--src_bucket", default='idc-open-cr')
    parser.add_argument("--dst_bucket", default="idc-open-data-staging")
    parser.add_argument("--batch", default=1000)
    parser.add_argument("--processes", default=32)
    args = parser.parse_args()
    args.id = 0 # Default process ID

    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    query = f"""
SELECT DISTINCT CONCAT(se_uuid, "/", i_uuid, ".dcm") blob_name
FROM `idc-dev-etl.idc_v24_dev.all_joined_public_and_current`
WHERE collection_id = "Phantom FDA"
ORDER BY blob_name
    """

    copy_all_blobs(args, query)