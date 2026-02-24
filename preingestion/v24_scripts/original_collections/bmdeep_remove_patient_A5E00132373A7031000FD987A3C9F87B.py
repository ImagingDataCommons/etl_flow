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
import sys
import argparse
from google.cloud import bigquery

from python_settings import settings
from preingestion.preingestion_code.remove_list_of_instances import perform_partial_deletion
from google.cloud import storage

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    args.client=storage.Client()

    client = bigquery.Client()
    query = f"""
SELECT DISTINCT sop_instance_uid
FROM `idc-dev-etl.idc_v23_dev.all_joined_public_and_current`
WHERE submitter_case_id = 'A5E00132373A7031000FD987A3C9F87B'
    """

    instances = [row.sop_instance_uid for row in client.query(query).result()]
    perform_partial_deletion(args, instances)
