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
# Duplicate idc_vxx datasets in idc_pdp_staging.
import settings
import argparse
import json
from utilities.logging_config import successlogger, progresslogger, errlogger
from google.cloud import bigquery


def delete_datasets(args):
    client = bigquery.Client()
    datasets = [row for row in client.list_datasets(args.trg_project)]
    for dataset in datasets:
        if dataset.dataset_id.startswith(args.trg_dataset_prefix):
            client.delete_dataset(dataset, delete_contents=True)
    return



if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version number')
    parser.add_argument('--trg_project', default="idc-dev-etl", help='Project to which tables are copied')
    parser.add_argument('--trg_dataset_prefix', default=f"time_travel_", help="Prefix to prepend to trg dataset names")
    args = parser.parse_args()

    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')
    delete_datasets(args)
