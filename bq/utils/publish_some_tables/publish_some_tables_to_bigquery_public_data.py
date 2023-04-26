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

# Copy some tables to another project
import settings
import argparse
import json
from utilities.logging_config import successlogger, progresslogger, errlogger
from publish_some_tables import publish_tables


if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()
    parser.add_argument('--src_project', default="idc-pdp-staging", help='Project from which tables are copied')
    parser.add_argument('--trg_project', default="bigquery-public-data", help='Project to which tables are copied')
    args = parser.parse_args()

    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')
    dones = open(f'{successlogger.handlers[0].baseFilename}').read().splitlines()
    errors = [row.split(':')[-1] for row in open(f'{errlogger.handlers[0].baseFilename}').read().splitlines()]

    tables = [
        ('dicom_metadata', 1, 1),
        ('dicom_derived_all', 2, 7),
        ('dicom_derived_all', 13, 13),
        ('measurement_groups', 13, 13),
        ('qualitative_measurements', 13, 13),
        ('quantitative_measurements', 13, 13)
    ]

    for table_name, min_version, max_version in tables:

        publish_tables(args, table_name, min_version, max_version, dones)
