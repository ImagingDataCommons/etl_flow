#
# Copyright 2015-2024, Institute for Systems Biology
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

import argparse
import json
import settings

from utilities.logging_config import successlogger, progresslogger,errlogger
from google.cloud import bigquery
from google.api_core.exceptions import NotFound


def skip(args):
    progresslogger.info("Skipped stage")
    return

def schema_has(schema, field_name):
    return next((row for row in schema if row.name == field_name),-1) != -1


def get_table_hash(table_id):

    client = bigquery.Client()
    try:
        table = client.get_table(table_id)
    except NotFound:
        progresslogger.info(f'{table_id} not found')
        return ""

    query = f"""
    WITH no_urls AS (
        SELECT * 
        FROM `{table}`
    )
    SELECT BIT_XOR(DISTINCT FARM_FINGERPRINT(TO_JSON_STRING(t))) as table_hash
    FROM no_urls  AS t
    """

    table_hash =  [dict(row) for row in client.query(query)][0]['table_hash']
    return table_hash

    # return result.json()


def validate_dataset(args, dones, table_ids={}):
    for table_name in table_ids:
        table1 = f'{args.src_project}.{args.src_dataset}.{table_name}'
        table2 = f'{args.trg_project}.{args.trg_dataset}.{table_name}'

        if table2 not in dones:
            ref_hash = get_table_hash(
                table1,
            )
            # Validate the view in prod
            test_hash = get_table_hash( \
                table2,
            )
            if str(ref_hash) == str(test_hash):
                successlogger.info(table2)
            else:
                errlogger.error(table2)
        else:
            progresslogger.info(f'{table2} previously validated')

    return

if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version number')
    parser.add_argument('--dev_project', default=settings.DEV_PROJECT,
                        help="Project containing mitigation dataset")
    parser.add_argument('--range', default=[15, 18], help='Range of versions over which to validate')
    args = parser.parse_args()

    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    dones = set(open(f'{successlogger.handlers[0].baseFilename}').read().splitlines())

    for version in range(args.range[0], args.range[1] + 1):
        if version in (1, 2, 3, 4, 5, 6, 7):
            table_ids = {
                "auxiliary_metadata": "TABLE",
                "dicom_derived_all": "TABLE",
                "dicom_metadata": "TABLE",
            }

        elif version in (8, 9):
            table_ids = {
                "auxiliary_metadata": "TABLE",
                "dicom_derived_all": "TABLE",
                "dicom_metadata": "TABLE",
            }

        elif version == 10:
            table_ids = {
                "auxiliary_metadata": "TABLE",
                "dicom_all": "TABLE",
                "dicom_derived_all": "TABLE",
                "dicom_metadata": "TABLE",
                "dicom_metadata_curated": "TABLE",
                "measurement_groups": "TABLE",
                "qualitative_measurements": "TABLE",
                "quantitative_measurements": "TABLE",
                "segmentations": "TABLE",
            }

        elif version in (11, 12):
            table_ids = {
                "auxiliary_metadata": "TABLE",
                "dicom_all": "TABLE",
                "dicom_derived_all": "TABLE",
                "dicom_metadata": "TABLE",
                "dicom_metadata_curated": "TABLE",
                "measurement_groups": "TABLE",
                "qualitative_measurements": "TABLE",
                "quantitative_measurements": "TABLE",
                "segmentations": "TABLE",
            }

        elif version == 13:
            table_ids = {
                "auxiliary_metadata": "TABLE",
                "dicom_all": "TABLE",
                "dicom_derived_all": "TABLE",
                "dicom_metadata": "TABLE",
                "dicom_metadata_curated": "TABLE",
                "measurement_groups": "TABLE",
                "qualitative_measurements": "TABLE",
                "quantitative_measurements": "TABLE",
                "segmentations": "TABLE",
            }

        elif version == 14:
            table_ids = {
                "auxiliary_metadata": "TABLE",
                "dicom_all": "TABLE",
                "dicom_derived_all": "TABLE",
                "dicom_metadata": "TABLE",
                "dicom_metadata_curated": "TABLE",
                "measurement_groups": "TABLE",
                "qualitative_measurements": "TABLE",
                "quantitative_measurements": "TABLE",
                "segmentations": "TABLE",
            }

        elif version in (15,16):
            table_ids = {
                "auxiliary_metadata": "TABLE",
                "dicom_all": "TABLE",
                "dicom_derived_all": "TABLE",
                "dicom_metadata": "TABLE",
                "dicom_metadata_curated": "TABLE",
                "measurement_groups": "TABLE",
                "mutable_metadata": "TABLE",
                "qualitative_measurements": "TABLE",
                "quantitative_measurements": "TABLE",
                "segmentations": "TABLE",
            }

        elif version in (17,18):
            table_ids = {
                "auxiliary_metadata": "TABLE",
                "dicom_all": "TABLE",
                "dicom_metadata": "TABLE",
                "dicom_metadata_curated": "TABLE",
                "dicom_pivot": "TABLE",
                "measurement_groups": "TABLE",
                "mutable_metadata": "TABLE",
                "qualitative_measurements": "TABLE",
                "quantitative_measurements": "TABLE",
                "segmentations": "TABLE",
            }
        else:
            errlogger.error(f'This script needs to be extended for version {version}')

        args.src_project = settings.PDP_PROJECT
        if version <= 16:
            args.trg_project = 'bigquery-public-data'
        else:
            args.trg_project = settings.AH_PROJECT

        args.src_dataset = f'idc_v{version}'
        args.trg_dataset = f'idc_v{version}'

        validate_dataset(args, dones, table_ids)