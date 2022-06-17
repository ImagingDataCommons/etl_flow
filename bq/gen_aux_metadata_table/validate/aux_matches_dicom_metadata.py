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

# Validate that auxiliary_metadata and dicom_metadata have the same SOPInstanceUIDs

import settings
import argparse
import sys
from google.cloud import bigquery
from utilities.logging_config import successlogger, errlogger

def compare_tables(args):
    client = bigquery.Client()
    query = f"""
    SELECT count(aux.SOPInstanceUID) aux_cnt, count(dm.SOPInstanceUID) as dm_cnt
    FROM `{args.project }.{args.dataset}.dicom_metadata` dm
    FULL OUTER JOIN `{args.project }.{args.dataset}.auxiliary_metadata` aux
    ON dm.SOPInstanceUID = aux.SOPInstanceUID
    WHERE dm.SOPInstanceUID is NULL or aux.SOPInstanceUID is null

    """

    results = client.query(query)

    if list(results)[0].aux_cnt == 0 and list(results)[0].dm_cnt == 0:
        successlogger.info("SOPInstanceUIDs match")
    else:
        errlogger.error("SOPInstanceUIDs do not match")
    return

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    compare_tables()