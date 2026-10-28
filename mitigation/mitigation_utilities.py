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

from google.cloud import bigquery, storage
from utilities.logging_config import successlogger, progresslogger, errlogger
import settings
import argparse
import boto3
from collections import Counter

def get_version_range():
    client = bigquery.Client()

    query = f"""
    SELECT min(i_init_idc_version) init_idc_version, max(i_final_idc_version) final_idc_version
    FROM `{settings.DEV_MITIGATION_PROJECT}.m{settings.MITIGATION_VERSION}.redactions`
    """

    try:
        results = [dict(row) for row in client.query(query).result()]
    except Exception as exc:
        errlogger.error(f'Error querying redactions table: {exc} ')
        exit(-1)

    init_idc_version = results[0]['init_idc_version']
    final_idc_version = settings.CURRENT_VERSION if results[0]['final_idc_version'] == 0 else results[0]['final_idc_version']
    return [init_idc_version, final_idc_version]
