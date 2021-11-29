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

import argparse

from python_settings import settings
import settings as etl_settings
settings.configure(etl_settings)


import psycopg2
from psycopg2.extras import DictCursor


conn = psycopg2.connect(dbname='idc_v6', user=settings.CLOUD_USERNAME, port=settings.CLOUD_PORT,
                        password=settings.CLOUD_PASSWORD, host=settings.CLOUD_HOST)
cur = conn.cursor(cursor_factory=DictCursor)

tables = [
    "instance_mm",
    "series_instance",
    "series_mm",
    "study_series",
    "study_mm",
    "patient_study",
    "patient_mm",
    "collection_patient",
    "collection_mm",
    "version_collection",
    "version_mm",
    ]

for table in tables:
    try:
        query = f"DROP TABLE {table} CASCADE"
        cur.execute(query)
        conn.commit()
    except:
        print(f'Failed to drop {table}')
