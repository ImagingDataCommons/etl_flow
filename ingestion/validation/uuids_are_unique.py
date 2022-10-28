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
# Verify the TCIA DICOM hierarchy.
import argparse
import settings
import psycopg2
from psycopg2.extras import DictCursor

import logging
from utilities.logging_config import successlogger,errlogger

def validate_uuid_uniqueness(args):
    conn = psycopg2.connect(dbname=args.db, user=settings.CLOUD_USERNAME, port=settings.CLOUD_PORT,
                            password=settings.CLOUD_PASSWORD, host=settings.CLOUD_HOST)
    with conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("""
            SELECT COUNT(uuid) as total_uuids, COUNT(DISTINCT uuid) as distinct_uuids 
            FROM (
            SELECT uuid FROM collection
            UNION ALL 
            SELECT uuid FROM patient
            UNION ALL
            SELECT uuid FROM study
            UNION ALL
            SELECT uuid FROM series
            UNION ALL
            SELECT uuid FROM instance) as x
            """)

            counts = cur.fetchone()
            if counts['total_uuids'] == counts['distinct_uuids']:
                print("UUIDs are unique")
                successlogger.info("UUIDs are unique")
            else:
                print("UUIDs are not unique")
                errlogger.error("UUIDs are not unique")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default=f'idc_v{settings.CURRENT_VERSION}', help='Database on which to operate')
    args = parser.parse_args()

    validate_uuid_uniqueness(args)