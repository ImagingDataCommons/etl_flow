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

# Verify that StudyInstanceUIDs, SeriesInstanceUIDs and SOPInstanceUIDs in the
# database (as represented by BQ tables) are unique.

import argparse
import settings
import psycopg2
from google.cloud import bigquery
from utilities.logging_config import successlogger,progresslogger, errlogger

def validate_sopinstanceuid_uniqueness(args):
    client = bigquery.Client()
    query = f"""
    WITH p AS (
    SELECT DISTINCT series_instance_uid, sop_instance_uid 
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined` 
    ),
    d AS (SELECT sop_instance_uid, COUNT(series_instance_uid) count FROM p
    GROUP BY sop_instance_uid
    HAVING count>1)
    SELECT p.*
    FROM p
    JOIN d
    ON p.sop_instance_uid=d.sop_instance_uid
    """
    dups = [row for row in client.query(query)]
    if not dups:
        successlogger.info(f'DICOM SOPInstanceUIDs are unique')
    else:
        errlogger.error(f'DICOM SOPInstanceUIDs are not unique')
        errlogger.error(f'SOPInstanceUID    Count')
        for row in dups:
            errlogger.error(f'{row.sop_instance_uid}   {row.count}')


def validate_seriesinstanceuid_uniqueness(args):
    client = bigquery.Client()
    query = f"""
    WITH p AS (
    SELECT DISTINCT study_instance_uid, series_instance_uid 
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined` 
    ),
    d AS (SELECT series_instance_uid, COUNT(study_instance_uid) count FROM p
    GROUP BY series_instance_uid
    HAVING count>1)
    SELECT p.*
    FROM p
    JOIN d
    ON p.series_instance_uid=d.series_instance_uid
    """
    dups = [row for row in client.query(query)]
    if not dups:
        successlogger.info(f'DICOM SeriesInstanceUIDs are unique')
    else:
        errlogger.error(f'DICOM SeriesInstanceUIDs are not unique')
        errlogger.error(f'SeriesInstanceUID    Count')
        for row in dups:
            errlogger.error(f'{row.series_instance_uid}   {row.count}')


def validate_studyinstanceuid_uniqueness(args):
    client = bigquery.Client()
    query = f"""
    WITH p AS (
    SELECT DISTINCT submitter_case_id, study_instance_uid 
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined` 
    ),
    d AS (SELECT study_instance_uid, COUNT(submitter_case_id) count FROM p
    GROUP BY study_instance_uid
    HAVING count>1)
    SELECT p.*
    FROM p
    JOIN d
    ON p.study_instance_uid=d.study_instance_uid
    """
    dups = [row for row in client.query(query)]
    if not dups:
        successlogger.info(f'DICOM StudyInstanceUIDs are unique')
    else:
        errlogger.error(f'DICOM StudyInstanceUIDs are not unique')
        errlogger.error(f'StudyInstanceUID    Count')
        for row in dups:
            errlogger.error(f'{row.study_instance_uid}   {row.count}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default=f'idc_v{settings.CURRENT_VERSION}', help='Database on which to operate')
    args = parser.parse_args()

    validate_sopinstanceuid_uniqueness(args)
    validate_seriesinstanceuid_uniqueness(args)
    validate_studyinstanceuid_uniqueness(args)
