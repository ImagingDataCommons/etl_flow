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

# Verify that the DB and the DICOM metadata have the same DICOM UIDs.
#
# Note that this script will currently report correctly that the
# DICOM PatientIDs (also called submitter_case_id in the DB) of 300
# NLST instances do not match.

import argparse
import settings
import psycopg2
from google.cloud import bigquery
from utilities.logging_config import successlogger,progresslogger, errlogger

def validate_UIDs_match(args):
    client = bigquery.Client()
    query = f"""
    SELECT distinct aji.collection_id collection_id, aji.submitter_case_id bq_patientID, dm.patientid dm_patientID, 
        aji.Study_Instance_UID bq_study, dm.StudyInstanceUID dm_study, aji.Series_Instance_UID bq_series, 
        dm.SeriesInstanceUID dm_series, aji.SOP_Instance_UID instance 
    FROM `idc-dev-etl.{args.dev_dataset}.all_joined_public_and_current` aji
    JOIN `idc-dev-etl.{args.pub_dataset}.dicom_metadata` dm
    ON aji.SOP_Instance_UID=dm.SOPInstanceUID
    WHERE 
    idc_version = {args.version}
    AND (aji.submitter_case_id!=dm.patientid
    OR aji.Study_Instance_UID!=dm.StudyInstanceUID
    OR aji.Series_Instance_UID!=dm.SeriesInstanceUID)
    """
    dups = [row for row in client.query(query)]
    if not dups:
        successlogger.info(f'DICOM UIDs match')
    else:
        errlogger.error(f'{len(dups)} DICOM UIDs do not match')
        errlogger.error(f'collection_id SOPInstanceUID  submitter_case_id   StudyInstanceUID    SeriesInstanceUID')
        for row in dups:
            patients_match = "Matching" if row.bq_patientID==row.dm_patientID else f'{row.bq_patientID}!={row.dm_patientID}'
            studies_match = "Matching" if row.bq_study==row.dm_study else f'{row.bq_study}!={row.dm_study}'
            series_match = "Matching" if row.bq_series==row.dm_series else f'{row.bq_series}!={row.dm_series}'
            errlogger.error(f'{row.collection_id}   {row.instance}    {patients_match}    {studies_match} {series_match}')
        errlogger.error(f'NOTE: It is expected that 300 NLST instances have mismatching submitter_case_ids')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default=f'idc_v{settings.CURRENT_VERSION}', help='Database on which to operate')
    parser.add_argument('--version', default=settings.CURRENT_VERSION)
    # parser.add_argument('--version', default=14)
    parser.add_argument('--dev_dataset', default=settings.BQ_DEV_INT_DATASET)
    # parser.add_argument('--dev_dataset', default='idc_v14_dev')
    parser.add_argument('--pub_dataset', default=settings.BQ_DEV_EXT_DATASET)
    # parser.add_argument('--pub_dataset', default='idc_v14_pub')
    args = parser.parse_args()

    validate_UIDs_match(args)
