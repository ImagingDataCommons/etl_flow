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

# This script deletes redacted instances from specified BQ tables. 
# The script depends on the existence of a BQ tables that enumerates the redacted instances.

import settings
import argparse
import json
from utilities.logging_config import successlogger, progresslogger, errlogger
from google.cloud import bigquery
import datetime

# Generate a table of the hashes that changed due to deleting instances
def gen_hash_table(args):
    client = bigquery.Client() 
    query = f"""
    WITH redacted AS (
      SELECT DISTINCT uuid as i_uuid, `hash` instance_hash
      FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.instance` i
      WHERE redacted=TRUE
    )
    SELECT DISTINCT
        v.version,
        if(c.hashes.all_hash='','', c.collection_id) AS collection_id,
        c.hashes.all_hash AS collection_hash,
        c.rev_idc_version as c_rev_idc_version,
        c.final_idc_version as c_final_idc_version,
        if(p.hashes.all_hash='','', p.submitter_case_id) AS submitter_case_id,
        p.hashes.all_hash AS patient_hash,
        p.rev_idc_version as p_rev_idc_version,
        p.final_idc_version as p_final_idc_version,
         if(st.hashes.all_hash='','', st.study_instance_uid) AS study_instance_uid,
        st.hashes.all_hash AS study_hash,
        st.rev_idc_version as st_rev_idc_version,
        st.final_idc_version as st_final_idc_version,
        if(se.hashes.all_hash='','', se.series_instance_uid) AS series_instance_uid,
        se.hashes.all_hash AS series_hash,
        se.rev_idc_version as se_rev_idc_version,
        se.final_idc_version as se_final_idc_version,
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.version` v
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.version_collection` vc ON v.version = vc.version
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.collection` c ON vc.collection_uuid = c.uuid
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.collection_patient` cp ON c.uuid = cp.collection_uuid
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.patient` p ON cp.patient_uuid = p.uuid
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.patient_study` ps ON p.uuid = ps.patient_uuid
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.study` st ON ps.study_uuid = st.uuid
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.study_series` ss ON st.uuid = ss.study_uuid
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.series` se ON ss.series_uuid = se.uuid
     JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.series_instance` si ON se.uuid = si.series_uuid
     JOIN redacted ON si.instance_uuid = redacted.i_uuid
     ORDER BY v.version, collection_id, submitter_case_id, study_instance_uid, series_instance_uid
    """
    destination = f'{settings.DEV_PROJECT}.mitigation.hashes'
    job_config = bigquery.QueryJobConfig(
        destination=destination,
        write_disposition='WRITE_TRUNCATE'
    )
    try:
        result = client.query(query, job_config=job_config).result()
        successlogger.info(f'Created table {destination}')
    except Exception as exc:
        breakpoint()
    return


def update_hashes(args, table_ids):
    client = bigquery.Client()

    job_config = bigquery.QueryJobConfig(dry_run=args.dry_run)
    for table in table_ids:
        # Update series hashes
        query = f"""
UPDATE `{args.trg_project}.{args.trg_dataset}.{table}` t
SET series_hash = hashes.series_hash 
FROM `{args.trg_project}.mitigation.hashes` hashes
WHERE hashes.version = {version}
AND t.SeriesInstanceUID = hashes.series_instance_uid
"""
        result = client.query(query, job_config=job_config)
        while result.state != 'DONE':
            result = client.get_job(result.job_id)
        if result.error_result != None:
            breakpoint()
        else:
            successlogger.info(f'\tRevised series hashes for {args.trg_project}.{args.trg_dataset}.{table}')

        # Update study hashes
        query = f"""
UPDATE `{args.trg_project}.{args.trg_dataset}.{table}` t
SET study_hash = hashes.study_hash 
FROM `{args.trg_project}.mitigation.hashes` hashes
WHERE hashes.version = {version}
AND t.StudyInstanceUID = hashes.study_instance_uid
"""
        result = client.query(query, job_config=job_config)
        while result.state != 'DONE':
            result = client.get_job(result.job_id)
        if result.error_result != None:
            breakpoint()
        else:
            successlogger.info(f'\tRevised study hashes for {args.trg_project}.{args.trg_dataset}.{table}')

        # Update patient hashes
        query = f"""
UPDATE `{args.trg_project}.{args.trg_dataset}.{table}` t
SET patient_hash = hashes.patient_hash 
FROM `{args.trg_project}.mitigation.hashes` hashes
WHERE hashes.version = {version}
AND t.{'submitter_case_id' if table=='auxiliary_metadata' else 'PatientID'} = hashes.submitter_case_id
"""
        result = client.query(query, job_config=job_config)
        while result.state != 'DONE':
            result = client.get_job(result.job_id)
        if result.error_result != None:
            breakpoint()
        else:
            successlogger.info(f'\tRevised patient hashes for {args.trg_project}.{args.trg_dataset}.{table}')

        # Update collection hashes
        query = f"""
UPDATE `{args.trg_project}.{args.trg_dataset}.{table}` t
SET collection_hash = hashes.collection_hash 
FROM `{args.trg_project}.mitigations.{args.mitigation_id}_hashes` hashes
WHERE hashes.version = {version}
AND t.{args.collection_id} = hashes.collection_id
"""
        result = client.query(query, job_config=job_config)
        while result.state != 'DONE':
            result = client.get_job(result.job_id)
        if result.error_result != None:
            breakpoint()
        else:
            successlogger.info(f'\tRevised collection hashes for {args.trg_project}.{args.trg_dataset}.{table}')
    return


if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version number')
    parser.add_argument('--dev_project', default=settings.DEV_PROJECT, help="Project containing mitigation dataset")
    parser.add_argument('--range', default = [1,18], help='Range of versions over which to clone')
    parser.add_argument('--dry_run', default=False, help='Perform a BQ dry run')
    args = parser.parse_args()

    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')
    gen_hash_table(args)
    for project in (settings.DEV_PROJECT, settings.PDP_PROJECT):
        args.trg_project = project
        for version in range(args.range[0], args.range[1]+1):
            progresslogger.info(f'Version {version}')
            if version <=7:
                table_ids = (
                    "auxiliary_metadata",
                )

                args.trg_dataset = f'idc_v{version}'
            elif version <= 9:
                table_ids = (
                    "auxiliary_metadata",
                )
                args.trg_dataset = f'idc_v{version}_pub'
            elif version <=18:
                table_ids = (
                    "auxiliary_metadata",
                    "dicom_all"
                )
                args.trg_dataset = f'idc_v{version}_pub'
            else:
                errlogger.error("Revise this script for newer versions")

            if version <= 15:
                args.collection_id = 'tcia_api_collection_id'
            else:
                args.collection_id = 'collection_name'

            update_hashes(args, table_ids=table_ids)


