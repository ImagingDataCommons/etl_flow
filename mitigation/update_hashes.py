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


def update_hashes(args, idc_version, table_ids):
    client = bigquery.Client()

    # Create table of hashes
    query = f"""
CREATE TABLE `{args.trg_project}.{args.trg_dataset}.v{idc_version}_hashes` 
OPTIONS (expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS
SELECT 

"""

    for table in table_ids:
        query = f"""
DELETE FROM `{args.trg_project}.{args.trg_dataset}.{table}` t
WHERE t.SOPInstanceUID IN (
    SELECT DISTINCT sop_instance_uid AS SOPInstanceUID
    FROM `{args.trg_project}.mitigations.{args.mitigation_id}`
    WHERE i_rev_idc_version <= {idc_version}  
        AND ({idc_version} <= i_final_idc_version OR i_final_idc_version = 0 )
)

"""

        result = client.query(query)
        while result.state != 'DONE':
            result = client.get_job(result.job_id)
        if result.error_result != None:
            breakpoint()
        else:
            successlogger.info(f'Redacted instance from {args.trg_project}.{args.trg_dataset}.{table}')

    return


if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version number')
    parser.add_argument('--trg_project', default=settings.DEV_MITIGATION_PROJECT, help='Project to which tables are copied')
    parser.add_argument('--range', default = [1,18], help='Range of versions over which to clone')
    parser.add_argument('--mitigation_id', default='m1', help='ID of this mitigation, name of BQ table enumerating it')
    args = parser.parse_args()

    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')


    query=f"""
WITH redacted AS (
  SELECT DISTINCT uuid as i_uuid, `hash` instance_hash
  FROM `idc-dev-mitigation.idc_v19_dev.instance` i
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


   FROM `idc-dev-mitigation.idc_v19_dev.version` v
     JOIN `idc-dev-mitigation.idc_v19_dev.version_collection` vc ON v.version = vc.version
     JOIN `idc-dev-mitigation.idc_v19_dev.collection` c ON vc.collection_uuid = c.uuid
     JOIN `idc-dev-mitigation.idc_v19_dev.collection_patient` cp ON c.uuid = cp.collection_uuid
     JOIN `idc-dev-mitigation.idc_v19_dev.patient` p ON cp.patient_uuid = p.uuid
     JOIN `idc-dev-mitigation.idc_v19_dev.patient_study` ps ON p.uuid = ps.patient_uuid
     JOIN `idc-dev-mitigation.idc_v19_dev.study` st ON ps.study_uuid = st.uuid
     JOIN `idc-dev-mitigation.idc_v19_dev.study_series` ss ON st.uuid = ss.study_uuid
     JOIN `idc-dev-mitigation.idc_v19_dev.series` se ON ss.series_uuid = se.uuid
     JOIN `idc-dev-mitigation.idc_v19_dev.series_instance` si ON se.uuid = si.series_uuid
     JOIN redacted ON si.instance_uuid = redacted.i_uuid
     ORDER BY v.version, collection_id, submitter_case_id, study_instance_uid, series_instance_uid
"""

    for version in range(args.range[0], args.range[1]+1):
        args.skipped_table_ids = []
        args.table_ids = []
        if version in (1,2,3,4,5,6,7):
            pub_table_ids = (
                "auxiliary_metadata",
                "dicom_metadata",
            )

            args.trg_dataset = f'idc_v{version}'
            update_hashes(args, version, table_ids=pub_table_ids)

        elif version in (8,9):
            pub_table_ids = (
                "auxiliary_metadata",
                "dicom_metadata",
            )
            args.trg_dataset = f'idc_v{version}_pub'
            update_hashes(args, version, table_ids=pub_table_ids)

        elif version == 10:
            pub_table_ids = (
                "auxiliary_metadata",
                "dicom_metadata",
            )
            args.trg_dataset = f'idc_v{version}_pub'
            update_hashes(args, version, table_ids=pub_table_ids)

        elif version in (11,12):
            pub_table_ids = (
                "auxiliary_metadata",
                "dicom_metadata",
            )
            args.trg_dataset = f'idc_v{version}_pub'
            update_hashes(args, version, table_ids=pub_table_ids)

        elif version == 13:
            pub_table_ids = (
                "auxiliary_metadata",
                "dicom_metadata",
            )
            args.trg_dataset = f'idc_v{version}_pub'
            update_hashes(args, version, table_ids=pub_table_ids)

        elif version == 14:
            pub_table_ids = (
                "auxiliary_metadata",
                "dicom_metadata",
            )
            args.trg_dataset = f'idc_v{version}_pub'
            update_hashes(args, version, table_ids=pub_table_ids)

        elif version == 15:
            pub_table_ids = (
                "auxiliary_metadata",
                "dicom_metadata",
            )
            args.trg_dataset = f'idc_v{version}_pub'
            update_hashes(args, version, table_ids=pub_table_ids)

        elif version in (16,17,18):
            pub_table_ids = (
                "auxiliary_metadata",
                "dicom_metadata",
            )
            args.trg_dataset = f'idc_v{version}_pub'
            update_hashes(args, version, table_ids=pub_table_ids)
        else:
            errlogger.error(f'This script needs to be extended for version {version}')
