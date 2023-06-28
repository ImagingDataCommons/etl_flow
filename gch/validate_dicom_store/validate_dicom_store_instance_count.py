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

# Validate that dicom_metadata, and therefore, a DICOM store has the expected number of instances
from google.cloud import bigquery
import settings
import argparse
from utilities.logging_config import successlogger,errlogger

def validate_dicom_metadata_counts():
    client = bigquery.Client()
    query = f"""
    WITH sopinstanceuids AS (
    SELECT aj.sop_instance_uid as SOPInstanceUID
    FROM `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.all_joined` as aj
    JOIN `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.all_collections` ac
    ON aj.collection_id = ac.tcia_api_collection_id
    WHERE aj.idc_version = {settings.CURRENT_VERSION}
    AND aj.i_excluded is False
    AND ((i_source='tcia' AND ac.tcia_access='Public') OR (i_source='idc' AND ac.idc_access='Public'))
    )
    select count(siu.sopinstanceuid) as siu_cnt, count(dm.sopinstanceuid) as dcm_cnt
    FROM sopinstanceuids as siu
    FULL OUTER JOIN `idc-dev-etl.idc_v{settings.CURRENT_VERSION}_pub.dicom_metadata` as dm
    ON siu.SOPInstanceUID = dm.SOPInstanceUID
    WHERE siu.SOPInstanceUID is NULL or dm.SOPInstanceUID is null
    """

    results = client.query(query)
    if list(results)[0].siu_cnt == 0 and list(results)[0].dcm_cnt == 0:
        successlogger.info("dicom_metadata has the correct SOPInstanceUIDs")
        return 0
    else:
        errlogger.error("Error; SOPInstanceUIDs do not match expected set")
        return -1


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    validate_dicom_metadata_counts()