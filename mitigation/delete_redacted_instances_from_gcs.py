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

# Backup instance to be redacted to the mitigation project
import argparse
from utilities.logging_config import successlogger, progresslogger, errlogger

import google
from google.cloud import storage, bigquery
from google.auth.transport import requests
import settings

from utilities.logging_config import successlogger, progresslogger, errlogger

def delete_instance(args, dicomweb_session, study_instance_uid, series_instance_uid, sop_instance_uid):
    # URL to the Cloud Healthcare API endpoint and version
    base_url = "https://healthcare.googleapis.com/v1"
    url = "{}/projects/{}/locations/{}".format(base_url, settings.PUB_PROJECT, settings.GCH_REGION)
    dicomweb_path = "{}/datasets/{}/dicomStores/{}/dicomWeb/studies/{}/series/{}/instances/{}".format(
        url, settings.GCH_DATASET, settings.GCH_DICOMSTORE, study_instance_uid, series_instance_uid, sop_instance_uid
    )
    # Set the required application/dicom+json; charset=utf-8 header on the request
    headers = {"Content-Type": "application/dicom+json; charset=utf-8"}

    response = dicomweb_session.delete(dicomweb_path, headers=headers)
    retries = 3
    while retries:
        if response.status_code == 200:
            successlogger.info(sop_instance_uid)
            return
        else:
            retries -= 1
    errlogger.error(sop_instance_uid)


def get_redactions(version):
    client = bigquery.Client()
    query = f"""
    SELECT DISTINCT study_instance_uid, series_instance_uid , sop_instance_uid
    FROM `{settings.DEV_PROJECT}.mitigation.redactions`
    WHERE i_rev_idc_version<={version}
    AND ({version}<=i_final_idc_version) OR i_final_idc_version=0)
    """

    try:
        results = [dict(row) for row in client.query(query).result()]
    except Exception as exc:
        errlogger.error(f'Error querying redactions table: {exc} ')
        exit(-1)

    return results

def backup_redactions(args):
    client = storage.Client()
    instances = get_redactions(args)

    scoped_credentials, project = google.auth.default(
        ["https://www.googleapis.com/auth/cloud-platform"]
    )
    # Create a DICOMweb requests Session object with the credentials.
    dicomweb_sess = requests.AuthorizedSession(scoped_credentials)
    for instance in instances:
        delete_instance(args, dicomweb_sess, instance['study_instance_uid'],
                        instance['series_instance_uid'], instance['sop_instance_uid'])
        progresslogger.info(f"{n}: Instance {instance['sop_instance_uid']}  deleted")

if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()
    parser.add_argument('--trg_bucket', default='redacted_instances', help='Bucket to which to backup redacted instances')

    args = parser.parse_args()
    breakpoint() # Not yet debugged
    backup_redactions(args)

