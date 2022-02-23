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

# Delete one or more series from a DICOM store
import argparse
from python_settings import settings
import requests

import settings as etl_settings

if not settings.configured:
    settings.configure(etl_settings)
import google
from google.cloud import storage
from google.auth.transport import requests

def delete_series(args, dicomweb_session, study_instance_uid, series_instance_uid):
    # URL to the Cloud Healthcare API endpoint and version
    base_url = "https://healthcare.googleapis.com/v1"
    url = "{}/projects/{}/locations/{}".format(base_url, args.dst_project, args.dataset_region)
    dicomweb_path = "{}/datasets/{}/dicomStores/{}/dicomWeb/studies/{}/series/{}".format(
        url, args.gch_dataset_name, args.dicomstore, study_instance_uid, series_instance_uid
    )
    # Set the required application/dicom+json; charset=utf-8 header on the request
    headers = {"Content-Type": "application/dicom+json; charset=utf-8"}

    response = dicomweb_session.delete(dicomweb_path, headers=headers)
    # response.raise_for_status()
    if response.status_code == 200:
        print(f'{study_instance_uid}/{study_instance_uid} deleted')
    else:
        print(f'{study_instance_uid}/{study_instance_uid} delete failed ')
        print(response.text)


def delete_seriess(args):
    scoped_credentials, project = google.auth.default(
        ["https://www.googleapis.com/auth/cloud-platform"]
    )
    # Creates a requests Session object with the credentials.
    dicomweb_sess = requests.AuthorizedSession(scoped_credentials)
    n=0
    for row in args.series:
        delete_series(args, dicomweb_sess, row['study_instance_uid'], row['series_instance_uid'])
        print(f"{n}: {row['study_instance_uid']}/{row['study_instance_uid']}  deleted")
        n += 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=8, help='Version to work on')
    parser.add_argument('--client', default=storage.Client())
    args = parser.parse_args()
    parser.add_argument('--dst_project', default='canceridc-data')
    parser.add_argument('--dataset_region', default='us')
    parser.add_argument('--gch_dataset_name', default='idc')
    parser.add_argument('--dicomstore', default=f'v8')
    # parser.add_argument('--dicomstore', default=f'v{args.version}')
    parser.add_argument('--series', default=[
        {'study_instance_uid':'2.25.191236165605958868867890945341011875563',
         'series_instance_uid':'1.3.6.1.4.1.5962.99.1.3426307341.632503471.1639808879885.2.0'}],
         help = 'List of (study_instance_uid, series_instance_uid) pairs')
    args = parser.parse_args()
    args.id = 0 # Default process ID

    delete_seriess(args)
