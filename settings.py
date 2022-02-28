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

import os
import json
from os.path import join, dirname, exists
from dotenv import load_dotenv

CURRENT_VERSION=9
PREVIOUS_VERSION=8

SECURE_LOCAL_PATH = os.environ.get('SECURE_LOCAL_PATH', '')

if not exists(join(dirname(__file__), SECURE_LOCAL_PATH, '.env.idc-dev-etl')):
    print("[ERROR] Couldn't open .env.idc-dev-etl file expected at {}!".format(
        join(dirname(__file__), SECURE_LOCAL_PATH, '.env.idc-dev-etl'))
    )
    print("[ERROR] Exiting settings.py load - check your Pycharm settings and secure_path.env file.")
    exit(1)

load_dotenv(dotenv_path=join(dirname(__file__), SECURE_LOCAL_PATH, '.env.idc-dev-etl'))

DEBUG = (os.environ.get('DEBUG', 'False') == 'True')

print("[STATUS] DEBUG mode is "+str(DEBUG))

# These are no longer used since we moved to Cloud SQL.
# Kept here in case we need to run PSQL locally
LOCAL_USERNAME = os.environ.get('LOCAL_USERNAME', '')
LOCAL_PASSWORD = os.environ.get('LOCAL_PASSWORD', '')
LOCAL_HOST='localhost'
LOCAL_PORT='5432'
#LOCAL_DATABASE_NAME=idc_nlst

# Parameters fpor accessing the Cloud SQL DB server
CLOUD_USERNAME = os.environ.get('CLOUD_USERNAME', '')
CLOUD_PASSWORD = os.environ.get('CLOUD_PASSWORD', '')
CLOUD_HOST='0.0.0.0'
CLOUD_PORT='5433'
CLOUD_INSTANCE='idc-dev-etl:us-central1:idc-dev-etl-psql-whc'
CLOUD_DATABASE = f'idc_v{CURRENT_VERSION}'

# Various projects that we operate in
DEV_PROJECT='idc-dev-etl'
PDP_PROJECT='idc-pdp-staging'
PUB_PROJECT='canceridc-staging'

# GCH DICOM stores are now only created in the PUB_PROJECT
GCH_PROJECT=PUB_PROJECT
GCH_REGION='us'
GCH_DATASET='idc'
GCH_DICOMSTORE=f'v{CURRENT_VERSION}'

# IDs of the various dev buckets
GCS_DEV_OPEN='idc-dev-open'
GCS_DEV_CR='idc-dev-cr'
GCS_DEV_MASKABLE='idc-dev-defaced'
GCS_DEV_REDACTED='idc-dev-redacted'
GCS_DEV_EXCLUDED='idc-dev-excluded'

# IDs of the public buckets.
GCS_PUB_OPEN='idc-open-pdp-staging'
GCS_PUB_CR='idc-open-cr'
GCS_PUB_MASKABLE='idc-open-idc1'

# IDs of the dev and public BQ datasets
BQ_DEV_INT_DATASET=f'idc_v{CURRENT_VERSION}_dev'
BQ_DEV_EXT_DATASET=f'idc_v{CURRENT_VERSION}_dev'
BQ_PUB_DATASET=f'idc_v{CURRENT_VERSION}'

# IDs and passwords to accessing some TCIA API endpoints
TCIA_ID = os.environ.get('TCIA_ID')
TCIA_PASSWORD = os.environ.get('TCIA_PASSWORD')
TCIA_CLIENT_ID = os.environ.get('TCIA_CLIENT_ID')
TCIA_CLIENT_SECRET= os.environ.get('TCIA_CLIENT_SECRET')




