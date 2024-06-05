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
import sys
from os.path import join, dirname, exists
from dotenv import load_dotenv

CURRENT_VERSION=19
PREVIOUS_VERSION=18

# # IF MITIGATION_VERSION is defined, logging will be to m<MITIGATION_VERSION) rather than v<CURRENT_VERSION>
# MITIGATION_VERSION="m1"

SECURE_LOCAL_PATH = os.environ.get('SECURE_LOCAL_PATH', '')

if not exists(join(dirname(__file__), SECURE_LOCAL_PATH, '.env')):
    print("[ERROR] Couldn't open .env.idc-dev-etl file expected at {}!".format(
        join(dirname(__file__), SECURE_LOCAL_PATH, '.env'))
    )
    print("[ERROR] Exiting settings.py load - check your Pycharm settings and secure_path.env file.")
    exit(1)

load_dotenv(dotenv_path=join(dirname(__file__), SECURE_LOCAL_PATH, '.env'))

DEBUG = (os.environ.get('DEBUG', 'False') == 'True')

#print("[STATUS] DEBUG mode is "+str(DEBUG))

# These are no longer used since we moved to Cloud SQL.
# Kept here in case we need to run PSQL locally
LOCAL_USERNAME = os.environ.get('LOCAL_USERNAME', '')
LOCAL_PASSWORD = os.environ.get('LOCAL_PASSWORD', '')
LOCAL_HOST='localhost'
LOCAL_PORT='5432'
#LOCAL_DATABASE_NAME=idc_nlst

# Parameters for accessing the Cloud SQL DB server
CLOUD_USERNAME = os.environ.get('CLOUD_USERNAME', '')
CLOUD_PASSWORD = os.environ.get('CLOUD_PASSWORD', '')
CLOUD_HOST = os.environ.get('CLOUD_HOST', '')
CLOUD_PORT= os.environ.get('CLOUD_PORT', '')
CLOUD_INSTANCE = os.environ.get('CLOUD_INSTANCE', '')
CLOUD_DATABASE = f'idc_v{CURRENT_VERSION}'

# Various projects that we operate in
DEV_PROJECT=os.environ.get('DEV_PROJECT', '')
PDP_PROJECT=os.environ.get('PDP_PROJECT', '')
PUB_PROJECT=os.environ.get('PUB_PROJECT', '')

DEV_MITIGATION_PROJECT=os.environ.get('DEV_MITIGATION_PROJECT', '')
STAGING_MITIGATION_PROJECT=os.environ.get('STAGING_MITIGATION_PROJECT', '')

SUBMISSION_PROJECT=os.environ.get('SUBMISSION_PROJECT', '')

# GCH DICOM stores are now only created in the PUB_PROJECT
GCH_PROJECT=os.environ.get('GCH_PROJECT', '')
GCH_REGION=os.environ.get('GCH_REGION', '')
GCH_DATASET=os.environ.get('GCH_DATASET', '')
GCH_DICOMSTORE=f'v{CURRENT_VERSION}'

# IDs of the various dev buckets
GCS_DEV_OPEN=os.environ.get('GCS_DEV_OPEN', '')
GCS_DEV_CR=os.environ.get('GCS_DEV_CR', '')
GCS_DEV_MASKABLE=os.environ.get('GCS_DEV_MASKABLE', '')
GCS_DEV_REDACTED=os.environ.get('GCS_DEV_REDACTED', '')
GCS_DEV_EXCLUDED=os.environ.get('GCS_DEV_EXCLUDED', '')

# IDs of the public buckets.
GCS_PUB_OPEN=os.environ.get('GCS_PUB_OPEN', '')
GCS_PUB_CR=os.environ.get('GCS_PUB_CR', '')
GCS_PUB_MASKABLE=os.environ.get('GCS_PUB_MASKABLE', '')

# IDs of the dev and public BQ datasets
BQ_REGION='us'
# BQ_DEV_INT_DATASET=f'idc_v{CURRENT_VERSION}_dev' if CURRENT_VERSION>=8 else f'idc_v{CURRENT_VERSION}'
BQ_DEV_INT_DATASET=f'idc_v{CURRENT_VERSION}_dev'
BQ_DEV_EXT_DATASET=f'idc_v{CURRENT_VERSION}_pub' if CURRENT_VERSION>=8 else f'idc_v{CURRENT_VERSION}'
BQ_PUB_DATASET=f'idc_v{CURRENT_VERSION}'
BQ_PDP_DATASET=f'idc_v{CURRENT_VERSION}'

# IDs and passwords to accessing some TCIA API endpoints
# TCIA_ID = os.environ.get('TCIA_ID')
# TCIA_PASSWORD = os.environ.get('TCIA_PASSWORD')
# TCIA_CLIENT_ID = os.environ.get('TCIA_CLIENT_ID')
# TCIA_CLIENT_SECRET= os.environ.get('TCIA_CLIENT_SECRET')

if os.getenv("CI",''):
    LOGGING_BASE = f'{os.getenv("LOG_DIR")}/v{CURRENT_VERSION}'
else:
    if 'MITIGATION_VERSION' in locals():
        LOGGING_BASE = f'/mnt/disks/idc-etl/logs/{MITIGATION_VERSION}'
    else:
        LOGGING_BASE = f'/mnt/disks/idc-etl/logs/v{CURRENT_VERSION}'
BASE_NAME = sys.argv[0].rsplit('/',1)[-1].rsplit('.',1)[0]

LOG_DIR = f'{LOGGING_BASE}/{BASE_NAME}'

ETL_LOGGING_RECORDS_BUCKET = os.environ.get('ETL_LOGGING_RECORDS_BUCKET', '')

AH_PROJECT = os.environ.get('AH_PROJECT', '')           # Analytics Hub project
AH_EXCHANGE_ID = os.environ.get('AH_EXCHANGE_ID', '')    # ID of the Analytics Hub exchange
AH_EXCHANGE_LOCATION = os.environ.get('AH_EXCHANGE_LOCATION', '')



