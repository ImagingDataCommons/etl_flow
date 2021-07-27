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



SECURE_LOCAL_PATH =     os.environ.get('SECURE_LOCAL_PATH', '')

if not exists(join(dirname(__file__), SECURE_LOCAL_PATH, '.env')):
    print("[ERROR] Couldn't open .env file expected at {}!".format(
        join(dirname(__file__), SECURE_LOCAL_PATH, '.env'))
    )
    print("[ERROR] Exiting settings.py load - check your Pycharm settings and secure_path.env file.")
    exit(1)

load_dotenv(dotenv_path=join(dirname(__file__), SECURE_LOCAL_PATH, '.env'))

DEBUG =                 (os.environ.get('DEBUG', 'False') == 'True')

print("[STATUS] DEBUG mode is "+str(DEBUG))

GCP_PROJECT =           os.environ.get('GCP_PROJECT', '')
BIGQUERY_DATASET =      os.environ.get('BIGQUERY_DATASET', '')
BIGQUERY_AUXILLIARY_METADATA = "auxilliary_metadata"

# DATABASE_NAME =         os.environ.get('DATABASE_NAME', '')
LOCAL_DATABASE_USERNAME =     os.environ.get('LOCAL_DATABASE_USERNAME', '')
LOCAL_DATABASE_PASSWORD =     os.environ.get('LOCAL_DATABASE_PASSWORD', '')
LOCAL_DATABASE_HOST =         os.environ.get('LOCAL_DATABASE_HOST', '')
LOCAL_DATABASE_PORT =         os.environ.get('LOCAL_DATABASE_PORT', '')

LOGGER_NAME =           os.environ.get('ETL_LOGGER_NAME', 'main_logger')

TCIA_ID =                os.environ.get('TCIA_ID')
TCIA_PASSWORD =          os.environ.get('TCIA_PASSWORD')
TCIA_CLIENT_ID =         os.environ.get('TCIA_CLIENT_ID')
TCIA_CLIENT_SECRET=     os.environ.get('TCIA_CLIENT_SECRET')



