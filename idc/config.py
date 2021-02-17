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

from os import environ
import logging
from python_settings import settings
import settings as etl_settings

settings.configure(etl_settings)
assert settings.configured

logger = logging.getLogger(settings.LOGGER_NAME)

from sqlalchemy.engine import create_engine
from sqlalchemy import MetaData, Table


# Google BigQuery config
# gcp_credentials = environ.get('GCP_CREDENTIALS')
gcp_project = environ.get('GCP_PROJECT')
bigquery_dataset = environ.get('GCP_BIGQUERY_DATASET')
bigquery_table = environ.get('GCP_BIGQUERY_TABLE')
bigquery_uri = f'bigquery://{gcp_project}/{bigquery_dataset}'

# SQL database config
sql_user = environ.get('DATABASE_USERNAME')
sql_pass = environ.get('DATABASE_PASSWORD')
sql_host = environ.get('DATABASE_HOST')
sql_port = environ.get('DATABASE_PORT')
sql_name = environ.get('DATABASE_NAME')
sql_uri = f'postgresql+psycopg2://{sql_user}:{sql_pass}@{sql_host}:{sql_port}/{sql_name}'

# Locally stored queries
local_sql_folder = 'sql'
