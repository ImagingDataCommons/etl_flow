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

import settings
from sqlalchemy import create_engine
from sqlalchemy_utils import register_composites
from sqlalchemy.orm import Session
from idc.models import Base

# Create an SQLAlchemy session
def sa_session(echo=False):
    # print(f'''
    # CLOUD_USERNAME: {settings.CLOUD_USERNAME}
    # CLOUD_PASSWORD: {settings.CLOUD_PASSWORD}
    # CLOUD_HOST: {settings.CLOUD_HOST}
    # CLOUD_PORT: {settings.CLOUD_PORT}
    # CLOUD_DATABASE: {settings.CLOUD_DATABASE}
    # CLOUD_INSTANCE: {settings.CLOUD_INSTANCE}
    # ''')
    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{settings.CLOUD_DATABASE}'
    sql_engine = create_engine(sql_uri, echo=echo)

    # Enable the underlying psycopg2 to deal with composites
    conn = sql_engine.connect()
    register_composites(conn)

    sess = Session(sql_engine)

    return sess

