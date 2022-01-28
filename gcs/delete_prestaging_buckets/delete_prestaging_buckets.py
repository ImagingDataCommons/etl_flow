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

# Delete pre-staging buckets populated by ingestion.

import os
import argparse
import logging
from logging import INFO
from google.cloud import storage


from idc.models import Base, Collection, CR_Collections, Defaced_Collections, Excluded_Collections, Open_Collections, Redacted_Collections
import settings as etl_settings
from python_settings import settings
if not settings.configured:
    settings.configure(etl_settings)
from google.cloud import storage
from gcs.empty_bucket_mp.empty_bucket_mp import pre_delete

from sqlalchemy import create_engine
from sqlalchemy_utils import register_composites
from sqlalchemy.orm import Session


def delete_buckets(args):
    client = storage.Client()
    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{args.db}'
    # sql_engine = create_engine(sql_uri, echo=True) # Use this to see the SQL being sent to PSQL
    sql_engine = create_engine(sql_uri)
    args.sql_uri = sql_uri # The subprocesses need this uri to create their own SQL engine

    # Create the tables if they do not already exist
    Base.metadata.create_all(sql_engine)

    # Enable the underlying psycopg2 to deal with composites
    conn = sql_engine.connect()
    register_composites(conn)

    with Session(sql_engine) as sess:
        revised_collection_ids = sorted([row.collection_id for row in sess.query(Collection).filter(Collection.rev_idc_version == args.version).all()])
        for collection_id in revised_collection_ids:
            prestaging_collection_id = collection_id.lower().replace('-','_').replace(' ','_')
            prestaging_bucket = f"{args.prestaging_bucket_prefix}{prestaging_collection_id}"
            args.bucket = prestaging_bucket
            if client.bucket(prestaging_bucket).exists():
                pre_delete(args)
                client.bucket(prestaging_bucket).delete()



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=7, help='Version to work on')
    parser.add_argument('--client', default=storage.Client())
    args = parser.parse_args()
    parser.add_argument('--db', default=f'idc_v{args.version}', help='Database on which to operate')
    parser.add_argument('--project', default='idc-dev-etl')
    parser.add_argument('--prestaging_bucket_prefix', default=f'idc_v{args.version}_', help='Copy instances here before forwarding to --staging_bucket')
    parser.add_argument('--processes', default=8, help="Number of concurrent processes")
    parser.add_argument('--batch', default=100, help='Size of batch assigned to each process')
    parser.add_argument('--log_dir', default=f'/mnt/disks/idc-etl/logs/empty_bucket_mp')
    args = parser.parse_args()
    args.id = 0 # Default process ID

    proglogger = logging.getLogger('root.prog')
    prog_fh = logging.FileHandler(f'{os.environ["PWD"]}/logs/bucket.log')
    progformatter = logging.Formatter('%(levelname)s:prog:%(message)s')
    proglogger.addHandler(prog_fh)
    prog_fh.setFormatter(progformatter)
    proglogger.setLevel(INFO)

    successlogger = logging.getLogger('root.success')
    successlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')

    delete_buckets(args)