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

from utilities.logging_config import successlogger, progresslogger
from utilities.sqlalchemy_helpers import sa_session
from idc.models import Base, Collection, CR_Collections, Defaced_Collections, Excluded_Collections, Open_Collections, Redacted_Collections
import settings
from google.cloud import storage
from gcs.empty_bucket_mp.empty_bucket_mp import pre_delete

from sqlalchemy import create_engine
from sqlalchemy_utils import register_composites
from sqlalchemy.orm import Session


def delete_buckets(args):
    client = storage.Client()
    with sa_session() as sess:
        revised_collection_ids = sorted([row.collection_id for row in sess.query(Collection).filter(Collection.rev_idc_version == args.version).all()])
        for collection_id in revised_collection_ids:
            prestaging_collection_id = collection_id.lower().replace('-','_').replace(' ','_')
            for prefix in args.prestaging_bucket_prefix:
                prestaging_bucket = f"{prefix}{prestaging_collection_id}"
                if client.bucket(prestaging_bucket).exists():
                    args.bucket = prestaging_bucket
                    progresslogger.info(f'Deleting bucket {prestaging_bucket}')
                    # Delete the contents of the bucket
                    pre_delete(args)
                    # Delete the bucket itself
                    client.bucket(prestaging_bucket).delete()



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--prestaging_bucket_prefix', default=[f'idc_v{settings.CURRENT_VERSION}_tcia_', f'idc_v{settings.CURRENT_VERSION}_path_'], help='Prefix of premerge buckets')
    parser.add_argument('--processes', default=8, help="Number of concurrent processes")
    parser.add_argument('--batch', default=100, help='Size of batch assigned to each process')
    args = parser.parse_args()

    delete_buckets(args)