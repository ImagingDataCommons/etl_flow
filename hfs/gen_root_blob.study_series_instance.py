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
import argparse
import logging
from logging import INFO
from utilities.logging_config import successlogger, progresslogger, errlogger
from idc.models import Base, Version, Collection, Patient, Study, Series, Instance
from google.cloud import storage
from sqlalchemy.orm import Session
from python_settings import settings
from sqlalchemy import create_engine, update

import json
from ingestion.utilities.utils import get_merkle_hash
from hfs.gen_version_blobs import gen_version_object


def gen_root_obj(args):
    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{settings.CLOUD_DATABASE}'
    # sql_engine = create_engine(sql_uri, echo=True)
    sql_engine = create_engine(sql_uri)
    # Create the tables if they do not already exist
    Base.metadata.create_all(sql_engine)

    # todos = open(args.todos).read().splitlines()

    with Session(sql_engine) as sess:

        # bq_client = bigquery.Client()
        # destination = get_versions_in_root(args)
        # versions = [version for page in bq_client.list_rows(destination, page_size=args.batch).pages for version in page ]
        if not args.dst_bucket.blob("idc.idc").exists():
            print(f"Root started")
            versions = sess.query(Version).order_by(Version.version)
            for version in versions:
                # gen_version_object(args, version.idc_version, version.previous_idc_version, version.md5_hash)
                gen_version_object(args, sess, version)
            root = {
                "encoding": "1.0",
                "object_type": "root",
                "md5_hash": get_merkle_hash([version.hashes.all_sources for version in versions]),
                "self_uri": f"gs://{args.dst_bucket.name}/idc.idc",
                "children": {
                    "count": versions.count(),
                    "object_ids": [f"idc_v{version.version}" for version in versions],
                    "gs":{
                        "region": "us-central1",
                        "bucket": f"{args.dst_bucket.name}",
                        "gs_object_ids": [
                            f"idc_v{version.version}.idc" for version in versions
                        ]
                    }
                 },
                # "parents": {
                #     "count": 0,
                #     "object_ids": []
                # }
            }

            blob=args.dst_bucket.blob("idc.idc").upload_from_string(json.dumps(root))
            print(f"Root completed")
        else:
            print(f"Root skipped")
        return

if __name__ == '__main__':
    client = storage.Client()
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=9, help='Version to work on')
    parser.add_argument('--collections', default=['APOLLO-5-LSCC', 'CPTAC-SAR', 'TCGA-ESCA', 'TCGA-READ'])
    parser.add_argument('--hfs_level', default='study',help='Name blobs as study/series/instance if study, series/instance if series')
    parser.add_argument('--dst_bucket_name', default='whc_ssi', help='Bucket into which to copy blobs')
    args = parser.parse_args()
    args.id = 0  # Default process ID
    print(f'args: {json.dumps(args.__dict__, indent=2)}')
    args.dst_bucket = client.bucket(args.dst_bucket_name)

    gen_root_obj(args)
