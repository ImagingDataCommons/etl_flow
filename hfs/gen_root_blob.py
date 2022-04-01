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
from idc.models import Base, Version, Collection, Patient, Study, Series, Instance
from google.cloud import storage
from sqlalchemy.orm import Session
from python_settings import settings
from sqlalchemy import create_engine, update

import json
from ingestion.utils import get_merkle_hash
from hfs.gen_version_blobs import gen_version_object


# Copy the blobs that are new to a version from dev pre-staging buckets
# to dev staging buckets.

def get_versions_in_root(args):
    client = bigquery.Client()
    query = f"""
    SELECT
      distinct
      idc_version,
      previous_idc_version,
      v_hashes.all_sources as md5_hash
    FROM
      `idc-dev-etl.whc_dev.hfs_all_joined`
    ORDER BY idc_version
    """
    # urls = list(client.query(query))
    query_job = client.query(query)  # Make an API request.
    query_job.result()  # Wait for the query to complete.
    destination = query_job.destination
    destination = client.get_table(destination)
    return destination


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
        versions = sess.query(Version).order_by(Version.version)
        root = {
            "encoding": "v1",
            "object_type": "root",
            "md5_hash": get_merkle_hash([version.hashes.all_sources for version in versions]),
            "self_uri": f"gs://{args.dst_bucket.name}/idc.idc",
            "versions": {
                "gs":{
                    "region": "us-central1",
                    "urls":
                        {
                            "bucket": f"{args.dst_bucket.name}",
                            "blobs":
                                [
                                    {"version": f"idc_v{version.version}",
                                     "blob_name": f"idc_{version.version}.idc"} for version in versions
                                ]
                       }
                    }
                 }
            }

        blob=args.dst_bucket.blob("idc.idc").upload_from_string(json.dumps(root))
        for version in versions:
            # gen_version_object(args, version.idc_version, version.previous_idc_version, version.md5_hash)
            gen_version_object(args, version)
        print(f"Root")
        return

if __name__ == '__main__':
    client = storage.Client()
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=8, help='Version to work on')
    parser.add_argument('--log_dir', default=f'/mnt/disks/idc-etl/logs/hfs_gen_version_blobs')
    parser.add_argument('--collections', default="('APOLLO-5-LSCC', 'CPTAC-SAR', 'TCGA-ESCA', 'TCGA-READ')")
    parser.add_argument('--src_bucket', default='idc-dev-open', help='Bucket from which to copy blobs')
    parser.add_argument('--dst_bucket', default=client.bucket('whc_dev'), help='Bucket into which to copy blobs')
    parser.add_argument('--batch', default=1)
    parser.add_argument('--processes', default=32)
    args = parser.parse_args()
    args.id = 0  # Default process ID

    if not os.path.exists('{}'.format(args.log_dir)):
        os.mkdir('{}'.format(args.log_dir))

    successlogger = logging.getLogger('root.success')
    successlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')

    # Change logging file. File name includes bucket ID.
    for hdlr in successlogger.handlers[:]:
        successlogger.removeHandler(hdlr)
    success_fh = logging.FileHandler('{}/success.log'.format(args.log_dir))
    successlogger.addHandler(success_fh)
    successformatter = logging.Formatter('%(message)s')
    success_fh.setFormatter(successformatter)

    for hdlr in errlogger.handlers[:]:
        errlogger.removeHandler(hdlr)
    err_fh = logging.FileHandler('{}/error.log'.format(args.log_dir))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    gen_root_obj(args)
