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

# Copy pre-staging buckets populated by ingestion to staging buckets.
# Ingestion copies data into prestaging buckets named by version and
# collection, e.g. idc_v7_tcga_brca. The data in these buckets must be
# copied to one of the idc-dev-etl staging buckets:
# idc-dev-open, idc-dev-cr, idc-dev-defaced, idc-dev-redacted, idc-dev-excluded.

import os
import argparse
import logging
from logging import INFO

from idc.models import Base, Collection, CR_Collections, Defaced_Collections, Excluded_Collections, Open_Collections, Redacted_Collections
# import settings as etl_settings
# from python_settings import settings
# settings.configure(etl_settings)
import settings
from google.cloud import storage, bigquery
from gcs.copy_bucket_mp.copy_bucket_mp import pre_copy

from sqlalchemy import create_engine
from sqlalchemy_utils import register_composites
from sqlalchemy.orm import Session


def get_collection_groups():
    client = bigquery.Client()
    collections = {}
    # for c in sess.query(CR_Collections.tcia_api_collection_id, CR_Collections.premerge_tcia_url, CR_Collections.premerge_path_url, CR_Collections.dev_url):
    #     collections[c.tcia_api_collection_id.lower().replace('-','_').replace(' ','_')] = {"premerge_tcia_url":c.premerge_tcia_url, "premerge_path_url":c.premerge_path_url, "dev_url":c.dev_url}
    # for c in sess.query(Defaced_Collections.tcia_api_collection_id, Defaced_Collections.premerge_tcia_url, Defaced_Collections.premerge_path_url, CR_Collections.dev_url):
    #     collections[c.tcia_api_collection_id.lower().replace('-', '_').replace(' ', '_')] = {"premerge_tcia_url": c.premerge_tcia_url, "premerge_path_url": c.premerge_path_url, "dev_url": c.dev_url}
    # for c in sess.query(Open_Collections.tcia_api_collection_id, Open_Collections.premerge_tcia_url, Open_Collections.premerge_path_url, Open_Collections.dev_url):
    #     collections[c.tcia_api_collection_id.lower().replace('-', '_').replace(' ', '_')] = {"premerge_tcia_url": c.premerge_tcia_url, "premerge_path_url": c.premerge_path_url, "dev_url": c.dev_url}
    # for c in sess.query(Redacted_Collections.tcia_api_collection_id, Redacted_Collections.premerge_tcia_url, Redacted_Collections.premerge_path_url, Redacted_Collections.dev_url):
    #     collections[c.tcia_api_collection_id.lower().replace('-', '_').replace(' ', '_')] = {"premerge_tcia_url": c.premerge_tcia_url, "premerge_path_url": c.premerge_path_url, "dev_url": c.dev_url}

    query = f"""
    SELECT idc_webapp_collection_id, dev_tcia_url, dev_path_url
    FROM `idc-dev-etl.{settings.BQ_DEV_INT_DATASET}.all_included_collections`
    """

    result = client.query(query).result()
    for row in result:
        collections[row['idc_webapp_collection_id']] = {"dev_tcia_url": row["dev_tcia_url"], "dev_path_url": row["dev_path_url"]}

    return collections


def copy_prestaging_to_staging(args, prestaging_bucket, staging_bucket):
    print(f'Copying {prestaging_bucket} to {staging_bucket}')
    args.src_bucket = prestaging_bucket
    args.dst_bucket = staging_bucket
    pre_copy(args)
    return


def copy_dev_buckets(args):
    client = storage.Client()
    bucket_data= get_collection_groups()
    for collection_id in bucket_data:
        if client.bucket(f'idc_v{args.version}_tcia_{collection_id}').exists():
            copy_prestaging_to_staging(args, f'idc_v{args.version}_tcia_{collection_id}', bucket_data[collection_id]['dev_tcia_url'])
        if client.bucket(f'idc_v{args.version}_path_{collection_id}').exists():
            copy_prestaging_to_staging(args, f'idc_v{args.version}_path_{collection_id}', bucket_data[collection_id]['dev_path_url'])
    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='Version to work on')
    parser.add_argument('--processes', default=16, help="Number of concurrent processes")
    parser.add_argument('--batch', default=100, help='Size of batch assigned to each process')
    parser.add_argument('--log_dir', default=f'/mnt/disks/idc-etl/logs/v9/copy_premerge_to_staging_bucket_mp')
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

    copy_dev_buckets(args)