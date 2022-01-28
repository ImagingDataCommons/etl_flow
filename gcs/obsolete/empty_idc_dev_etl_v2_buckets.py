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

# One time script to delete the idc-tcia-v2-xxx buckets in idc-dev-etl.
# Could be adapted tp remove idc-tcia- buckets in canceridc-data.
import argparse
import os
from google.cloud import storage, bigquery
import logging
from logging import INFO
from gcs.empty_bucket_mp.empty_bucket_mp import pre_delete


def get_collections_in_version(args):
    client = bigquery.Client()
    query = f"""
        SELECT c.tcia_api_collection_id 
        FROM `idc-dev-etl.idc_v2.collection` AS c
        ORDER BY c.tcia_api_collection_id
        """
    result = client.query(query).result()
    collection_ids = [collection['tcia_api_collection_id'] for collection in result]
    return collection_ids


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', default='idc-dev-v5-dicomstore-staging')
    parser.add_argument('--processes', default=128, help="Number of concurrent processes")
    parser.add_argument('--batch', default=100, help='Size of batch assigned to each process')
    parser.add_argument('--project', default='idc-dev-etl')
    parser.add_argument('--log_dir', default=f'/mnt/disks/idc-etl/logs/empty_idc_dev_etl_v2_buckets')
    parser.add_argument('--dones', default=f'{os.environ["PWD"]}/logs/dones.log')

    args = parser.parse_args()

    if not os.path.exists('{}'.format(args.log_dir)):
        os.mkdir('{}'.format(args.log_dir))

    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler(f'{os.environ["PWD"]}/logs/bucket.log')
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    successlogger = logging.getLogger('success')
    successlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')

    dones = open(args.dones).read().splitlines()

    client = storage.Client(project=args.project)

    collections = [collection.lower().replace(' ','-').replace('_','-') for collection in get_collections_in_version(args)]

    # for collection in collections:
    #     found=False
    #     if client.bucket(f'idc-tcia-1-{collection}', user_project=args.project).exists():
    #         print(f'{collection:32}: idc-tcia-1-{collection}')
    #         found=True
    #     if client.bucket(f'idc-tcia-2-{collection}', user_project=args.project).exists():
    #         print(f'{collection:32}: idc-tcia-2-{collection}')
    #         found=True
    #     if not found:
    #         print(f'{collection}: ***No bucket***')
    #


    for collection in collections:
        args.bucket = f"idc-tcia-2-{collection}"
        if not args.bucket in dones:
            bucket = client.bucket(args.bucket, user_project=args.project)
            tried = 0
            tries = 2
            while tried < tries:
                try:
                    if bucket.exists():
                        pre_delete(args)
                        bucket.delete()
                        rootlogger.info(f'Deleted bucket %s',args.bucket)
                        break
                    else:
                        break
                except Exception  as exc:
                    print(f'p0: Delete bucket failed: {exc}')
                    tried += 1
            if tried == tries:
                errlogger.error(f'Failed to delete bucket %s', args.bucket)
            with open(args.dones, 'a') as f:
                f.write(f'{args.bucket}\n')


