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

# Remove a collection from
# the idc_xxx hierarchy

import sys
import argparse
from python_settings import settings
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, update
from google.cloud import storage

from idc.models import IDC_Collection
from remove_elements import remove_collection

def prebuild(args):
    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{settings.CLOUD_DATABASE}'
    # sql_engine = create_engine(sql_uri, echo=True)
    sql_engine = create_engine(sql_uri)

    with Session(sql_engine) as sess:
        client = storage.Client()
        for collection_id in args.collection_ids:
            # print(f'{reader.line_num-1}/{rows}: {row}')
            collection = sess.query(IDC_Collection).filter(IDC_Collection.collection_id == collection_id).first()
            remove_collection(client, args, sess, collection)
        sess.commit()
    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--source_url', default='https://doi.org/10.5281/zenodo.11099099', \
                        help='Only delete instances having this source_url')
    parser.add_argument('--collection_ids', type=str, default=['GTEx'], nargs='*', \
      help='A list of collections to remove.')
    # parser.add_argument('--log_dir', default=f'{settings.LOGGING_BASE}/{settings.BASE_NAME}')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    args.client=storage.Client()

    prebuild(args)
