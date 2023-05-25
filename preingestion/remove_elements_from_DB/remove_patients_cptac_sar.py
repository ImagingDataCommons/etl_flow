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

# Remove a patients of a collection from
# the idc_xxx hierarchy

import sys
import argparse
from python_settings import settings
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, update
from google.cloud import storage

from idc.models import IDC_Collection, IDC_Patient
from remove_elements import remove_patient
from preingestion.populate_idc_metadata_tables.gen_hashes import gen_hashes

def prebuild(args):
    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{settings.CLOUD_DATABASE}'
    # sql_engine = create_engine(sql_uri, echo=True)
    sql_engine = create_engine(sql_uri)

    with Session(sql_engine) as sess:
        client = storage.Client()
        collection = sess.query(IDC_Collection).filter(IDC_Collection.collection_id == args.collection_id).first()
        for submitter_case_id in args.submitter_case_ids:
            if patient := next((patient for patient in collection.patients if patient.submitter_case_id == submitter_case_id),0):
                remove_patient(client, args, sess, collection, patient)
        sess.commit()
        gen_hashes(args.collection_id)
    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--collection_id', type=str, default='CPTAC-SAR', nargs='*', \
          help='A list of collections to remove.')
    parser.add_argument('--submitter_case_ids', type=str, default=['C3L-03551'], nargs='*', \
        help='A list of submitter_case_ids to remove.')
    # parser.add_argument('--log_dir', default=f'{settings.LOGGING_BASE}/{settings.BASE_NAME}')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    args.client=storage.Client()

    prebuild(args)
