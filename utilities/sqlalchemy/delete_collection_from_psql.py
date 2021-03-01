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
from idc.models import Version, Collection, Patient, Study, Series, Instance, sql_engine
from sqlalchemy import select
from sqlalchemy.orm import Session

import argparse
import sys

def main(args):
    with Session(sql_engine) as sess:
        version = sess.query(Version).filter_by(idc_version_number=2).one()

        # collections = sess.query(Collection).filter_by(done=False)
        collections = version.collections
        for collection in collections:
            # print(f'{collection.tcia_api_collection_id}')
            if collection.tcia_api_collection_id == args.collection:
                length = len(collection.patients)
                print(f'Deleting collection {collection.tcia_api_collection_id}; {length} patients')
                index = 1
                for patient in collection.patients:
                    print(f'Deleting patient {patient.submitter_case_id}; {index} of {length}')
                    sess.delete(patient)
                    sess.commit()
                    index += 1
                sess.delete(collection)
                sess.commit()
                break

if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--version', default=2)
    parser.add_argument('--collection', default='4D-Lung', help='Delete a collection from PSQL DB')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)

    main(args)
