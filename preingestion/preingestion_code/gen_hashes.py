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
# Update hierarchical hashes in the WSI tables
import sys
import argparse
from idc.models import IDC_Collection
from ingestion.utilities.utils import get_merkle_hash

from utilities.logging_config import progresslogger
from python_settings import settings

from utilities.sqlalchemy_helpers import sa_session
from google.cloud import storage


def gen_hashes(collection_ids=[]):
    with sa_session(echo=False) as sess:
        if collection_ids:
            collections = sess.query(IDC_Collection).filter(IDC_Collection.collection_id.in_(collection_ids))
        else:
            collections = sess.query(IDC_Collection).all()
        n = 0
        for collection in collections:
            for patient in collection.patients:
                for study in patient.studies:
                    for series in study.seriess:
                        hashes = [instance.hash for instance in series.instances]
                        series.hash = get_merkle_hash(hashes)
                        progresslogger.info('\t\t\tseries hash %s', series.series_instance_uid)
                    hashes = [series.hash for series in study.seriess]
                    study.hash = get_merkle_hash(hashes)
                    progresslogger.info('\t\tstudy hash %s', study.study_instance_uid)
                hashes = [study.hash for study in patient.studies]
                patient.hash = get_merkle_hash(hashes)
                progresslogger.info('\tpatient hash %s', patient.submitter_case_id)
                n += 1
                if not n % 100:
                    sess.commit()

            hashes = [patient.hash for patient in collection.patients]
            collection.hash = get_merkle_hash(hashes)
            progresslogger.info('Collection hash %s', collection.collection_id)
        sess.commit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--version', default=settings.CURRENT_VERSION)
    parser.add_argument('--collection', default='NLST',
                        help='If not null, gen hash of this collection, else all collections')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    args.client = storage.Client()

    gen_hashes(args.collection)
