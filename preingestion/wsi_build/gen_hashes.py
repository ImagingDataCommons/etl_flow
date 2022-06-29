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
import io
import os
import sys
import argparse
import csv
from idc.models import Base, WSI_Collection, WSI_Patient, WSI_Study, WSI_Series, WSI_Instance
from ingestion.utilities.utils import get_merkle_hash, list_skips

from logging import INFO, DEBUG
from utilities.logging_config import successlogger, errlogger, progresslogger
from base64 import b64decode
from python_settings import settings

from sqlalchemy.orm import Session
from sqlalchemy import create_engine, update
from google.cloud import storage

def gen_hashes(args, sess):
    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{settings.CLOUD_DATABASE}'
    # sql_engine = create_engine(sql_uri, echo=True)
    sql_engine = create_engine(sql_uri)

    with Session(sql_engine) as sess:
        collections = sess.query(WSI_Collection).all()
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
            hashes = [patient.hash for patient in collection.patients]
            collection.hash = get_merkle_hash(hashes)
            progresslogger.info('Collection hash %s', collection.collection_id)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--version', default=settings.CURRENT_VERSION)

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    args.client=storage.Client()

    gen_hashes(args)

