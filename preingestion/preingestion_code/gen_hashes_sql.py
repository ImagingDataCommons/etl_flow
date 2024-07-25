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
# This script updates all hashes in the idc hierarchy
import sys
import argparse
from idc.models import IDC_Collection, IDC_Patient, IDC_Study, IDC_Series, IDC_Instance
from ingestion.utilities.utils import get_merkle_hash

from utilities.logging_config import progresslogger
from python_settings import settings
from sqlalchemy import func, update, literal_column
from utilities.sqlalchemy_helpers import sa_session
from sqlalchemy.dialects.postgresql import aggregate_order_by
from sqlalchemy.sql import text

from google.cloud import storage


def gen_series_hashes(sess):
    update_hashes = text(
        "UPDATE idc_series "
        "SET hash = md5(hashes.hashes) "
        "FROM "
        "(SELECT series_instance_uid, string_agg(hash, '' ORDER BY hash) hashes "
        "FROM idc_instance "
        "GROUP BY series_instance_uid ) AS hashes "
        "WHERE idc_series.series_instance_uid = hashes.series_instance_uid "
        "RETURNING idc_series.series_instance_uid, hash "
    )
    result = sess.execute(update_hashes).fetchall()
    pass
def gen_study_hashes(sess):
    update_hashes = text(
        "UPDATE idc_study "
        "SET hash = md5(hashes.hashes) "
        "FROM "
        "(SELECT study_instance_uid, string_agg(hash, '' ORDER BY hash) hashes "
        "FROM idc_series "
        "GROUP BY study_instance_uid) AS hashes "
        "WHERE idc_study.study_instance_uid = hashes.study_instance_uid "
        "RETURNING idc_study.study_instance_uid, hash "
    )
    result = sess.execute(update_hashes).fetchall()
    pass


def gen_patient_hashes(sess):
    update_hashes = text(
        "UPDATE idc_patient "
        "SET hash = md5(hashes.hashes) "
        "FROM "
        "(SELECT submitter_case_id, string_agg(hash, '' ORDER BY hash) hashes "
        "FROM idc_study "
        "GROUP BY submitter_case_id) AS hashes "
        "WHERE idc_patient.submitter_case_id = hashes.submitter_case_id "
        "RETURNING idc_patient.submitter_case_id, hash "
    )
    result = sess.execute(update_hashes).fetchall()
    pass


def gen_collection_hashes(sess):
    update_hashes = text(
        "UPDATE idc_collection "
        "SET hash = md5(hashes.hashes) "
        "FROM "
        "(SELECT collection_id, string_agg(hash, '' ORDER BY hash) hashes "
        "FROM idc_patient "
        "GROUP BY collection_id) AS hashes "
        "WHERE idc_collection.collection_id = hashes.collection_id "
        "RETURNING idc_collection.collection_id, hash "
    )
    result = sess.execute(update_hashes).fetchall()
    pass

def gen_hashes():
    with sa_session(echo=True) as sess:
        gen_series_hashes(sess)
        gen_study_hashes(sess)
        gen_patient_hashes(sess)
        gen_collection_hashes(sess)
        sess.commit()

if __name__ == '__main__':
    gen_hashes()
