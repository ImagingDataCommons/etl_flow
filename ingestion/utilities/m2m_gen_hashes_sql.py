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
# This script updates all hashes in the M2M hierarchy
# and for a specified collection
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
        "UPDATE series "
        "SET hash = md5(hashes.hashes) "
        "FROM "
        "(SELECT se_uuid, string_agg(hash, '' ORDER BY hash) hashes "
        "FROM series_instance se_i"
        "JOIN instance "
        "ON se_i.sop_instance_uud = instance.sop_instance_uid"
        "GROUP BY se_uuid ) AS hashes "
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


def gen_version_hashes(sess):
    # update_hashes = """
    #         WITH c_hashes AS (
    #         SELECT v.version idc_version,
    #         string_agg((c.hashes).tcia, '' ORDER BY (c.hashes).tcia) tcia_hashes,
    #         string_agg((c.hashes).idc, '' ORDER BY (c.hashes).idc) idc_hashes,
    #         string_agg((c.hashes).all_sources, '' ORDER BY (c.hashes).all_sources) all_sources_hashes
    #         FROM version v
    #         JOIN version_collection v_c
    #         ON v.version = v_c.version
    #         JOIN collection c
    #         ON v_c.collection_uuid = c.uuid
    #         GROUP BY v.version),
    #         v_hashes AS (
    #         SELECT idc_version,
    #         CASE WHEN c_hashes.tcia_hashes != '' THEN md5(c_hashes.tcia_hashes) ELSE '' END AS tcia_hash ,
    #         CASE WHEN c_hashes.idc_hashes != '' THEN md5(c_hashes.idc_hashes) ELSE '' END AS idc_hash ,
    #         CASE WHEN c_hashes.all_sources_hashes != '' THEN md5(c_hashes.all_sources_hashes) ELSE '' END AS all_sources_hash
    #         FROM c_hashes
    #         GROUP BY idc_version, c_hashes.tcia_hashes, c_hashes.idc_hashes, c_hashes.all_sources_hashes)
    #         UPDATE version_dev
    #         SET hashes.tcia = tcia_hash,  hashes.idc = idc_hash,  hashes.all_sources = all_sources_hash
    #         FROM v_hashes
    #         WHERE version = v_hashes.idc_version
    # """


#     update_hashes = text(
#         "WITH c_hashes AS ("
#         "SELECT idc_version, "
#         "string_agg((c_hashes).tcia, '' ORDER BY (c_hashes).tcia) tcia_hashes,"
#         "string_agg((c_hashes).idc, '' ORDER BY (c_hashes).idc) idc_hashes,"
#         "string_agg((c_hashes).all_sources, '' ORDER BY (c_hashes).all_sources) all_sources_hashes "
#         "FROM all_joined "
#         "GROUP BY idc_version),"
#         "v_hashes AS ("
#         "SELECT idc_version, "
#         "CASE WHEN c_hashes.tcia_hashes != '' THEN md5(c_hashes.tcia_hashes) ELSE '' END AS tcia_hash ,"
#         "CASE WHEN c_hashes.idc_hashes != '' THEN md5(c_hashes.idc_hashes) ELSE '' END AS idc_hash ,"
#         "CASE WHEN c_hashes.all_sources_hashes != '' THEN md5(c_hashes.all_sources_hashes) ELSE '' END AS all_sources_hash "
#         "FROM c_hashes "
#         "GROUP BY idc_version, c_hashes.tcia_hashes, c_hashes.idc_hashes, c_hashes.all_sources_hashes)"
#         "UPDATE version_dev "
#         "SET hashes.tcia = tcia_hash,  hashes.idc = idc_hash,  hashes.all_sources = all_sources_hash  "
#         "FROM v_hashes "
#         "WHERE version = v_hashes.idc_version"
#     )
#

    update_hashes = """
        WITH c_hashes AS (
        SELECT idc_version,
        string_agg((c_hashes).tcia, '' ORDER BY (c_hashes).tcia) tcia_hashes,
        string_agg((c_hashes).idc, '' ORDER BY (c_hashes).idc) idc_hashes,
        string_agg((c_hashes).all_sources, '' ORDER BY (c_hashes).all_sources) all_sources_hashes
        FROM all_joined_hashes_vc
        GROUP BY idc_version),
        v_hashes AS (
        SELECT idc_version,
        -- CASE WHEN c_hashes.tcia_hashes != '' THEN md5(c_hashes.tcia_hashes) ELSE '' END AS tcia_hash ,
        -- CASE WHEN c_hashes.idc_hashes != '' THEN md5(c_hashes.idc_hashes) ELSE '' END AS idc_hash ,
        -- CASE WHEN c_hashes.all_sources_hashes != '' THEN md5(c_hashes.all_sources_hashes) ELSE '' END AS all_sources_hash
        md5(c_hashes.tcia_hashes) tcia_hash ,
        md5(c_hashes.idc_hashes) idc_hash ,
        md5(c_hashes.all_sources_hashes) all_sources_hash
        FROM c_hashes
        GROUP BY idc_version, c_hashes.tcia_hashes, c_hashes.idc_hashes, c_hashes.all_sources_hashes)
        UPDATE version_dev
        SET hashes.tcia = tcia_hash,  hashes.idc = idc_hash,  hashes.all_sources = all_sources_hash
        FROM v_hashes
        WHERE version = v_hashes.idc_version
"""
    result = sess.execute(update_hashes)
    pass

def gen_hashes():
    with sa_session(echo=True) as sess:
        # gen_series_hashes(sess)
        # gen_study_hashes(sess)
        # gen_patient_hashes(sess)
        # gen_collection_hashes(sess)
        gen_version_hashes(sess)
        sess.commit()
    progresslogger.info("Updated hashes")

if __name__ == '__main__':
    gen_hashes()
