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

# One time use script to gen Merkle (hierarchical) hash for series, studies, patients, collections and versions.

import sys
import os
import argparse
import logging
from logging import INFO


from python_settings import settings
import settings as etl_settings

settings.configure(etl_settings)
assert settings.configured
import psycopg2
from psycopg2.extras import DictCursor


def gen_version_hashes(cur, args):
    query = f"""
        UPDATE
          version{args.suffix}
        SET
          version_hash= hashes.version_hash
        FROM (
          SELECT
            big.idc_version_number,
            md5(STRING_AGG(big.collection_hash,'' ORDER BY big.collection_hash ASC)) AS version_hash
          FROM (
            SELECT
              v.idc_version_number,
              c.collection_hash
            FROM
              collection{args.suffix} AS c
            JOIN
              version{args.suffix} AS v
            ON
              c.version_id = v.id
            ) AS big
          GROUP BY big.idc_version_number, big.idc_version_number ) AS hashes
        WHERE
          version{args.suffix}.idc_version_number=hashes.idc_version_number
      """
    cur.execute(query)


def gen_collection_hashes(cur, args):
    query = f"""
        UPDATE
          collection{args.suffix}
        SET
          collection_hash= hashes.collection_hash
        FROM (
          SELECT
            big.tcia_api_collection_id,
            md5(STRING_AGG(big.patient_hash,'' ORDER BY big.patient_hash ASC)) AS collection_hash
          FROM (
            SELECT
              v.idc_version_number,
              c.tcia_api_collection_id,
              p.patient_hash
            FROM
              patient{args.suffix} AS p
            JOIN
              collection{args.suffix} AS c
            ON
              p.collection_id = c.id
            JOIN
              version{args.suffix} AS v
            ON
              c.version_id = v.id
            ) AS big
          GROUP BY big.tcia_api_collection_id, big.idc_version_number ) AS hashes
        WHERE
          collection{args.suffix}.tcia_api_collection_id=hashes.tcia_api_collection_id
      """
    cur.execute(query)


def gen_patient_hashes(cur, args):
    query = f"""
        UPDATE
          patient{args.suffix}
        SET
          patient_hash= hashes.patient_hash
        FROM (
          SELECT
            big.submitter_case_id,
            md5(STRING_AGG(big.study_hash,'' ORDER BY big.study_hash ASC)) AS patient_hash
          FROM (
            SELECT
                v.idc_version_number,
                p.submitter_case_id,
                st.study_hash
            FROM
              study{args.suffix} AS st
            JOIN
              patient{args.suffix} AS p
            ON
              st.patient_id = p.id
            JOIN
              collection{args.suffix} AS c
            ON
              p.collection_id = c.id
            JOIN
              version{args.suffix} AS v
            ON
              c.version_id = v.id
            ) AS big
          GROUP BY big.submitter_case_id, big.idc_version_number ) AS hashes
        WHERE
          patient{args.suffix}.submitter_case_id=hashes.submitter_case_id
      """
    cur.execute(query)


def gen_study_hashes(cur, args):
    query = f"""
        UPDATE
          study{args.suffix}
        SET
          study_hash= hashes.study_hash
        FROM (
          SELECT
            big.study_instance_uid AS study_instance_uid,
            md5(STRING_AGG(big.series_hash, '' ORDER BY big.series_hash ASC)) AS study_hash
          FROM (
            SELECT
                v.idc_version_number,
                st.study_instance_uid,
                se.series_hash
            FROM
              series{args.suffix} AS se
            JOIN
              study{args.suffix} AS st
            ON
              st.id = se.study_id
            JOIN
              patient{args.suffix} AS p
            ON
              st.patient_id = p.id
            JOIN
              collection{args.suffix} AS c
            ON
              p.collection_id = c.id
            JOIN
              version{args.suffix} AS v
            ON
              c.version_id = v.id
            ) AS big
          GROUP BY big.study_instance_uid, big.idc_version_number ) AS hashes
        WHERE
          study{args.suffix}.study_instance_uid=hashes.study_instance_uid
      """
    cur.execute(query)


def gen_series_hashes(cur, args):
    query = f"""
        UPDATE
          series{args.suffix}
        SET
          series_hash = hashes.series_hash
        FROM (
          SELECT
            big.series_instance_uid,
            md5(STRING_AGG(big.instance_hash, '' ORDER BY big.instance_hash ASC)) AS series_hash
          FROM (
            SELECT
                v.idc_version_number,
                se.series_instance_uid,
                i.instance_hash
            FROM
              instance{args.suffix} AS i
            JOIN
              series{args.suffix} AS se
            ON
              se.id = i.series_id
            JOIN
              study{args.suffix} AS st
            ON
              st.id = se.study_id
            JOIN
              patient{args.suffix} AS p
            ON
              st.patient_id = p.id
            JOIN
              collection{args.suffix} AS c
            ON
              p.collection_id = c.id
            JOIN
              version{args.suffix} AS v
            ON
              c.version_id = v.id
            ) AS big
          GROUP BY big.series_instance_uid, big.idc_version_number) AS hashes
        WHERE
          series{args.suffix}.series_instance_uid=hashes.series_instance_uid
      """
    cur.execute(query)


def gen_merkle_hashes(args):
    conn = psycopg2.connect(dbname=settings.DATABASE_NAME, user=settings.DATABASE_USERNAME,
                            password=settings.DATABASE_PASSWORD, host=settings.DATABASE_HOST)
    with conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            gen_series_hashes(cur, args)
            gen_study_hashes(cur, args)
            gen_patient_hashes(cur, args)
            gen_collection_hashes(cur, args)
            gen_version_hashes(cur, args)
            pass



if __name__ == '__main__':
    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/gen_merkle_log.log'.format(os.environ['PWD']))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/gen_merkle_err.log'.format(os.environ['PWD']))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    parser = argparse.ArgumentParser()
    parser.add_argument('--suffix', default="")
    args = parser.parse_args()

    print("{}".format(args), file=sys.stdout)

    gen_merkle_hashes(args)
