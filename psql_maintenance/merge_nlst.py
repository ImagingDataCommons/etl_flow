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

"""One time script to merge NLST data into a different DB """

import sys
import os
import argparse
from logging import INFO
import logging
from python_settings import settings
import settings as etl_settings

settings.configure(etl_settings)
assert settings.configured

import psycopg2
from psycopg2.extras import DictCursor
import pg8000


def merge_collections(cur, args):
    query = """
    select distinct 
        collection_timestamp, 
        tcia_api_collection_id, 
        c_revised, c_done, c_is_new, c_expanded, 
        collection_hash 
        from nlst
    """
    cur.execute(query)
    rows = cur.fetchall()
    assert len(rows)==1
    l='c'
    level='collection'
    values=[]
    # for row in rows:
    for row in rows:
        data = [
            row[f'{level}_timestamp'],
            row['tcia_api_collection_id'],
            row[f'{l}_revised'],
            row[f'{l}_done'],
            row[f'{l}_is_new'],
            row[f'{l}_expanded'],
            4,
            4,
            row[f'{level}_timestamp'],
            (True,False),
            (row[f'{level}_hash'],"", "")
        ]
        values.append(data)

    query = f"""
        INSERT INTO {level}_nlst (
            min_timestamp,
            collection_id,
            revised,
            done,
            is_new,
            expanded,
            init_idc_version,
            rev_idc_version,
            max_timestamp,
            sources,
            hashes) values %s
    """

    psycopg2.extras.execute_values(
        cur, query, values, template=None, page_size=1000
    )
    cur.connection.commit()


def merge_patients(cur, args):
    l='p'
    level='patient'
    query = f"""
    select distinct 
        {level}_timestamp, 
        submitter_case_id,
        idc_case_id,
        {l}_revised, {l}_done, {l}_is_new, {l}_expanded,
        tcia_api_collection_id,
        {level}_hash 
        from nlst
    """
    cur.execute(query)
    rows = cur.fetchall()
    values=[]
    # for row in rows:
    for row in rows:
        data = [
            row[f'{level}_timestamp'],
            row['submitter_case_id'],
            row['idc_case_id'],
            row[f'{l}_revised'],
            row[f'{l}_done'],
            row[f'{l}_is_new'],
            row[f'{l}_expanded'],
            4,
            4,
            row[f'tcia_api_collection_id'],
            row[f'{level}_timestamp'],
            (True,False),
            (row[f'{level}_hash'],"", "")
        ]
        values.append(data)

    query = f"""
        INSERT INTO {level}_nlst (
            min_timestamp,
            submitter_case_id,
            idc_case_id,
            revised,
            done,
            is_new,
            expanded,
            init_idc_version,
            rev_idc_version,
            collection_id,
            max_timestamp,
            sources,
            hashes) values %s
    """

    psycopg2.extras.execute_values(
        cur, query, values, template=None, page_size=1000
    )
    cur.connection.commit()


def merge_studies(cur, args):
    l='st'
    level='study'
    query = f"""
    select distinct 
        {level}_timestamp, 
        {level}_instance_uid,
        {level}_uuid,
        {level}_instances,
        {l}_revised, {l}_done, {l}_is_new, {l}_expanded,
        submitter_case_id,
        {level}_hash 
        from nlst
    """
    cur.execute(query)
    rows = cur.fetchall()
    values=[]
    # for row in rows:
    for row in rows:
        data = [
            row[f'{level}_timestamp'],
            row[f'{level}_instance_uid'],
            row[f'{level}_uuid'],
            row[f'{level}_instances'],
            row[f'{l}_revised'],
            row[f'{l}_done'],
            row[f'{l}_is_new'],
            row[f'{l}_expanded'],
            4,
            4,
            row[f'submitter_case_id'],
            row[f'{level}_timestamp'],
            (True,False),
            (row[f'{level}_hash'],"", "")
        ]
        values.append(data)

    query = f"""
        INSERT INTO {level}_nlst (
            min_timestamp,
            study_instance_uid,
            uuid,
            study_instances,
            revised,
            done,
            is_new,
            expanded,
            init_idc_version,
            rev_idc_version,
            submitter_case_id,
            max_timestamp,
            sources,
            hashes) values %s
    """

    psycopg2.extras.execute_values(
        cur, query, values, template=None, page_size=1000
    )
    cur.connection.commit()


def merge_series(cur, args):
    l='se'
    level='series'
    query = f"""
    select distinct 
        {level}_timestamp, 
        {level}_instance_uid,
        {level}_uuid,
        {level}_instances,
        source_doi,
        {l}_revised, {l}_done, {l}_is_new, {l}_expanded,
        study_instance_uid,
        {level}_hash 
        from nlst
    """
    cur.execute(query)
    rows = cur.fetchall()
    values=[]
    # for row in rows:
    for row in rows:
        data = [
            row[f'{level}_timestamp'],
            row[f'{level}_instance_uid'],
            row[f'{level}_uuid'],
            row[f'{level}_instances'],
            'https://wiki.cancerimagingarchive.net/display/NLST/National+Lung+Screening+Trial',
            row[f'{l}_revised'],
            row[f'{l}_done'],
            row[f'{l}_is_new'],
            row[f'{l}_expanded'],
            4,
            4,
            row[f'study_instance_uid'],
            row[f'{level}_timestamp'],
            (True,False),
            (row[f'{level}_hash'],"", "")
        ]
        values.append(data)

    query = f"""
        INSERT INTO {level}_nlst (
            min_timestamp,
            series_instance_uid,
            uuid,
            series_instances,
            source_doi,
            revised,
            done,
            is_new,
            expanded,
            init_idc_version,
            rev_idc_version,
            study_instance_uid,
            max_timestamp,
            sources,
            hashes) values %s
    """

    psycopg2.extras.execute_values(
        cur, query, values, template=None, page_size=1000
    )
    cur.connection.commit()


# def merge_instances(cur, args):
#     l='i'
#     level='instance'
#     for ch in '0123456789abcdef':
#         query = f"""
#         select distinct
#             {level}_timestamp,
#             sop_instance_uid,
#             {level}_uuid,
#             {level}_hash,
#             {level}_size,
#             {l}_revised, {l}_done, {l}_is_new, {l}_expanded,
#             series_instance_uid
#             from nlst
#             where instance_uuid like '{ch}%'
#         """
#         # query = """
#         #     select * from nlst"""
#         cur.execute(query)
#         rows = cur.fetchall()
#
#         values=[]
#         # for row in rows:
#         for row in rows:
#             data = [
#                 row[f'{level}_timestamp'],
#                 row[f'sop_instance_uid'],
#                 row[f'{level}_uuid'],
#                 row[f'{level}_hash'],
#                 row[f'{level}_size'],
#                 row[f'{l}_revised'],
#                 row[f'{l}_done'],
#                 row[f'{l}_is_new'],
#                 row[f'{l}_expanded'],
#                 4,
#                 4,
#                 row[f'series_instance_uid'],
#                 'tcia'
#             ]
#             values.append(data)
#
#         query = f"""
#             INSERT INTO {level}_nlst (
#                 timestamp,
#                 sop_instance_uid,
#                 uuid,
#                 hash,
#                 size,
#                 revised,
#                 done,
#                 is_new,
#                 expanded,
#                 init_idc_version,
#                 rev_idc_version,
#                 series_instance_uid,
#                 source) values %s
#         """
#
#         psycopg2.extras.execute_values(
#             cur, query, values, template=None, page_size=50000
#         )
#         count = len(rows)
#         print(f'Merged {count} instances')
#
#         # while True:
#         #     rows = cur.fetchmany(increment)
#         #     if len(rows) == 0:
#         #         break
#         #
#         #     values=[]
#         #     # for row in rows:
#         #     for row in rows:
#         #         data = [
#         #             row[f'{level}_timestamp'],
#         #             row[f'sop_instance_uid'],
#         #             row[f'{level}_uuid'],
#         #             row[f'{level}_hash'],
#         #             row[f'{level}_size'],
#         #             row[f'{l}_revised'],
#         #             row[f'{l}_done'],
#         #             row[f'{l}_is_new'],
#         #             row[f'{l}_expanded'],
#         #             4,
#         #             4,
#         #             row[f'series_instance_uid'],
#         #             'tcia'
#         #         ]
#         #         values.append(data)
#         #
#         #     query = f"""
#         #         INSERT INTO {level}_nlst (
#         #             timestamp,
#         #             sop_instance_uid,
#         #             uuid,
#         #             hash,
#         #             size,
#         #             revised,
#         #             done,
#         #             is_new,
#         #             expanded,
#         #             init_idc_version,
#         #             rev_idc_version,
#         #             series_instance_uid,
#         #             source) values %s
#         #     """
#         #
#         #     psycopg2.extras.execute_values(
#         #         cur, query, values, template=None, page_size=50000
#         #     )
#         #     count += len(rows)
#         #     print(f'Merged {count} instances')
#
#     cur.connection.commit()


def merge_instances(cur, args):
    l='i'
    level='instance'
    query = f"""
    select distinct 
        {level}_timestamp, 
        sop_instance_uid,
        {level}_uuid,
        {level}_hash,
        {level}_size,
        {l}_revised, {l}_done, {l}_is_new, {l}_expanded,
        series_instance_uid
        from nlst
    """
    cur.execute(query)
    count =0
    increment = 50000
    values = []
    while True:
        rows = cur.fetchmany(increment)
        if len(rows) == 0:
            break

        # for row in rows:
        for row in rows:
            data = [
                row[f'{level}_timestamp'],
                row[f'sop_instance_uid'],
                row[f'{level}_uuid'],
                row[f'{level}_hash'],
                row[f'{level}_size'],
                row[f'{l}_revised'],
                row[f'{l}_done'],
                row[f'{l}_is_new'],
                row[f'{l}_expanded'],
                4,
                4,
                row[f'series_instance_uid'],
                'tcia'
            ]
            values.append(data)

        query = f"""
            INSERT INTO {level}_nlst (
                timestamp,
                sop_instance_uid,
                uuid,
                hash,
                size,
                revised,
                done,
                is_new,
                expanded,
                init_idc_version,
                rev_idc_version,
                series_instance_uid,
                source) values %s
        """

        count += len(rows)
        print(f'Merged {count} instances')

    psycopg2.extras.execute_values(
        cur, query, values, template=None, page_size=1000
    )
    cur.connection.commit()




def merge(args):
    conn = psycopg2.connect(dbname=args.db, user='idc',
                            password=settings.CLOUD_PASSWORD, host='0.0.0.0', port=settings.CLOUD_PORT)
    # conn = psycopg2.connect(dbname=args.db, user='idc',
    #                         password=settings.LOCAL_PASSWORD, host='0.0.0.0', port=settings.LOCAL_PORT)
    with conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            # merge_collections(cur, args)
            # merge_patients(cur, args)
            # merge_studies(cur, args)
            # merge_series(cur, args)
            merge_instances(cur, args)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', default='idc-dev-etl')
    parser.add_argument('--db', default='idc_v4')
    args = parser.parse_args()

    # Directory to which to download files from TCIA/NBIA
    args.dicom = 'dicom'
    print("{}".format(args), file=sys.stdout)

    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/vallog.log'.format(os.environ['PWD']))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    # donelogger = logging.getLogger('done')
    # done_fh = logging.FileHandler(args.validated)
    # doneformatter = logging.Formatter('%(message)s')
    # donelogger.addHandler(done_fh)
    # done_fh.setFormatter(doneformatter)
    # donelogger.setLevel(INFO)
    #
    errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/valerr.log'.format(os.environ['PWD']))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    merge(args)
