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

import os
import argparse
import logging
from logging import INFO
from google.cloud import bigquery, storage
import json
from hfs.gen_series_blobs import gen_series_object


# Copy the blobs that are new to a version from dev pre-staging buckets
# to dev staging buckets.

def get_series_in_study(args, uuid):
    client = bigquery.Client()
    query = f"""
    SELECT
      distinct
      series_instance_uid,
      source_doi,
      source_url,
      se_uuid uuid,
      se_hashes.all_hash md5_hash,
      se_init_idc_version init_idc_version,
      se_rev_idc_version rev_idc_version,
      se_final_idc_version final_idc_version
    FROM
      `idc-dev-etl.whc_dev.hfs_all_joined`
    WHERE
      st_uuid = '{uuid}' 
    ORDER BY series_instance_uid
    """
    # urls = list(client.query(query))
    query_job = client.query(query)  # Make an API request.
    query_job.result()  # Wait for the query to complete.
    destination = query_job.destination
    destination = client.get_table(destination)
    return destination

# def get_parents_of_patient(args, uuid):
#     client = bigquery.Client()
#     query = f"""
#     SELECT
#       distinct
#       c_uuid uuid
#     FROM
#       `idc-dev-etl.whc_dev.hfs_all_joined`
#     WHERE
#       p_uuid = {uuid}
#     """
#     # urls = list(client.query(query))
#     query_job = client.query(query)  # Make an API request.
#     query_job.result()  # Wait for the query to complete.
#     destination = query_job.destination
#     destination = client.get_table(destination)
#     return destination


def gen_study_object(args,
            study_instance_uid,
            st_uuid,
            md5_hash,
            init_idc_version,
            rev_idc_version,
            final_idc_version):
    bq_client = bigquery.Client()
    destination = get_series_in_study(args, st_uuid)
    seriess = [series for page in bq_client.list_rows(destination, page_size=args.batch).pages for series in page ]
    # destination = get_parents_of_patient(args, st_uuid)
    # collections = [collection for page in bq_client.list_rows(destination, page_size=args.batch).pages for collection in page ]

    study = {
        "encoding": "v1",
        "object_type": "study",
        "StudyInstanceUID": study_instance_uid,
        "uuid": st_uuid,
        "md5_hash": md5_hash,
        "init_idc_version": init_idc_version,
        "rev_idc_version": rev_idc_version,
        "final_idc_version": final_idc_version,
        "self_uri": f"gs://{args.dst_bucket.name}/{st_uuid}.idc",
        "drs_object_id": f"dg.4DFC/{st_uuid}",
        "series": {
            "gs":{
                "region": "us-central1",
                "urls":
                    {
                        "bucket": f"{args.dst_bucket.name}",
                        "blobs":
                            [
                                {"SeriesInstanceUID": f"{series.series_instance_uid}",
                                 "blob_name": f"{series.uuid}.idc"} for series in seriess
                            ]
                    }
                },
            "drs":{
                "urls":
                    {
                        "server": "drs://nci-crdc.datacommons.io",
                        "object_ids":
                            [f"dg.4DFC/{series.uuid}" for series in seriess]
                    }
                }
            }
        }
    blob = args.dst_bucket.blob(f"{st_uuid}.idc").upload_from_string(json.dumps(study))
    for series in seriess:
        gen_series_object(args,
            series.series_instance_uid,
            series.source_doi,
            series.source_url,
            series.uuid,
            series.md5_hash,
            series.init_idc_version,
            series.rev_idc_version,
            series.final_idc_version)
    print(f'\t\t\t\tStudy {study_instance_uid}')

    return