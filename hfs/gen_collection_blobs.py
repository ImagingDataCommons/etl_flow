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
from hfs.gen_patient_blobs import gen_patient_object


def get_patients_in_collection(args, uuid):
    client = bigquery.Client()
    query = f"""
    SELECT
      distinct
      submitter_case_id,
      idc_case_id,
      p_uuid uuid,
      p_hashes.all_hash md5_hash,
      p_init_idc_version init_idc_version,
      p_rev_idc_version rev_idc_version,
      p_final_idc_version final_idc_version
    FROM
      `idc-dev-etl.whc_dev.hfs_all_joined`
    WHERE
      c_uuid = '{uuid}'
    ORDER BY submitter_case_id
    """
    # urls = list(client.query(query))
    query_job = client.query(query)  # Make an API request.
    query_job.result()  # Wait for the query to complete.
    destination = query_job.destination
    destination = client.get_table(destination)
    return destination

# def get_parents_of_collection(args, uuid):
#     client = bigquery.Client()
#     query = f"""
#     SELECT
#       distinct
#       idc_version
#     FROM
#       `idc-dev-etl.whc_dev.hfs_all_joined`
#     WHERE
#       c_uuid = {uuid}
#     """
#     # urls = list(client.query(query))
#     query_job = client.query(query)  # Make an API request.
#     query_job.result()  # Wait for the query to complete.
#     destination = query_job.destination
#     destination = client.get_table(destination)
#     return destination


def gen_collection_object(args,
            collection_id,
            c_uuid,
            md5_hash,
            init_idc_version,
            rev_idc_version,
            final_idc_version):
    bq_client = bigquery.Client()
    destination = get_patients_in_collection(args, c_uuid)
    patients = [patient for page in bq_client.list_rows(destination, page_size=args.batch).pages for patient in page ]
    # destination = get_parents_of_collection(args, c_uuid)
    # versions = [version for page in bq_client.list_rows(destination, page_size=args.batch).pages for version in page ]

    collection = {
        "encoding": "v1",
        "object_type": "collection",
        "tcia_api_collection_id": collection_id,
        "idc_webapp_collection_id": collection_id.lower().replace('-','_').replace(' ','_'),
        "uuid": c_uuid,
        "md5_hash": md5_hash,
        "init_idc_version": init_idc_version,
        "rev_idc_version": rev_idc_version,
        "final_idc_version": final_idc_version,
        "self_uri": f"gs://{args.dst_bucket.name}/{c_uuid}.idc",
        "patients": {
            "gs":{
                "region": "us-central1",
                "urls":
                    {
                        "bucket": f"{args.dst_bucket.name}",
                        "blobs":
                            [
                                {"submitter_case_id": f"{patient.submitter_case_id}",
                                 "blob_name": f"{patient.uuid}.idc"} for patient in patients
                            ]
                    }
                }
             }
        }
    blob = args.dst_bucket.blob(f"{c_uuid}.idc").upload_from_string(json.dumps(collection))
    for patient in patients:
        gen_patient_object(args,
            patient.submitter_case_id,
            patient.idc_case_id,
            patient.uuid,
            patient.md5_hash,
            patient.init_idc_version,
            patient.rev_idc_version,
            patient.final_idc_version)
    print(f'\t\tCollection {collection_id}')

    return