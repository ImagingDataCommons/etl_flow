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


# Copy the blobs that are new to a version from dev pre-staging buckets
# to dev staging buckets.

def get_instances_in_series(args, uuid):
    client = bigquery.Client()
    query = f"""
    SELECT
      distinct
      sop_instance_uid,
      i_uuid uuid,
      i_hash md5_hash,
      i_size size,
      i_init_idc_version init_idc_version,
      i_rev_idc_version rev_idc_version,
      i_final_idc_version final_idc_version
    FROM
      `idc-dev-etl.whc_dev.hfs_all_joined`
    WHERE
      se_uuid = '{uuid}' 
    ORDER BY sop_instance_uid
    """
    # urls = list(client.query(query))
    query_job = client.query(query)  # Make an API request.
    query_job.result()  # Wait for the query to complete.
    destination = query_job.destination
    destination = client.get_table(destination)
    return destination

def gen_series_object(args,
            series_instance_uid,
            source_doi,
            source_url,
            se_uuid,
            md5_hash,
            init_idc_version,
            rev_idc_version,
            final_idc_version):
    bq_client = bigquery.Client()
    destination = get_instances_in_series(args, se_uuid)
    instances = [instance for page in bq_client.list_rows(destination, page_size=args.batch).pages for instance in page ]
    # destination = get_parents_of_patient(args, st_uuid)
    collections = [collection for page in bq_client.list_rows(destination, page_size=args.batch).pages for collection in page ]

    series = {
        "encoding": "v1",
        "object_type": "series",
        "SeriesInstanceUID": series_instance_uid,
        "source_doi": source_doi,
        "source_url": source_url,
        "uuid": se_uuid,
        "md5_hash": md5_hash,
        "init_idc_version": init_idc_version,
        "rev_idc_version": rev_idc_version,
        "final_idc_version": final_idc_version,
        "self_uri": f"gs://{args.dst_bucket.name}/{se_uuid}.idc",
        "drs_object_id": f"dg.4DFC/{se_uuid}",
        "instances": {
            "gs":{
                "region": "us-central1",
                "urls":
                    {
                        "bucket": f"{args.dst_bucket.name}",
                        "folder": f"{se_uuid}",
                        "blobs":
                            [
                                {"SOPInstance_UID": f"{instance.sop_instance_uid}",
                                 "blob_name": f"{instance.uuid}.idc"} for instance in instances
                            ]
                    }
                },
            "drs":{
                "urls":
                    {
                        "server": "drs://nci-crdc.datacommons.io",
                        "object_ids":
                            [f"dg.4DFC/{instance.uuid}" for instance in instances]
                    }
                }
            }
        }
    blob = args.dst_bucket.blob(f"{se_uuid}.idc").upload_from_string(json.dumps(series))
    print(f'\t\t\t\t\tSeries {series_instance_uid}')

    return