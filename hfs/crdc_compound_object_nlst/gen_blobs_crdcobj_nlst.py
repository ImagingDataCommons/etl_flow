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

# Genrate study and series folder objects which each contain a manifest of child objects
import json
import argparse
from collection_list_crdcobj_nlst import collection_list
from utilities.logging_config import successlogger, progresslogger, errlogger
from idc.models import Base, Version, Collection, Patient, Study, Series, Instance, All_Included_Collections
from google.cloud import storage
from google.cloud import bigquery, storage
import settings


def get_series(args):
    client = bigquery.Client()
    query = f"""
    SELECT
      distinct
      aj.se_uuid uuid,
      aj.series_instance_uid,
    FROM
      `idc-sandbox-001.dataset_nlst.nlst_analyzed_cohort_Sept2022` nlst
    JOIN
      `idc-dev-etl.idc_v{args.version}_dev.all_joined` aj
    ON nlst.SeriesInstanceUID = aj.series_instance_uid
    """
    # urls = list(client.query(query))
    series = client.query(query)  # Make an API request.
    # series = [{'SeriesInstanceUID': row['series_instance_uid'], 'series_uuid': row['se_uuid']} for row in query_job.result()]  # Wait for the query to complete.
    return series

def get_instances(args, series_uuid):
    client = bigquery.Client()
    query = f"""
    SELECT
      distinct i_uuid uuid, sop_instance_uid
    FROM
      `idc-dev-etl.idc_v{args.version}_dev.all_joined`
    WHERE se_uuid = '{series_uuid}'
    """
    # urls = list(client.query(query))
    instances = client.query(query)  # Make an API request.
    # instances = [row['i_uuid'] for row in query_job.result()]  # Wait for the query to complete.
    return instances

def gen_series_object(args, series, instances):
    level = "Series"
    if not args.dst_bucket.blob(f"{series.uuid}.idc").exists():
        print(f'\t\t\t{level} {series.uuid} started')
        # Create a combined "folder" and "bundle" blob
        contents = {
            'encoding_version': '1.0',
            'description': 'IDC CRDC DICOM series compound object',
            'object_type': 'DICOM series',
            'id': series.uuid,
            'name': series.series_instance_uid,
            'self_uri': f'drs://dg.4DFC/{series.uuid}',
            'access_methods': [
                {
                    'method': 'children',
                    "mime_type": 'application/dicom',
                    'description': 'List of DRS URIs of instances in this series',
                    'contents': [
                        {
                            'name': i.sop_instance_uid,
                            'drs_uri': f'drs://dg.4DFC/{i.uuid}'
                        } for i in instances
                    ],
                },
                {
                    'method': 'folder_object',
                    "mime_type": 'application/json',
                    'description': 'DRS URI that resolves to a gs or s3 folder corresponding to this series',
                    'contents': [
                        {
                            'name': f'{series.series_instance_uid}/',
                            'drs_uri': f'drs://dg.4DFC/some_TBD_folder_object_uuid'
                        }
                    ]
                },
                {
                    'method': 'archive_package',
                    "mime_type": 'application/zip',
                    'description': 'DRS URI that resolves to a zip archive of the instances in this series',
                    'contents': [
                        {
                            'name': f'{series.series_instance_uid}.zip',
                            'drs_uri': f'drs://dg.4DFC/some_TBD_archive_uuid'
                        }
                    ]
                }
            ],
        }
        blob = args.dst_bucket.blob(f"{series.uuid}/").upload_from_string(json.dumps({}))
        if not args.dst_bucket.blob(f"{series.uuid}/").exists():
            errlogger.error(f"{series.uuid}/ doesn't exist")
        blob = args.dst_bucket.blob(f"{series.uuid}/crdcobj.json").upload_from_string(json.dumps(contents))
        if not args.dst_bucket.blob(f"{series.uuid}/crdcobj.json").exists():
            errlogger.error(f"{series.uuid}/crdcobj.json doesn't exist")

        print(f'\t\t\t{level} {series.uuid} completed')
    else:

        print(f'\t\t\t{level} {series.uuid} skipped')
    return


def gen_all(args):
    seriess = get_series(args)
    for series in seriess:
        instances = get_instances(args, series.uuid)
        gen_series_object(args, series, instances)



if __name__ == '__main__':
    client = storage.Client()
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=12, help='Version to work on')
    breakpoint() # Bucket?
    parser.add_argument('--dst_bucket_name', default='crdcobj_dev', help='Bucket into which to copy blobs')
    args = parser.parse_args()

    args.id = 0  # Default process ID
    print(f'args: {json.dumps(args.__dict__, indent=2)}')
    args.dst_bucket = client.bucket(args.dst_bucket_name)

    gen_all(args)
