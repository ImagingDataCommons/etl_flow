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

# Generate various manifests for a specified IDC sourced collection
# or analysis result. The instances or series included are identified
# by their source_doi.
## NOTE: Zenodo DOIs are versioned and we associated the version specific
## DOI with data when it is added or revised. It seems possible that objects
## in an IDC-version of a collection or result could have different DOI versions.
## We need to be careful to deal with this.
# The resulting manifest is copied to GCS


import argparse
import sys
from google.cloud import bigquery, storage

from time import sleep

import settings
from utilities.bq_helpers import query_BQ, export_BQ_to_GCS, delete_BQ_Table



def s5cmd_manifest(args, filename_prefix, source_doi, versioned_source_doi, service, url, subcollection_name=None):
    bq_client = bigquery.Client(project='idc-dev-etl')
    gcs_client = storage.Client(project='idc-dev-etl')
    query = f"""
    SELECT distinct concat('cp s3://', pub_{service}_bucket, '/', se_uuid, '/*  .') URL
    FROM `idc-dev-etl.idc_v{args.version}_dev.all_joined_public_and_current` aj
    WHERE source_doi = '{source_doi}' {f"AND REPLACE(REPLACE(LOWER(collection_id),'-','_'),' ','_')='{subcollection_name}'" if subcollection_name else ''}
    ORDER by URL
    """

    if subcollection_name:
        subcollection_id = subcollection_name
        file_name = f"{filename_prefix}-{subcollection_id}-{service}.s5cmd"
    else:
        file_name = f"{filename_prefix}-{service}.s5cmd"

    header = \
f'''# To download the files in this manifest, 
# first install idc-index python package (https://github.com/ImagingDataCommons/idc-index),
# download this manifest as {file_name}, then run the following command: 
#   idc download {file_name}
# 
# See IDC documentation for more details: https://learn.canceridc.dev/data/downloading-data'''
#     header = \
# f'''# To download the files in this manifest, first install s5cmd (https://github.com/peak/s5cmd)
# # then run the following command, substituting the name of this file:
# # s5cmd --no-sign-request --endpoint-url {url} run {file_name}'''

    series = [row.URL for row in bq_client.query(query).result()]
    manifest = header + '\n' + '\n'.join(series)

    bucket = gcs_client.bucket(args.manifest_bucket)

    # blob = bucket.blob(f"{source_doi.replace('/','_').replace('.','_')}/{versioned_source_doi.replace('/','_').replace('.','_')}/{file_name}")
    blob = bucket.blob(f"{filename_prefix}/v{args.version}/{file_name}")
    blob.upload_from_string(manifest)

    return


def dcf_manifest(args, filename_prefix, source_doi, versioned_source_doi, service, url, subcollection_name=None):
    bq_client = bigquery.Client(project='idc-dev-etl')
    gcs_client = storage.Client(project='idc-dev-etl')
    query = f"""
    SELECT distinct concat('dg.4DFC/',i_uuid) drs_uri
    FROM `idc-dev-etl.idc_v{args.version}_dev.all_joined_public_and_current` aj
    WHERE source_doi = '{source_doi}' {f"AND REPLACE(REPLACE(LOWER(collection_id),'-','_'),' ','_')='{subcollection_name}'" if subcollection_name else ''}
    ORDER by drs_uri
    """

    if subcollection_name:
        subcollection_id = subcollection_name
        file_name = f"{filename_prefix}-{subcollection_id}-{service}.csv"
    else:
        file_name = f"{filename_prefix}-{service}.csv"

    header = \
f'''# To obtain GCS and AWS URLs of the instances in this manifest, 
# resolve each drs_uri in this manifest, e.g.
# $ curl {url}<drs_uri>. 
# The GCS and AWS URL of the instance are found in the 
# 'access_methods' array of the returned JSON GA4GH DrsObject
# See https://ga4gh.github.io/data-repository-service-schemas/
# for more information on the GA4GH DRS'''

    drs_uris = [row.drs_uri for row in bq_client.query(query).result()]
    manifest = header + '\n' + '\n'.join(drs_uris)

    bucket = gcs_client.bucket(args.manifest_bucket)

    # blob = bucket.blob(f"{source_doi.replace('/','_').replace('.','_')}/{versioned_source_doi.replace('/','_').replace('.','_')}/{file_name}")
    blob = bucket.blob(f"{filename_prefix}/v{args.version}/{file_name}")
    blob.upload_from_string(manifest)

    return
