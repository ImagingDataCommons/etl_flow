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

# Generate the manifest for a single (sub)collection version
# The resulting manifest is copied to GCS

import argparse
import sys
from google.cloud import bigquery, storage

from time import sleep

import settings
from utilities.bq_helpers import query_BQ, export_BQ_to_GCS, delete_BQ_Table



def s5cmd_manifest(args, collection_id, manifest_version, source_doi, service, url):
    bq_client = bigquery.Client(project='idc-dev-etl')
    gcs_client = storage.Client(project='idc-dev-etl')
    # file_name = f"{collection_id.lower().replace('-','_').replace(' ','-')}_v{manifest_version}_{service}.s5cmd"
    file_name = f"{collection_id.lower().replace('-','_').replace(' ','-')}_{service}.s5cmd"
    query = f"""
    SELECT distinct concat('cp s3://', pub_{service}_idc_url, '/', se_uuid, '/*  .') URL
    FROM `idc-dev-etl.idc_v{args.version}_dev.all_joined` aj
    JOIN `idc-dev-etl.idc_v{args.version}_dev.all_collections` ac
    ON aj.idc_collection_id = ac.idc_collection_id
    WHERE collection_id = '{collection_id}' 
        AND idc_version = {manifest_version} 
        AND source_doi = '{source_doi}'
        AND se_sources.idc=True
    ORDER by URL
    """

    header = \
f'''# To download the files in this manifest, first install s5cmd (https://github.com/peak/s5cmd)
# then run the following command, substituting the name of this file:
# s5cmd --no-sign-request --endpoint-url {url} run {file_name}'''

    series = [row.URL for row in bq_client.query(query).result()]
    manifest = header + '\n' + '\n'.join(series)

    bucket = gcs_client.bucket(args.manifest_bucket)
    blob = bucket.blob(file_name)
    blob.upload_from_string(manifest)

    return


def dcf_manifest(args, collection_id, manifest_version, source_doi, service, url):
    bq_client = bigquery.Client(project='idc-dev-etl')
    gcs_client = storage.Client(project='idc-dev-etl')
    # file_name = f"{collection_id.lower().replace('-','_').replace(' ','-')}_v{manifest_version}_{service}.csv"
    file_name = f"{collection_id.lower().replace('-','_').replace(' ','-')}_{service}.csv"
    query = f"""
    SELECT distinct concat('dg.4DFC/',i_uuid) drs_uri
    FROM `idc-dev-etl.idc_v{args.version}_dev.all_joined` aj
    JOIN `idc-dev-etl.idc_v{args.version}_dev.all_collections` ac
    ON aj.idc_collection_id = ac.idc_collection_id
    WHERE collection_id = '{collection_id}' 
        AND idc_version = {manifest_version} 
        AND source_doi = '{source_doi}'
        AND se_sources.idc=True
    ORDER by drs_uri
    """

    header = \
f'''# To obtain GCS and AWS URLs of the instances in this series, 
# resolve each drs_uri in this manifest, e.g.
# $ curl {url}<drs_uri>. 
# The GCS and AWS URL of the instance are found in the 
# 'access_methods' array of the returned JSON DrsObject'''

    drs_uris = [row.drs_uri for row in bq_client.query(query).result()]
    manifest = header + '\n' + '\n'.join(drs_uris)

    bucket = gcs_client.bucket(args.manifest_bucket)
    blob = bucket.blob(file_name)
    blob.upload_from_string(manifest)

    return



if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    # parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version for which to build the table')
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version for which to build the table')
    parser.add_argument('--temp_table_bqdataset', default=f'whc_dev')
    parser.add_argument('--temp_table', default=f'doi_subcollection')
    parser.add_argument('--manifest_bucket', default='doi_manifests')
    parser.add_argument('--collection_id', default='RMS-Mutation-Prediction')
    parser.add_argument('--manifest_version', default=settings.CURRENT_VERSION, help='IDC revision of the collection whose manifest is to be generated')
    parser.add_argument('--source_doi', default='10.5281/zenodo.8225131', help='DOI of series to be included in the manifest')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)

    dcf_manifest(args, args.collection_id, args.manifest_version, args.source_doi, 'dcf', 'https://nci-crdc.datacommons.io/ga4gh/drs/v1/objects/')
    s5cmd_manifest(args, args.collection_id, args.manifest_version, args.source_doi, 'gcs', 'https://storage.googleapis.com')
    s5cmd_manifest(args, args.collection_id, args.manifest_version, args.source_doi, 'aws', 'https://s3.amazonaws.com')

