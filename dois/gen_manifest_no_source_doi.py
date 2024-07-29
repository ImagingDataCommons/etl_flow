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

# Generates manifests for all IDC sourced original collection data
# that does not already have a Zenodo page. This is exclusively
# IDC pathology except for nlm_visible_human_project

import argparse
import sys
from google.cloud import bigquery, storage

from time import sleep

import settings
from utilities.logging_config import successlogger,progresslogger,errlogger


def s5cmd_manifest(args, row, service):
    bq_client = bigquery.Client(project='idc-dev-etl')
    gcs_client = storage.Client(project='idc-dev-etl')
    # file_name = f"{collection_id.lower().replace('-','_').replace(' ','-')}_v{manifest_version}_{service}.s5cmd"
    file_name = f"{row['collection_id'].lower().replace('-','_').replace(' ','-')}_{service}.s5cmd"
    query = f"""
    SELECT distinct concat('cp s3://', pub_{service}_idc_url, '/', se_uuid, '/*  .') URL
    FROM `idc-dev-etl.idc_v{args.version}_dev.all_joined_public` aj
    WHERE collection_id = '{row["collection_id"]}' 
    AND idc_version = {row["idc_version"]} 
    AND i_source = 'idc'
    AND lower(source_url) = '{row["source_url"].lower()}'
    ORDER by URL
    """

    header = \
f'''# To download the files in this manifest, first install idc-index python package (https://github.com/ImagingDataCommons/idc-index),
# then run the following command: 
#   idc download {file_name}
# 
# See IDC documentation for more details: https://learn.canceridc.dev/data/downloading-data'''
# f'''# To download the files in this manifest, first install s5cmd (https://github.com/peak/s5cmd)
# # then run the following command:
# # s5cmd --no-sign-request --endpoint-url {url} run {file_name}'''

    series = [row.URL for row in bq_client.query(query).result()]
    manifest = header + '\n' + '\n'.join(series)

    bucket = gcs_client.bucket(args.manifest_bucket)
    blob = bucket.blob(f"{row['collection_id'].lower().replace('-','_').replace(' ','-')}/v{row['idc_version']}/{file_name}")
    blob.upload_from_string(manifest)

    return


def dcf_manifest(args, row, service, url):
    bq_client = bigquery.Client(project='idc-dev-etl')
    gcs_client = storage.Client(project='idc-dev-etl')
    # file_name = f"{collection_id.lower().replace('-','_').replace(' ','-')}_v{manifest_version}_{service}.csv"
    file_name = f"{row['collection_id'].lower().replace('-','_').replace(' ','-')}_{service}.csv"
    query = f"""
    SELECT distinct concat('dg.4DFC/',i_uuid) drs_uri
    FROM `idc-dev-etl.idc_v{args.version}_dev.all_joined_public` aj
    WHERE collection_id = '{row["collection_id"]}' 
    AND idc_version = {row["idc_version"]} 
    AND i_source = 'idc'
    AND lower(source_url) = '{row["source_url"].lower()}'
    ORDER by drs_uri
    """

    header = \
f'''# To obtain GCS and AWS URLs of the instances in this manifest, 
# resolve each drs_uri in this manifest, e.g.
# $ curl {url}<drs_uri>. 
# The GCS and AWS URL of the instance are found in the 
# 'access_methods' array of the returned JSON GA4GH DrsObject
# See https://ga4gh.github.io/data-repository-service-schemas/
# For more information on the GA4GH DRS'''

    drs_uris = [row.drs_uri for row in bq_client.query(query).result()]
    manifest = header + '\n' + '\n'.join(drs_uris)

    bucket = gcs_client.bucket(args.manifest_bucket)
    blob = bucket.blob(f"{row['collection_id'].lower().replace('-','_').replace(' ','-')}/v{row['idc_version']}/{file_name}")
    blob.upload_from_string(manifest)

    return

def gen_zenodo_manifests(args):
    dones = open(successlogger.handlers[0].baseFilename).read().splitlines()

    bq_client = bigquery.Client(project='idc-dev-etl')
    query = f"""    
with versions AS (
(SELECT DISTINCT se_rev_idc_version idc_version, collection_id, source_doi, source_url
 FROM `idc-dev-etl.idc_v{args.version}_dev.all_joined_public`
 WHERE i_source='idc'
 )
 UNION ALL
 (SELECT DISTINCT se_final_idc_version+1 idc_version, collection_id, source_doi, source_url
 FROM `idc-dev-etl.idc_v{args.version}_dev.all_joined_public`
 WHERE i_source='idc' AND se_final_idc_version != 0
 )
)
SELECT DISTINCT * FROM versions
WHERE source_url NOT LIKE '%zenodo%'
ORDER BY idc_version, collection_id"""

    for row in bq_client.query(query):
        if not f'{row.collection_id}/{row.idc_version}' in dones:
            dcf_manifest(args, row, 'dcf',
                         'https://nci-crdc.datacommons.io/ga4gh/drs/v1/objects/')
            s5cmd_manifest(args, row, 'gcs')
            s5cmd_manifest(args, row, 'aws')
            successlogger.info(f'{row.collection_id}/{row.idc_version}')
            print(f'{row.collection_id}/{row.idc_version}')

        else:
            print(f'{row.collection_id}/{row.idc_version} previously done')



if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    # parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version for which to build the table')
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version for which to build the table')
    parser.add_argument('--manifest_bucket', default='doi_manifests')
    parser.add_argument('--collection_id', default='CPTAC-LSCC')
    parser.add_argument('--idc_version', default=3, help='IDC revision of the collection whose manifest is to be generated')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)

    gen_zenodo_manifests(args)

    # dcf_manifest(args, args.collection_id, args.idc_version, 'dcf', 'https://nci-crdc.datacommons.io/ga4gh/drs/v1/objects/')
    # s5cmd_manifest(args, args.collection_id, args.idc_version, 'gcs', 'https://storage.googleapis.com')
    # s5cmd_manifest(args, args.collection_id, args.idc_version, 'aws', 'https://s3.amazonaws.com')

