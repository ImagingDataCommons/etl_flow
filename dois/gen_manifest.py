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



def s5cmd_manifest(args, collection_id, manifest_version, source_doi, service, url, subcollection_name=None):
    bq_client = bigquery.Client(project='idc-dev-etl')
    gcs_client = storage.Client(project='idc-dev-etl')
    # file_name = f"{collection_id.lower().replace('-','_').replace(' ','-')}_v{manifest_version}_{service}.s5cmd"
    query = f"""
    SELECT distinct concat('cp s3://', pub_{service}_idc_url, '/', se_uuid, '/*  .') URL
    FROM `idc-dev-etl.idc_v{args.version}_dev.all_joined_public_and_current` aj
    WHERE source_doi = '{source_doi}' {f"AND collection_id='{subcollection_name}'" if subcollection_name else ''}
    ORDER by URL
    """

    if subcollection_name:
        subcollection_id = subcollection_name.lower().replace('-','_').replace(' ','-')
        file_name = f"{collection_id.lower().replace('-','_').replace(' ','-')}-{subcollection_id}_{service}.s5cmd"
    else:
        file_name = f"{collection_id.lower().replace('-', '_').replace(' ', '-')}_{service}.s5cmd"

    header = \
f'''# To download the files in this manifest, first install idc-index python package (https://github.com/ImagingDataCommons/idc-index),
# then run the following command: 
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
    if subcollection_name:
        blob = bucket.blob(f"{collection_id.lower().replace('-','_').replace(' ','-')}/v{args.manifest_version}/{subcollection_id}/{file_name}")
    else:
        blob = bucket.blob(f"{collection_id.lower().replace('-','_').replace(' ','-')}/v{args.manifest_version}/{file_name}")
    blob.upload_from_string(manifest)

    return


def dcf_manifest(args, collection_id, manifest_version, source_doi, service, url, subcollection_name=None):
    bq_client = bigquery.Client(project='idc-dev-etl')
    gcs_client = storage.Client(project='idc-dev-etl')
    query = f"""
    SELECT distinct concat('dg.4DFC/',i_uuid) drs_uri
    FROM `idc-dev-etl.idc_v{args.version}_dev.all_joined_public_and_current` aj
    WHERE source_doi = '{source_doi}' {f"AND collection_id='{subcollection_name}'" if subcollection_name else ''}
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
    if subcollection_name:
        subcollection_id = subcollection_name.lower().replace('-','_').replace(' ','-')
        file_name = f"{collection_id.lower().replace('-','_').replace(' ','-')}-{subcollection_id}_{service}.csv"
        blob = bucket.blob(f"{collection_id.lower().replace('-','_').replace(' ','-')}/v{args.manifest_version}/{subcollection_id}/{file_name}")
    else:
        file_name = f"{collection_id.lower().replace('-', '_').replace(' ', '-')}_{service}.csv"
        blob = bucket.blob(f"{collection_id.lower().replace('-','_').replace(' ','-')}/v{args.manifest_version}/{file_name}")
    blob.upload_from_string(manifest)

    return



# if __name__ == '__main__':
#     parser =argparse.ArgumentParser()
#     # parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version for which to build the table')
#     parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version for which to build the table')
#     parser.add_argument('--temp_table_bqdataset', default=f'whc_dev')
#     parser.add_argument('--temp_table', default=f'doi_subcollection')
#     parser.add_argument('--manifest_bucket', default='doi_manifests')
#     parser.add_argument('--collection_id', default='rms_mutation_prediction_expert_annotations')
#     parser.add_argument('--manifest_version', default=settings.CURRENT_VERSION, help='IDC revision of the collection whose manifest is to be generated')
#     parser.add_argument('--source_doi', default='10.5281/zenodo.10462857', help="DOIs of series to be included in the manifest")
#
#     args = parser.parse_args()
#     print("{}".format(args), file=sys.stdout)
#
#     dcf_manifest(args, args.collection_id, args.manifest_version, args.source_dois, 'dcf', 'https://nci-crdc.datacommons.io/ga4gh/drs/v1/objects/')
#     s5cmd_manifest(args, args.collection_id, args.manifest_version, args.source_dois, 'gcs', 'https://storage.googleapis.com')
#     s5cmd_manifest(args, args.collection_id, args.manifest_version, args.source_dois, 'aws', 'https://s3.amazonaws.com')
#
