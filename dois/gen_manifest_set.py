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
from dois.gen_manifest import dcf_manifest, s5cmd_manifest
from google.cloud import bigquery
import settings

def get_all_dois(idc_version):
    client = bigquery.Client()
    query = f'''
SELECT DISTINCT aj.source_doi, aj.versioned_source_doi, aj.collection_id, arm.ID analysis_result_id
FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined` aj
LEFT JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.analysis_results_metadata_idc_source` arm
ON aj.source_doi = arm.source_doi
WHERE (aj.versioned_source_doi is not Null and aj.versioned_source_doi != "")
AND aj.i_rev_idc_version=20
order by aj.source_doi, aj.collection_id
    '''

    all_dois = client.query_and_wait(query).to_dataframe()
    return all_dois

def gen_all_manifests(args):
    all_dois = get_all_dois(args.version)

    source_dois = sorted(all_dois['source_doi'].unique())
    for source_doi in source_dois:
        if not source_doi in args.skips:
            source_doi_data = all_dois[all_dois['source_doi']==source_doi]
            if len(source_doi_data) == 1:
                for _, data in source_doi_data.iterrows():
                    source_doi = data['source_doi']
                    versioned_source_doi = data['versioned_source_doi']
                    if data['analysis_result_id']:
                        filename_prefix = data['analysis_result_id'].lower().replace('-','_').replace(' ', '_')
                    else:
                        filename_prefix = data['collection_id'].lower().replace('-','_').replace(' ', '_')
                    dcf_manifest(args, filename_prefix, source_doi, versioned_source_doi, 'dcf', \
                                 'https://nci-crdc.datacommons.io/ga4gh/drs/v1/objects/', '')
                    s5cmd_manifest(args, filename_prefix, source_doi, versioned_source_doi, 'gcs', \
                                   'https://storage.googleapis.com', '')
                    s5cmd_manifest(args, filename_prefix, source_doi, versioned_source_doi, 'aws', \
                                   'https://s3.amazonaws.com', '')
            else:
                for _, data in source_doi_data.iterrows():
                    source_doi = data['source_doi']
                    versioned_source_doi = data['versioned_source_doi']
                    if data['analysis_result_id']:
                        filename_prefix = data['analysis_result_id'].lower().replace('-','_').replace(' ', '_')
                    else:
                        filename_prefix = data['collection_id'].lower().replace('-','_').replace(' ', '_')
                    subcollection = data['collection_id'].lower().replace('-','_').replace(' ', '_')
                    dcf_manifest(args, filename_prefix, source_doi, versioned_source_doi, 'dcf', \
                                 'https://nci-crdc.datacommons.io/ga4gh/drs/v1/objects/', subcollection)
                    s5cmd_manifest(args, filename_prefix, source_doi, versioned_source_doi, 'gcs', \
                                   'https://storage.googleapis.com', subcollection)
                    s5cmd_manifest(args, filename_prefix, source_doi, versioned_source_doi, 'aws', \
                                   'https://s3.amazonaws.com', subcollection)


if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version for which to build the table')
    parser.add_argument('--manifest_bucket', default='zenodo_manifests')
    parser.add_argument('--skips', default=['10.5281/zenodo.8345959'])
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)

    gen_all_manifests(args)
