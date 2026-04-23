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

# Generate a table of current licenses for all sources in the current version, indexed by source_doi.
# License data obtained from idc-comet repo

import argparse
import sys
import json
import settings
from google.cloud import bigquery
from bq.bq_utilities import get_github_directory_contents_from_comet, get_data_from_comet
from utilities.logging_config import progresslogger, errlogger
import pandas as pd


def construct_license_table_from_comet(args):
    client = bigquery.Client()
    licenses = []
    collection_file_names = get_github_directory_contents_from_comet("collections/original", branch="release/v24")
    for collection_file_name in collection_file_names:
        collection_data = get_data_from_comet(f"collections/original/{collection_file_name}", branch="release/v24")
        for source in collection_data['sources']:
            licenses.append( dict(
                source_doi = source['concept_doi'] if 'concept_doi' in source else source['source_doi'],
                source_name = collection_data['collection_name'],
                source_type = 'original_data',
                license_url = source['license']['url'],
                license_long_name = source['license']['long_name'],
                license_short_name = source['license']['short_name']
            )
        )
    analysis_files_names = get_github_directory_contents_from_comet("collections/analysis", branch="release/v24")
    for analysis_file_name in analysis_files_names:
        analysis_data = get_data_from_comet(f"collections/analysis/{analysis_file_name}", branch="release/v24")
        licenses.append( dict(
            source_doi = analysis_data['source_doi'],
            source_name = analysis_data['analysis_result_id'],
            source_type = 'analysis_result',
            license_url = analysis_data['license']['url'],
            license_long_name = analysis_data['license']['long_name'],
            license_short_name = analysis_data['license']['short_name']
            )
        )
    licenses = pd.DataFrame(licenses)
    try:
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
        )
        table_id = f'idc-dev-etl.idc_v{settings.CURRENT_VERSION}_dev.{args.bqtable_name}'
        job = client.load_table_from_dataframe(
            licenses, table_id, job_config=job_config
        )  # Make an API request.
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )

        pass
    except Exception as exc:
        errlogger.error(f'Table creation failed: {exc}')
        exit

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version for which to build the table')
    parser.add_argument('--project', default=f'{settings.DEV_PROJECT}')
    parser.add_argument('--bqdataset_name', default=f'idc_v{settings.CURRENT_VERSION}_dev', help='BQ dataset name')
    parser.add_argument('--bqtable_name', default=f'licenses', help='BQ table name')
    args = parser.parse_args()

    print("{}".format(args), file=sys.stdout)

    # args.sql = open(args.sql).read()

    # construct_licenses_table(args)
    construct_license_table_from_comet(args)
