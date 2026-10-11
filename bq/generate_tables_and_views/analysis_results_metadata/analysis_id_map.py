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

# Add new analysis results to the analysis_id_map, generating a uuid4
# for each.
# This script assumes that the analysis_results_descriptions table has
# been previously updated with any new analysis results.

import argparse
from google.cloud import bigquery
import pandas_gbq
from uuid import uuid4
import settings
from bq.bq_utilities import get_github_directory_contents_from_comet, \
    get_data_from_comet
import markdown

def get_analysis_result_names(path, branch):
    collection_files = get_github_directory_contents_from_comet(path, branch=branch)
    analysis_result_names = []
    for collection_file in collection_files:
        # print(collection_file)
        collection_data = get_data_from_comet(f"{path}/{collection_file}", branch=branch)
        analysis_result_names.append(collection_data["analysis_result_name"])
    return analysis_result_names

def update_table():
    client = bigquery.Client()
    query=f'''
    SELECT *
    FROM `{settings.DEV_PROJECT}.idc_v{settings.PREVIOUS_VERSION}_dev.analysis_id_map`
    '''
    analysis_id_map =  client.query_and_wait(query).to_dataframe()
    analysis_id_map_lowered = analysis_id_map.copy(deep=True)
    analysis_id_map_lowered['collection_id'] = analysis_id_map_lowered['collection_id'].str.lower()
    analysis_results_names = get_analysis_result_names("collections/analysis", args.comet_branch)
    analysis_results_names_lowered = [name.lower() for name in analysis_results_names]

    for analysis_result_name in analysis_results_names:
        if analysis_result_name not in analysis_id_map['collection_id'].values:
            if analysis_result_name.lower() in analysis_id_map_lowered['collection_id'].values:
                # This is note a new analysis result, add to map with existing idc_id
                analysis_id_map.loc[len(analysis_id_map)] = {
                    'collection_id': analysis_result_name,
                    'idc_id': analysis_id_map_lowered[analysis_id_map_lowered['collection_id']==analysis_result_name.lower()]['idc_id'].item()
                }
            else:
                # It's new
                analysis_id_map.loc[len(analysis_id_map)] = {'collection_id': analysis_result_name, 'idc_id': str(uuid4())}
    for collection_id in analysis_id_map['collection_id']:
        if collection_id.lower() not in analysis_results_names_lowered:
            analysis_id_map = analysis_id_map[analysis_id_map['collection_id'] != collection_id]

    pandas_gbq.to_gbq(analysis_id_map, f'{settings.BQ_DEV_INT_DATASET}.analysis_id_map', project_id=settings.DEV_PROJECT, if_exists='replace')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--comet_branch", default = f'release/v{settings.CURRENT_VERSION}')

    args = parser.parse_args()
    print('args: {}'.format(args))
    update_table()
