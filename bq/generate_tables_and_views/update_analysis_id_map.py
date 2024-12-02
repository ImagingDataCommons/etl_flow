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

from google.cloud import bigquery
import pandas as pd
import pandas_gbq
from uuid import uuid4
import settings

def update_table():
    client = bigquery.Client()
    query=f'''
    SELECT *
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.analysis_id_map`
    '''
    analysis_id_map =  client.query_and_wait(query).to_dataframe()
    query = f'''
    SELECT *
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.analysis_results_descriptions`
    '''
    analysis_results_descriptions =  client.query_and_wait(query).to_dataframe()
    for id in analysis_results_descriptions['id']:
        if id not in analysis_id_map['collection_id'].values:
            analysis_id_map.loc[len(analysis_id_map)] = {'collection_id': id, 'idc_id': str(uuid4())}
    pandas_gbq.to_gbq(analysis_id_map, f'{settings.BQ_DEV_INT_DATASET}.analysis_id_mapx', project_id=settings.DEV_PROJECT, if_exists='replace')


if __name__ == '__main__':
    update_table()
