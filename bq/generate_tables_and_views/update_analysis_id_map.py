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
from utilities.sqlalchemy_helpers import sa_session
from idc.models import Analysis_Id_Map, Analysis_Results_Descriptions
from uuid import uuid4

def update_table():

    with sa_session() as sess:
        analysis_results_with_ids = [row[0] for row in sess.query(Analysis_Id_Map.collection_id).all()]
        analysis_results = [row[0] for row in sess.query(Analysis_Results_Descriptions.id).all()]
        for collection_id in analysis_results:
            if not collection_id in analysis_results_with_ids:
                map = Analysis_Id_Map()
                map.collection_id = collection_id
                map.idc_id = str(uuid4())
                sess.add(map)
        sess.commit()


if __name__ == '__main__':
    update_table()
