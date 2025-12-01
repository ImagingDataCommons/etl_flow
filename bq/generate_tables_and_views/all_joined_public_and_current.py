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

# Upload DB tables used in to generated subsequent BQ tables.
# Beginning with idc v8, the per version BQ datasets are split
# into idc_v<version>_dev and idc_v<version>_pub. These tables
# go into the former, generated tables into the latter.

import argparse
import settings
from google.cloud import bigquery
from utilities.bq_helpers import create_BQ_dataset

def create_all_joined_public_and_current(client):
    view_id = f"{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_public_and_current"
    view = bigquery.Table(view_id)

    view.view_query = f"""
    SELECT 
        aj.*, 
        li.license.license_url,
        li.license.license_long_name,
        li.license.license_short_name
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_public` aj
    JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.licenses` li
    ON aj.source_doi = li.source_doi AND aj.collection_id = li.collection_name
    WHERE idc_version={settings.CURRENT_VERSION} 
    AND metadata_sunset = 0
    """
    # Make an API request to create the view.
    client.delete_table(view_id, not_found_ok=True)
    view = client.create_table(view, exists_ok=True)
    print(f"Created {view.table_type}: {str(view.reference)}")
    return view


if __name__ == '__main__':
    # Create BQ datasets.
    BQ_client = bigquery.Client(project=settings.DEV_PROJECT)
    try:
        dataset = create_BQ_dataset(BQ_client, settings.BQ_DEV_INT_DATASET)
    except:
        # Presume the dataset already exists
        pass

    create_all_joined_public_and_current(BQ_client)




