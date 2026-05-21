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

# This script creates a BQ External Connection (see https://cloud.google.com/bigquery/docs/working-with-connections#bq).
import subprocess
import settings
import argparse
from google.cloud import bigquery_connection_v1
# from google.cloud import bigquery_connection_v1 as bq_connection


def create_connection():
    client = bigquery_connection_v1.ConnectionServiceClient()
    type_ = bigquery_connection_v1.types.CloudSqlProperties.DatabaseType(1)
    credential = bigquery_connection_v1.types.CloudSqlCredential(
        username = settings.CLOUD_USERNAME,
        password = settings.CLOUD_PASSWORD
    )
    cloud_sql = bigquery_connection_v1.types.CloudSqlProperties(
        instance_id =  f'{settings.DEV_PROJECT}:us-central1:{settings.DEV_PROJECT}-psql-whc',
        database = f'idc_v{settings.CURRENT_VERSION}',
        type_ = type_,
        credential = credential
    )

    connection = bigquery_connection_v1.types.Connection(
        name = f'projects/{settings.DEV_PROJECT}/locations/{settings.BQ_REGION}/connections/etl_federated_query_idc_v{settings.CURRENT_VERSION}',
        friendly_name = f'etl_federated_query_idc_v{settings.CURRENT_VERSION}',
        description = '',
        cloud_sql = cloud_sql
    )

    request  = bigquery_connection_v1.CreateConnectionRequest(
        parent=f'projects/{settings.DEV_PROJECT}/locations/{settings.BQ_REGION}',
        connection_id = f'etl_federated_query_idc_v{settings.CURRENT_VERSION}',
        connection = connection,
    )

    response = client.create_connection(
        request=request
    )
    return

# """Prints details and summary information about connections for a given admin project and location"""
    # client = bq_connection.ConnectionServiceClient()
    # print(f"List of connections in project {project_id} in location {location}")
    # req = bq_connection.ListConnectionsRequest(
    #     parent=client.common_location_path(project_id, location)
    # )
    # for connection in client.list_connections(request=req):
    #     print(f"\tConnection {connection.friendly_name} ({connection.name})")

if __name__ == '__main__':
    create_connection()




