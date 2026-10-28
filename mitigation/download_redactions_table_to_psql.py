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
# Code from Gemini
# Export a specified BQ table to Cloud SQL


# Copy the redactions table of the current mitigation to Cloud SQL using Python, including schema extraction and destination table creation,
# involves several steps: Extract BigQuery Table Schema.
# Use the BigQuery client library to fetch the schema of your source BigQuery table.
# This schema will be a list of SchemaField objects.


import settings
from bq.cloud_sql_bq_IO.download_bq_to_psql import create_postgresql_table, insert_data_into_postgresql, get_bigquery_schema_and_data
import psycopg2

import pandas as pd
from google.cloud import bigquery

if __name__ == "__main__":
    # BigQuery details
    breakpoint()
    bq_project_id = "idc-dev-mitigation"
    bq_dataset_id = f'm{settings.MITIGATION_VERSION}'
    bq_table_id = "redactions"

    # Cloud SQL PostgreSQL details
    pg_host = settings.CLOUD_HOST
    pg_database = f"idc_v{settings.CURRENT_VERSION}"
    pg_user = settings.CLOUD_USERNAME
    pg_password = settings.CLOUD_PASSWORD
    pg_table_name = f'm{settings.MITIGATION_VERSION}'

    # 1. Get BigQuery schema and data
    bq_schema, bq_data = get_bigquery_schema_and_data(bq_project_id, bq_dataset_id, bq_table_id)

    # 2. Connect to Cloud SQL PostgreSQL
    pg_conn = psycopg2.connect(host=pg_host, database=pg_database, user=pg_user, password=pg_password)

    try:
        # 3. Create destination table in PostgreSQL
        create_postgresql_table(pg_conn, pg_table_name, bq_schema)

        # 4. Insert data into PostgreSQL
        insert_data_into_postgresql(pg_conn, pg_table_name, bq_data)
        print(
            f"Data from BigQuery table '{bq_table_id}' successfully copied to Cloud SQL PostgreSQL table '{pg_table_name}'.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        pg_conn.close()