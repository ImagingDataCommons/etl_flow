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
import settings

# Copying a BigQuery table to Cloud SQL using Python, including schema extraction and destination table creation,
# involves several steps: Extract BigQuery Table Schema.
# Use the BigQuery client library to fetch the schema of your source BigQuery table.
# This schema will be a list of SchemaField objects.


from google.cloud import bigquery

def get_bigquery_schema_and_data(project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)

    # Extract schema
    schema = []
    for field in table.schema:
        schema.append({"name": field.name, "field_type": field.field_type, "mode": field.mode})

    # Extract data (you might want to stream this for large tables)
    rows = client.list_rows(table_ref).to_dataframe()
    return schema, rows

def bigquery_type_to_postgresql_type(bigquery_type):
    type_map = {
        "STRING": "TEXT",
        "INTEGER": "BIGINT",
        "FLOAT": "DOUBLE PRECISION",
        "NUMERIC": "NUMERIC",
        "BOOLEAN": "BOOLEAN",
        "TIMESTAMP": "TIMESTAMP WITH TIME ZONE",
        "DATE": "DATE",
        "DATETIME": "TIMESTAMP WITHOUT TIME ZONE",
        "BYTES": "BYTEA",
        "ARRAY": "TEXT[]",  # Or more specific array types if known
        "STRUCT": "JSONB",   # Or define a custom composite type
    }
    return type_map.get(bigquery_type, "TEXT") # Default to TEXT for unknown types

import psycopg2

def create_postgresql_table(db_conn, table_name, schema):
    columns_sql = []
    for field in schema:
        pg_type = bigquery_type_to_postgresql_type(field["field_type"])
        null_constraint = "NOT NULL" if field["mode"] == "REQUIRED" else ""
        columns_sql.append(f"{field['name']} {pg_type} {null_constraint}")

    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns_sql)});"
    with db_conn.cursor() as cursor:
        cursor.execute(create_table_sql)
    db_conn.commit()


def insert_data_into_postgresql(db_conn, table_name, data_dataframe):
    # Prepare column names for the INSERT statement
    columns = ", ".join(data_dataframe.columns)

    # Prepare placeholders for values
    placeholders = ", ".join(["%s"] * len(data_dataframe.columns))

    insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    with db_conn.cursor() as cursor:
        for index, row in data_dataframe.iterrows():
            # Convert NaN to None for proper PostgreSQL handling
            values = [None if pd.isna(val) else val for val in row.values]
            cursor.execute(insert_sql, values)
    db_conn.commit()

if __name__ == "__main__":
    import pandas as pd
    from google.cloud import bigquery

    # BigQuery details
    bq_project_id = "idc-dev-etl"
    bq_dataset_id = "idc_v24_dev"
    bq_table_id = "temp_source_file_hashes"

    # Cloud SQL PostgreSQL details
    pg_host = settings.CLOUD_HOST
    pg_database = "idc_v24"
    pg_user = settings.CLOUD_USERNAME
    pg_password = settings.CLOUD_PASSWORD
    pg_table_name = "temp_source_file_hashes"

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