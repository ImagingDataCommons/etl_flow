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


# Copying a BigQuery table to Cloud SQL using Python, including schema extraction and destination table creation,
# involves several steps:
# 1. Extract BigQuery Table Schema.
# Use the BigQuery client library to fetch the schema of your source BigQuery table.
# This schema will be a list of SchemaField objects.
# 2. Load all the data from some specified BQ table into a dataframe. Your VM will need enough memory for this
# 3. Create the destination table on a glcoud sql instance
# 4. Copy the data in BAtCH_SIZE chunks tp the table
#
# Performance is pretty poor, about 114s/MiB

import sys
from google.cloud import bigquery, storage
import settings
import argparse
from subprocess import run
import psycopg2
from utilities.logging_config import progresslogger


def get_bigquery_schema(project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)

    # Extract schema
    schema = []
    for field in table.schema:
        schema.append({"name": field.name, "field_type": field.field_type, "mode": field.mode})

    return schema


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


def create_postgresql_table(db_conn, table_name, schema):
    columns_sql = []
    for field in schema:
        pg_type = bigquery_type_to_postgresql_type(field["field_type"])
        null_constraint = "NOT NULL" if field["mode"] == "REQUIRED" else ""
        columns_sql.append(f"{field['name']} {pg_type} {null_constraint}")

    drop_table_sql = f"DROP TABLE IF EXISTS {table_name};"
    with db_conn.cursor() as cursor:
        cursor.execute(drop_table_sql)
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns_sql)});"
    with db_conn.cursor() as cursor:
        cursor.execute(create_table_sql)
    db_conn.commit()


def copy_table_to_gcs(project, dataset, table, thru_bucket):
    client = bigquery.Client()
    destination_uri = f"gs://{thru_bucket}/{table}_*.csv"
    dataset_ref = bigquery.DatasetReference(project, dataset)
    table_ref = dataset_ref.table(table)

    # Configure the export to be fast and compact
    job_config = bigquery.ExtractJobConfig()
    job_config.field_delimiter = ","
    job_config.print_header = False  # Postgres copy works faster without headers

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        job_config=job_config,
        location="US"  # Must match your BQ dataset location
    )
    result = extract_job.result()  # Wait for export to finish
    print("BigQuery table successfully exported to GCS shards.")

def copy_gcs_blobs_to_cloud_sql(cloudsql_instance, bucket, database, dst_table):
    client =storage.Client()
    bucket = client.bucket(bucket)
    blobs = bucket.list_blobs()
    for blob in blobs:
        cmd = [
            'gcloud',
            'sql',
            'import',
            'csv',
            cloudsql_instance,
            f'gs://{bucket.name}/{blob.name}',
            '--user=idc',
            f'--database={database}',
            f'--table={dst_table}',
            '--fields-terminated-by=2C',
            '--quiet',
            '--project=idc-dev-etl']
        cmd_string = ' '.join(cmd)
        result = run(cmd, capture_output=True)
        progresslogger.info(f'Copied {blob.name}')

    blobs = bucket.list_blobs()
    for blob in blobs:
        bucket.delete_blob(blob.name)

def main(args):
    # BigQuery details
    project = args.bq_project_id
    dataset_id = args.bq_dataset_id
    table_id = args.bq_table_id

    # Cloud SQL PostgreSQL details
    pg_host = settings.CLOUD_HOST
    pg_database = args.pg_database
    pg_user = settings.CLOUD_USERNAME
    pg_password = settings.CLOUD_PASSWORD
    pg_table_name = args.pg_table_name

    # Get BigQuery schema
    bq_schema = get_bigquery_schema(project, dataset_id, table_id)
    pg_conn = psycopg2.connect(host=pg_host, database=pg_database, user=pg_user, password=pg_password)
    create_postgresql_table(pg_conn, pg_table_name, bq_schema)

    cloudsql_instance = settings.CLOUD_INSTANCE.split(':')[-1]

    copy_table_to_gcs(project, dataset_id, table_id, args.thru_bucket)
    copy_gcs_blobs_to_cloud_sql(cloudsql_instance, args.thru_bucket, pg_database, pg_table_name)




if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # Table to copy from
    parser.add_argument("--bq_project_id", default="idc-dev-etl")
    parser.add_argument("--bq_dataset_id", default="idc_v0_dev")
    parser.add_argument("--bq_table_id", default="pre_instance")

    # Cloud SQL table to copy to
    parser.add_argument("--pg_database", default="idc_v0")
    parser.add_argument("--pg_table_name", default="pre_instanceq")

    parser.add_argument("--thru_bucket", default="whc_export_bq_to_cloud_sql")
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)

    main(args)
