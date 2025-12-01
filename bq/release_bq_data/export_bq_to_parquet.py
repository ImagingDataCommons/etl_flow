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

from google.cloud import bigquery
import pandas as pd
import settings
from utilities.logging_config import progresslogger

client = bigquery.Client()

def get_tables(dataset_name):
    """
    Queries distinct table names from a specified dataset in BigQuery and filters out tables containing 'view'.
    Returns lists of regular and clinical table names.

    Parameters:
    dataset_name (str): The name of the dataset to query.

    Returns:
    dict: A dictionary with two lists, one for regular table names and one for clinical table names,
          with table names not containing 'view'.
    """

    # Function to query distinct tables
    def query_tables(suffix):
        """
        Helper function to query distinct table names from a dataset with an optional suffix.

        Parameters:
        suffix (str): The suffix to append to the dataset name, if any (e.g., '_clinical').

        Returns:
        DataFrame: A DataFrame with distinct table names from the queried dataset.
        """
        tables_sql = f'''
        SELECT distinct table_name
        FROM {settings.AH_PROJECT}.{dataset_name}{suffix}.INFORMATION_SCHEMA.COLUMNS
        ORDER BY table_name
        '''
        return client.query(tables_sql).to_dataframe()

    # Get distinct tables for regular and clinical datasets
    regular_tables_df = query_tables('')
    clinical_tables_df = query_tables('_clinical')

    # Filter out table names containing 'view' using pandas
    def filter_tables(df):
        """
        Helper function to filter out table names containing 'view' from a DataFrame.

        Parameters:
        df (DataFrame): The DataFrame to filter.

        Returns:
        DataFrame: A DataFrame with table names not containing 'view'.
        """
        return df[~df['table_name'].str.contains('view', case=False)]

    # Apply the filter to both DataFrames and convert to lists
    regular_tables_list = filter_tables(regular_tables_df)['table_name'].tolist()
    clinical_tables_list = filter_tables(clinical_tables_df)['table_name'].tolist()

    # Return a dictionary containing both lists
    return {
        'regular_tables': regular_tables_list,
        'clinical_tables': clinical_tables_list
    }


def export_data_from_bigquery(bucket_id, dataset_name, table_name, overwrite=False):
    """
    Exports data from BigQuery to a Google Cloud Storage bucket.

    Args:
        bucket_id (str): The ID of the Google Cloud Storage bucket.
        dataset_version (str): The version of the BigQuery dataset.
        table_name (str): The name of the table in BigQuery.

    Returns:
        None
    """
    print(f"Exporting data from {dataset_name}.{table_name} to {bucket_id}")
    export_sql = f"""
    EXPORT DATA
      OPTIONS (
        uri = 'gs://{bucket_id}/bigquery_export/{dataset_name}/{table_name}/*.parquet',
        format = 'PARQUET',
        compression = 'ZSTD',
        overwrite = {overwrite})
    AS (
      SELECT *
      FROM {settings.AH_PROJECT}.{dataset_name}.{table_name}
    );
    """
    client.query(export_sql).result()


if __name__ == "__main__":
    bucket_id = 'bq_export_idc'

    dataset_name = settings.BQ_PDP_DATASET
    clinical_dataset_name = f'{dataset_name}_clinical'

    # Get both clinical and non clinical table names with just dataset name
    tables_dict = get_tables(dataset_name)
    regular_tables_list = tables_dict['regular_tables']
    clinical_tables_list = tables_dict['clinical_tables']

    for table in regular_tables_list:
        export_data_from_bigquery(bucket_id, dataset_name, table, overwrite=True)
    for table in clinical_tables_list:
        export_data_from_bigquery(bucket_id, clinical_dataset_name, table, overwrite=True)

    dataset_name = 'idc_current'
    clinical_dataset_name = f'{dataset_name}_clinical'

    # Get both clinical and non clinical table names with just dataset name
    tables_dict = get_tables(dataset_name)
    regular_tables_list = tables_dict['regular_tables']
    clinical_tables_list = tables_dict['clinical_tables']

    for table in regular_tables_list:
        export_data_from_bigquery(bucket_id, dataset_name, table, overwrite=True)
    for table in clinical_tables_list:
        export_data_from_bigquery(bucket_id, clinical_dataset_name, table, overwrite=True)
