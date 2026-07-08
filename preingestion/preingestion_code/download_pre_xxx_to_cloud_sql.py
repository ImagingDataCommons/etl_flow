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
import argparse
import settings
from bq.cloud_sql_bq_IO.download_bq_to_psql import main

if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    for params in [
        # {
        #     "table_name": 'pre_collection',
        #     "primary_key_constraint": ', CONSTRAINT pre_collection_pkey PRIMARY KEY (collection_name)',
        #     "foreign_key_constraint": ""
        # },
        # {
        #     "table_name": 'pre_patient',
        #     "primary_key_constraint": \
        #             ', CONSTRAINT pre_patient_pkey PRIMARY KEY (collection_name, patientID)',
        #     "foreign_key_constraint": \
        #             ', CONSTRAINT pre_patient_collection_fkey FOREIGN KEY (collection_name) REFERENCES pre_collection(collection_name) ON DELETE CASCADE'
        # },
        # {
        #     "table_name": 'pre_study',
        #     "primary_key_constraint": \
        #             ', CONSTRAINT pre_study_pkey PRIMARY KEY (studyinstanceuid)',
        #     "foreign_key_constraint": \
        #             ', CONSTRAINT pre_study_patient_fkey FOREIGN KEY (collection_name, patientID) REFERENCES pre_patient(collection_name, patientID) ON DELETE CASCADE'
        # },
        # {
        #     "table_name": 'pre_series',
        #     "primary_key_constraint": \
        #             ', CONSTRAINT pre_series_pkey PRIMARY KEY (seriesinstanceuid)',
        #     "foreign_key_constraint": \
        #             ', CONSTRAINT pre_series_study_fkey FOREIGN KEY (StudyInstanceUID) REFERENCES pre_study(StudyInstanceUID) ON DELETE CASCADE'
        # },
        {
            "table_name": 'pre_instance',
            "primary_key_constraint": \
                    ', CONSTRAINT pre_instance_pkey PRIMARY KEY (SOPInstanceUID)',
            "foreign_key_constraint": \
                    ', CONSTRAINT pre_instance_series_fkey FOREIGN KEY (SeriesInstanceUID) REFERENCES pre_series(SeriesInstanceUID) ON DELETE CASCADE'
        },
                ]:
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        # Table to copy from
        parser.add_argument("--bq_project_id", default="idc-dev-etl")
        parser.add_argument("--bq_dataset_id", default=f"idc_v{settings.CURRENT_VERSION}_dev")
        parser.add_argument("--bq_table_id", default=params['table_name'])

        # Cloud SQL table to copy to
        parser.add_argument("--pg_database", default=f"idc_v{settings.CURRENT_VERSION}")
        parser.add_argument("--pg_table_name", default=params['table_name'])

        args = parser.parse_args()
        print("{}".format(args), file=sys.stdout)

        main(args, params['primary_key_constraint'], params['foreign_key_constraint'])