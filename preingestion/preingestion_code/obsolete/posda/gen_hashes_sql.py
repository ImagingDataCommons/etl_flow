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

import sys
import argparse
from python_settings import settings
from google.cloud import bigquery

def gen_hashes(src_table = 'preingestion_snapshot', dst_table = 'preingestion_snapshot' ):
    client = bigquery.Client()
    query = f"""
BEGIN
CREATE TEMP TABLE all_data
AS (
  SELECT *
  FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.{src_table}`
);

CREATE OR REPLACE TEMP TABLE all_data AS (
  WITH se_hashes as (
    SELECT DISTINCT
      SeriesInstanceUID,
      TO_HEX(MD5(STRING_AGG(DISTINCT i_hash, '' ORDER BY i_hash))) se_hash
    FROM all_data
    GROUP BY SeriesInstanceUID
  )
  SELECT DISTINCT 
    all_data.* except(se_hash), se_hashes.se_hash
    FROM all_data
    JOIN se_hashes
    ON all_data.SeriesInstanceUID = se_hashes.SeriesInstanceUID
);

CREATE OR REPLACE TEMP TABLE all_data AS (
  WITH st_hashes as (
    SELECT DISTINCT
      StudyInstanceUID,
      TO_HEX(MD5(STRING_AGG(DISTINCT se_hash, '' ORDER BY se_hash))) st_hash
    FROM all_data
    GROUP BY StudyInstanceUID
  )
  SELECT DISTINCT 
    all_data.* except(st_hash), st_hashes.st_hash
    FROM all_data
    JOIN st_hashes
    ON all_data.StudyInstanceUID = st_hashes.StudyInstanceUID
);

CREATE OR REPLACE TEMP TABLE all_data AS (
  WITH p_hashes as (
    SELECT DISTINCT
      collection_name,
      patientID,
      TO_HEX(MD5(STRING_AGG(DISTINCT st_hash, '' ORDER BY st_hash))) p_hash
    FROM all_data
    GROUP BY collection_name, patientID
  )
  SELECT DISTINCT 
    all_data.* except(p_hash), p_hashes.p_hash
    FROM all_data
    JOIN p_hashes
    ON all_data.collection_name = p_hashes.collection_name AND all_data.patientID = p_hashes.patientID
);

CREATE OR REPLACE TEMP TABLE all_data AS (
  WITH c_hashes as (
    SELECT DISTINCT
      collection_name,
      TO_HEX(MD5(STRING_AGG(DISTINCT p_hash, '' ORDER BY p_hash))) c_hash
    FROM all_data
    GROUP BY collection_name
  )
  SELECT DISTINCT 
    all_data.* except(c_hash), c_hashes.c_hash
    FROM all_data
    JOIN c_hashes
    ON all_data.collection_name = c_hashes.collection_name
);

CREATE OR REPLACE TABLE `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.{dst_table}` AS
SELECT * 
FROM all_data
;

END;
"""

    query_job = client.query(query)
    query_job.result()
    return




if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--version', default=settings.CURRENT_VERSION)

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)

    gen_hashes(src_table='all_data_snapshot', dst_table = 'all_data_snapshot')
