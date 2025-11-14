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

# This routine returns a farm fingerprint hash of a table

from google.cloud import bigquery,storage

def get_table_hash(table):

    client = bigquery.Client()
    query = f"""
    WITH selected AS (
        SELECT * 
        FROM `{table}`
    )
    SELECT BIT_XOR(DISTINCT FARM_FINGERPRINT(TO_JSON_STRING(t))) as table_hash
    FROM selected  AS t
    """

    table_hash =  [dict(row) for row in client.query(query)][0]['table_hash']
    return table_hash
