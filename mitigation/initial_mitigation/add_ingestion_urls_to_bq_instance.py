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

# This is a one-time use script that populates the ingestion_url column in the instance table. THe value is ingestion_url
# is the GCS URL of the blob from which the instance was ingested. Only non-TCIA blobs have a non-null value.
# We populate the instance table on each idc_vX_idc dataset. The data for version vX is populated by successively extracting
# url data from the wsi_metadata, wsi_instance, and idc_instance tables of previous and currend idc_versions.
import settings
from google.cloud import bigquery


INSTANCE_VERSION = 3

def clear_ingestion_url_column(client, instance_version):
    query = f"""
        UPDATE `{settings.DEV_MITIGATION_PROJECT}.idc_v{instance_version}_dev.instance` i
        SET i.ingestion_url = ""
        WHERE True
    """
    result = client.query(query)
    while result.state != 'DONE':
        result = client.get_job(result.job_id)
    return

def add_source_urls(client, instance_version, source_urls_version):
    if source_urls_version <= 6:
        source_urls_query = f"""
            SELECT sop_instance_uid, gcs_url url
            FROM `{settings.DEV_MITIGATION_PROJECT}.idc_v{source_urls_version}_dev.wsi_metadata`
        """
        query = f"""
        UPDATE `{settings.DEV_MITIGATION_PROJECT}.idc_v{instance_version}_dev.instance` i
        SET i.ingestion_url = CONCAT('gs://af-dac-wsi-conversion-results/', s_u.url)
        FROM ({source_urls_query}) s_u
        WHERE 
            i.sop_instance_uid = s_u.sop_instance_uid AND
            i.rev_idc_version = {source_urls_version} 
        """
    else:
        if source_urls_version <= 12:
            source_urls_query = f"""
                 SELECT sop_instance_uid, url
                 FROM `{settings.DEV_MITIGATION_PROJECT}.idc_v{source_urls_version}_dev.wsi_instance`
             """
        else:
            source_urls_query = f"""
                 SELECT sop_instance_uid, gcs_url url
                 FROM `{settings.DEV_MITIGATION_PROJECT}.idc_v{source_urls_version}_dev.idc_instance`
             """

        query = f"""
        UPDATE `{settings.DEV_MITIGATION_PROJECT}.idc_v{instance_version}_dev.instance` i
        SET i.ingestion_url = s_u.url
        FROM ({source_urls_query}) s_u
        WHERE 
            i.sop_instance_uid = s_u.sop_instance_uid AND
            i.rev_idc_version = {source_urls_version} 
        """

    # job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    # query_job = client.query(query, job_config=job_config)
    result = query_job = client.query(query)
    while result.state != 'DONE':
        result = client.get_job(result.job_id)

    return

def validate_table(client, instance_version):
    query = f"""
    SELECT COUNT(*) cnt
    FROM `{settings.DEV_MITIGATION_PROJECT}.idc_v{instance_version}_dev.instance`
    WHERE source != 'tcia'
    """
    result = client.query(query)
    while result.state != 'DONE':
        result = client.get_job(result.job_id)
    print(f'Table {instance_version} IDC instances: {[row.cnt for row in result][0]}')

    query = f"""
    SELECT COUNT(*) cnt
    FROM `{settings.DEV_MITIGATION_PROJECT}.idc_v{instance_version}_dev.instance`
    WHERE source != 'tcia' and ingestion_url = ''
    """
    result = client.query(query)
    while result.state != 'DONE':
        result = client.get_job(result.job_id)
    print(f'Table {instance_version} IDC instances without url: {[row.cnt for row in result][0]}')


def add_urls(instance_version):
    client = bigquery.Client()
    clear_ingestion_url_column(client, instance_version)
    for source_urls_version in range(3, instance_version+1):
        add_source_urls(client, instance_version, source_urls_version)

    validate_table(client, instance_version)

if __name__ == "__main__":
    for instance_version in range(4,19):
        add_urls(instance_version)
