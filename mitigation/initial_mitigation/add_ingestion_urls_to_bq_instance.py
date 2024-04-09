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

VERSION = 4


def get_source_urls(version):

    if version <= 6:
        query = f"""
            SELECT sop_instance_uid, gcs_url
            FROM `{settings.DEV_MITIGATION_PROJECT}.idc_v{version}_dev.wsi_metadata`
        """
        results =bigquery.Client().query(query)
        source_urls = {row.sop_instance_uid: row.gcs_url for row in results}

def add_source_urls(version, source_urls):
    pass

def add_urls(version):
    for v in range(3, version+1):
        source_urls = get_source_urls(v)
        add_source_urls(v, source_urls)

if __name__ == "__main__":
    add_urls(VERSION)
