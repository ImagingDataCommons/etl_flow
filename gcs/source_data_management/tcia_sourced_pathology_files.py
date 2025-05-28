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

# Generate a dataframe of pathology files in idc-source-data source from TCIA
from google.cloud import bigquery, storage
from base64 import b64decode
import pandas as pd

from utilities.logging_config import progresslogger, errlogger

def tcia_sourced_pathology_files():
    # Skip buckets that don't have TCIA sourced pathology
    skipped_buckets = ['aimi-annotations', 'gevaert-cell-subtyping', 'htan-source-data', 'idc-lps-sardana', 'idc-rms-manual-region-annotations-xml', \
                     'idc-source-data-cmb-20240828', 'idc-source-data-bmdeep', 'idc-source-data-gtex', 'idc-source-data-pdxnet', \
                     'idc-source-data-rms', 'nlm_visible_human', 'til-wsi-tcga', 'til-wsi-tcga-nature-new-results', \
                     'idc-source-data-mci', 'tcia-nondicom-parked', 'tcga_pathology_source_data']

    storage_client = storage.Client(project='idc-source-data')
    buckets = storage_client.list_buckets()

    sources = []
    for bucket in buckets:
        if bucket.name in skipped_buckets:
            progresslogger.debug(f'Skipped bucket {bucket.name}')
        else:
            progresslogger.debug(f'Adding bucket {bucket.name}')
            blobs = bucket.list_blobs()
            for blob in blobs:
                try:
                    md5_hash = b64decode(blob.md5_hash).hex()
                except:
                    # A blob might not have an md5 hash
                    md5_hash = ""
                sources.append({"name": f"{bucket.name}/{blob.name}",
                               "md5_hash": md5_hash,
                               "created": blob.time_created,
                               "updated": blob.updated})

    df = pd.DataFrame(sources)

    return df

