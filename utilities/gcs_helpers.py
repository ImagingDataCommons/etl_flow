#!/usr/bin/env
#
# Copyright 2020, Institute for Systems Biology
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

import argparse
import sys
import os
import time
from google.cloud import storage
from google.cloud.exceptions import NotFound

# Return the name string through the StudyInstanceUID (dicom/<StudyInstanceUID>/) each blob in a bucket
def get_studies(storage_client, bucket_name, prefix='dicom/'):
    studies = []
    iterator = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter='/')
    for page in iterator.pages:
        studies.extend(page.prefixes)
    return studies

# Return the name string through the SeriesInstanceUID (dicom/<StudyInstanceUID>/<SeriesInstanceUID/) each blob in a bucket
def get_series(storage_client, bucket_name):
    studies = get_studies(storage_client, bucket_name)
    series = []

    for study in studies:
        # First get studies; really the prefixes of blobs to the study level.
        iterator = storage_client.list_blobs(bucket_name, prefix=study, delimiter='/')
        for page in iterator.pages:
            series.extend(page.prefixes)
    return series

def list_buckets(project):
    """Lists all buckets."""
    storage_client = storage.Client(project=project)
    buckets = storage_client.list_buckets()
    return buckets


def list_blobs(project):
    """Lists all buckets."""
    series = set()
    blobs = set()
    storage_client = storage.Client(project=project)
    iterator = storage_client.list_blobs('idc-dev-defaced', prefix='', delimiter='/')
    # iterator = storage_client.list_blobs('idc-dev-defaced', delimiter='/')
    for page in iterator.pages:
        if page.num_items:
            # series = series.union(set([series for series in page.prefixes]))
            for prefix in page.prefixes:
                blobs_iterator = storage_client.list_blobs('idc-dev-defaced', prefix=prefix)
                for blob_page in blobs_iterator.pages:
                    blobs = blobs.union(set([f'{prefix}/{blob.name}' for blob in blob_page]))

        # blobs = blobs - dones
        else:
            break
    return series

if __name__ == "__main__":
    blobs = list_blobs('idc-dev-etl')
    pass

