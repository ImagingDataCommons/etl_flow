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

# Adds/replaces data to the idc_collection/_patient/_study/_series/_instance DB tables
# from a specified bucket.
#
# For this purpose, the bucket containing the instance blobs is gcsfuse mounted, and
# pydicom is then used to extract needed metadata.
#
# The script walks the directory hierarchy from a specified subdirectory of the
# gcsfuse mount point

import sys
import argparse
import pathlib
import subprocess

from python_settings import settings
from preingestion.preingestion_code.populate_idc_metadata_tables_from_gcsfuse import prebuild_from_gcsfuse

from idc.models import IDC_Collection, IDC_Patient, IDC_Study, IDC_Series
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from google.cloud import storage


def get_collection_metadata():
    client = storage.Client()
    src_bucket = storage.Bucket(client, args.src_bucket)

    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{settings.CLOUD_DATABASE}'
    # sql_engine = create_engine(sql_uri, echo=True)
    sql_engine = create_engine(sql_uri)

    with Session(sql_engine) as sess:
        collection_metadata = sess.query(IDC_Collection.collection_id, IDC_Series.source_doi, IDC_Series.source_url, \
                                        IDC_Series.license_url, IDC_Series.license_long_name, IDC_Series.license_short_name).\
                                        distinct().join(IDC_Collection.patients).join(IDC_Patient.studies).join(IDC_Study.seriess).all()
        return {row['collection_id']: row._asdict() for row in collection_metadata}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--version', default=settings.CURRENT_VERSION)
    parser.add_argument('--src_bucket', default='idc-conversion-outputs-tcga-manufacturer-fixed', help='Source bucket containing instances')
    parser.add_argument('--mount_point', default='/mnt/disks/idc-etl/preeingestion_gcsfuse_mount_point', help='Directory on which to mount the bucket.\
                The script will create this directory if necessary.')
    # parser.add_argument('--subdir', \
    #         default='TiffAddedAgeFixed/20BR002 [20BR002]', \
    #         help="Subdirectory of mount_point at which to start walking directory")
    parser.add_argument('--startswith', \
            default=[],\
            help='Only include files whose name startswith a string in the list. If the list is empty, include all')
    parser.add_argument('--subset_of_db_expected', default=True, help='If True, validation will not report an error if the instances in the bucket are a subset of the instance in the DB')
    # parser.add_argument('--collection_id', default='CPTAC-BRCA', help='idc_webapp_collection id of the collection or ID of analysis result to which instances belong.')
    # parser.add_argument('--source_doi', default='10.7937/tcia.caem-ys80', help='Collection DOI. Might be empty string.')
    # parser.add_argument('--source_url', default='https://doi.org/10.7937/tcia.caem-ys80',\
    #                     help='Info page URL')
    # parser.add_argument('--license', default = {"license_url": 'https://creativecommons.org/licenses/by/3.0/',\
    #         "license_long_name": "Creative Commons Attribution 3.0 Unported License", \
    #         "license_short_name": "CC BY 3.0"}, help="(Sub-)Collection license")
    parser.add_argument('--third_party', type=bool, default=False, help='True if from a third party analysis result')
    parser.add_argument('--gen_hashes', default=True, help=' Generate hierarchical hashes of collection if True.')
    parser.add_argument('--validate', type=bool, default=True, help='True if validation is to be performed')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    args.client=storage.Client()

    # Get metadata for each collections. We updating collections, but the doi and license info are already in the DB
    # and are still valid
    all_collection_metadata = get_collection_metadata()

    # Now get a list of the actual TCGA collections that are being revised. This bucket has a subfolder for each such collection.
    client = storage.Client()

    blobs = client.list_blobs(args.src_bucket,prefix="TCGA/", delimiter="/")
    # We have to iterate over blobs in order that it populate the prefixes item, which is really stupid
    for blob in blobs: pass
    collection_folders = list(blobs.prefixes)
    collection_folders.sort()
    for folder in collection_folders:
        args.collection_id = folder.split('/')[1]
        metadata = all_collection_metadata[args.collection_id]
        args.subdir = folder
        args.source_doi = metadata['source_doi']
        args.source_url = metadata['source_url']
        args.license = {
            'license_url': metadata['license_url'],
            'license_long_name': metadata['license_long_name'],
            'license_short_name': metadata['license_short_name']
        }

        try:
            # gcsfuse mount the bucket
            pathlib.Path(args.mount_point).mkdir( exist_ok=True)
            subprocess.run(['gcsfuse', '--implicit-dirs', args.src_bucket, args.mount_point])
            prebuild_from_gcsfuse(args)
        finally:
            # Always unmount
            subprocess.run(['fusermount', '-u', args.mount_point])

