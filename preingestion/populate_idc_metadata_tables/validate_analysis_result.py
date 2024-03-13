#
# Copyright 2015-2024, Institute for Systems Biology
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

# Validate that the IDC DB contains the instances of some analysis result

from idc.models import Base, IDC_Collection, IDC_Patient, IDC_Study, IDC_Series, IDC_Instance, Collection, Patient
from utilities.logging_config import successlogger, errlogger, progresslogger
from python_settings import settings

from sqlalchemy.orm import Session
from sqlalchemy import create_engine, update
from google.cloud import storage

def validate_analysis_result(args):
    client = storage.Client()
    src_bucket = storage.Bucket(client, args.src_bucket)

    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{settings.CLOUD_DATABASE}'
    # sql_engine = create_engine(sql_uri, echo=True)
    sql_engine = create_engine(sql_uri)

    with Session(sql_engine) as sess:
        client = storage.Client()

        # Generate a set of the URLs of expected blobs
        expected_blobs = set()
        iterator = client.list_blobs(src_bucket, prefix=args.subdir)
        for page in iterator.pages:
            if page.num_items:
                for blob in page:
                    if not blob.name.endswith(('DICOMDIR','.zip','.csv')):
                        expected_blobs |= {f'gs://{args.src_bucket}/{blob.name}'}

        # # Generate a set of the URLs of blobs in the DB
        # found_blobs = set()
        # # collection = sess.query(IDC_Collection).filter(IDC_Collection.collection_id == args.collection_id).first()
        # seriess = sess.query(IDC_Series).filter(IDC_Series.source_url == args.source_url).all()
        # for series in seriess:
        #     for instance in series.instances:
        #         found_blobs |= {instance.gcs_url}

        found_blobs = sess.query(IDC_Series, IDC_Instance.gcs_url).join(IDC_Instance.seriess).filter(
            IDC_Series.source_url == args.source_url).all()
        found_blobs = set([row['gcs_url'] for row in found_blobs])

        if expected_blobs != found_blobs:
            if expected_blobs - found_blobs:
                errlogger.error('The following blobs are in the bucket but not in the DB:')
                for blob in expected_blobs - found_blobs:
                    errlogger.error(blob)
            if found_blobs - expected_blobs:
                errlogger.error('The following blobs are in the DB but not in the bucket:')
                for blob in found_blobs - expected_blobs:
                    errlogger.error(blob)
            return -1
        else:
            successlogger.info('All blobs in the bucket are in the DB')

    return 0


if __name__ == '__main__':
    import argparse
    import sys

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--version', default=settings.CURRENT_VERSION)
    parser.add_argument('--src_bucket', default='aimi-annotations', help='Bucket containing WSI instances')
    parser.add_argument('--subdir', default='', help="Subdirectory of mount_point at which to start walking directory")
    parser.add_argument('--source_url', default='https://doi.org/10.5281/zenodo.10081112',\
                        help='Info page URL')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    args.client=storage.Client()

    validate_analysis_result(args)


