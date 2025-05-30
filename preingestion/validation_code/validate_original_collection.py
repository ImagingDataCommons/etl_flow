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
import settings
# Validate that an original collection in the IDC DB contains the expected instances

from idc.models import Base, IDC_Collection, IDC_Patient, IDC_Study, IDC_Series, IDC_Instance, Collection, Patient
from utilities.logging_config import successlogger, errlogger, progresslogger
from utilities.sqlalchemy_helpers import sa_session
from google.cloud import storage

def validate_original_collection(args, collection_ids, ignore_if_contains=''):
    client = storage.Client()
    src_bucket = storage.Bucket(client, args.src_bucket)
    with sa_session(echo=False) as sess:
        client = storage.Client()

        # Generate a set of the URLs of blobs in the source bucket
        blobs_in_bucket = set()
        iterator = client.list_blobs(src_bucket, prefix=args.subdir)
        for page in iterator.pages:
            if page.num_items:
                for blob in page:
                    if not blob.name.endswith(('DICOMDIR', '.txt', 'csv')) and blob.size != 0 and \
                            ('exclusion_filter' not in args or (args.exclusion_filter == '' or args.exclusion_filter not in blob.name)):
                        blobs_in_bucket |= {f'gs://{args.src_bucket}/{blob.name}'}

        # Generate a set of the URLs of blobs added to the DB
        blobs_in_db = set()
        for collection_id in collection_ids:
            collection_metadata = sess.query(IDC_Collection.collection_id, IDC_Instance.ingestion_url). \
                distinct().join(IDC_Collection.patients).join(IDC_Patient.studies).join(IDC_Study.seriess).join(
                IDC_Series.instances). \
                filter(IDC_Collection.collection_id == collection_id). \
                filter(IDC_Instance.idc_version == args.version). \
                filter(IDC_Series.ingestion_script == settings.BASE_NAME). \
                all()
            blobs_in_db |= set(row.ingestion_url for row in collection_metadata)

        if blobs_in_bucket != blobs_in_db:
            if blobs_in_bucket - blobs_in_db:
                errlogger.error('The following blobs are in the bucket but not in the DB:')
                for blob in blobs_in_bucket - blobs_in_db:
                    errlogger.error(blob)
                return -1
            if blobs_in_db - blobs_in_bucket:
                if "subset_of_db_expected_in_bucket" in args and args.subset_of_db_expected_in_bucket:
                    progresslogger.info('Ignoring blobs in the DB but not in the bucket')
                    return 0
                else:
                    errlogger.error('The following blobs are in the DB but not in the bucket:')
                    for blob in blobs_in_db - blobs_in_bucket:
                        errlogger.error(blob)
                    return -1

        else:
            successlogger.info('All blobs in the bucket are in the DB')

        return 0


# if __name__ == '__main__':
#     import argparse
#     import sys
#     parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
#     parser.add_argument('--version', default=settings.CURRENT_VERSION)
#     parser.add_argument('--src_bucket', default='idc-conversion-outputs-rms', help='Bucket containing WSI instances')
#     parser.add_argument('--subdir', default='', help="Subdirectory of mount_point at which to start walking directory")
#     parser.add_argument('--collection_id', default='RMS-Mutation-Prediction', help='idc_webapp_collection id of the collection or ID of analysis result to which instances belong.')
#     args = parser.parse_args()
#     print("{}".format(args), file=sys.stdout)
#     args.client=storage.Client()
#
#     validate_original_collection(args)


