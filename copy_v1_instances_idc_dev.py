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

# IDC V1 instances are in per-collection buckets with names like
#       idc-tcia-1-<collection_id>/dicom/<StudyInstanceUID>/<SeriesInstanceUID>/<SOPInstanceUID>.dcm
# Copy these to the idc_dev bucket as:
#       idc_dev/<instance_uuid>.dcm
# For this purpose we traverse Version.idc_version_number==1 hierarchy because it only has IDC V1 instances


import sys
import os
import argparse
import logging
from logging import INFO
from google.cloud import storage
from idc.models import Version, Collection, Patient, Study, Series, Instance, sql_engine
from sqlalchemy import select
from sqlalchemy.orm import Session
from subprocess import run, PIPE

MAX_RETRIES = 8

def copy_instances_alt(args):
    client = storage.Client(project=args.project)
    dst_bucket = client.bucket(args.dst_bucket)
    copied = open(args.copied_alt).read().splitlines()
    with Session(sql_engine) as sess:
        stmt = select(Version).distinct()
        result = sess.execute(stmt)
        version = []
        for row in result:
            if row[0].idc_version_number == args.vnext:
                # We've at least started working on vnext
                version = row[0]
                break

        if not version:
            errlogger('Version %s no found', args.vnext)
            exit

        for collection in version.collections:
            idc_collection_id = collection.tcia_api_collection_id.lower()
            if not collection.tcia_api_collection_id in copied:
                rootlogger.info('Copying collection %s',idc_collection_id)
                src_bucket = client.bucket(f'{args.src_bucket_prefix}{idc_collection_id}')
                # result = run(["gsutil", "-m", "-q", "cp", "-r", f'gs://{args.src_bucket_prefix}{idc_collection_id}/*', f'gs://{args.dst_bucket}'])
                # if result.returncode < 0:
                #     errlogger.error('p%s: \tcopy_disk_to_prestaging_bucket failed for series %s', args.id,
                #                     series.series_instance_uid)
                #     raise RuntimeError('p%s: copy_disk_to_prestaging_bucket failed for series %s', args.id,
                #                        series.series_instance_uid)
                # with open(args.copied_alt, 'a') as f:
                #     f.write(f'{collection.tcia_api_collection_id}\n')


                for patient in collection.patients:
                    submitter_case_id = patient.submitter_case_id
                    if not submitter_case_id in copied:
                        rootlogger.info('  For patient %s', submitter_case_id)
                        for study in patient.studies:
                            study_instance_uid = study.study_instance_uid
                            if not study_instance_uid in copied:
                                rootlogger.info('    For study %s', study_instance_uid)
                                for series in study.seriess:
                                    series_instance_uid = series.series_instance_uid
                                    rootlogger.info('      For series %s', series_instance_uid)
                                    for instance in series.instances:
                                        sop_instance_uid = instance.sop_instance_uid
                                        instance_uuid = instance.instance_uuid
                                        blob = src_bucket.blob(f'dicom/{study_instance_uid}/{series_instance_uid}/{sop_instance_uid}.dcm')
                                        # blob = dst_bucket.blob(f'dicom/{study_instance_uid}/{series_instance_uid}/{sop_instance_uid}.dcm')
                                        attempt = 0
                                        while True:
                                            try:
                                                new_blob = src_bucket.copy_blob(blob, dst_bucket, new_name=instance_uuid)
                                                break
                                            except Exception as exc:
                                                errlogger.error('Copy failure %s on %s/dicom/%s/%s/%s to %s/%s',
                                                    idc_collection_id, study_instance_uid, series_instance_uid, sop_instance_uid,
                                                    args.dst_bucket, instance_uuid)
                                                errlogger.error(exc)
                                                if attempt == MAX_RETRIES:
                                                    raise exc
                                                attempt += 1

                                        # try:
                                        #     new_blob = dst_bucket.rename_blob(blob, f'{instance_uuid}.dcm')
                                        #     rootlogger.debug('        Renamed %s to %s',
                                        #                     f'dicom/{study_instance_uid}/{series_instance_uid}/{sop_instance_uid}.dcm',
                                        #                     instance_uuid)
                                        # except Exception as exc:
                                        #     # Determine if we have already renamed this instance
                                        #     if not storage.blob.Blob(f'{instance_uuid}.dcm', dst_bucket).exists:
                                        #         # No
                                        #         errlogger.error('Failed to rename instance from %s to %s',f'dicom/{study_instance_uid}/{series_instance_uid}/{sop_instance_uid}.dcm', f'{instance_uuid}.dcm')
                                with open(args.copied_alt, 'a') as f:
                                    f.write(f'{study_instance_uid}\n')
                                rootlogger.info('      Renamed instances in study %s', study_instance_uid)
                            else:
                                rootlogger.info('      Renaming instances in study %s previously done', study_instance_uid)
                        with open(args.copied_alt, 'a') as f:
                            f.write(f'{submitter_case_id}\n')
                        rootlogger.info('  Renamed instances in patient %s', submitter_case_id)
                    else:
                        rootlogger.info('  Renaming instances in patient %s previously done', submitter_case_id)
                with open(args.copied_alt, 'a') as f:
                    f.write(f'{collection.tcia_api_collection_id}\n')
                rootlogger.info('  Renamed instances in collection %s', collection.tcia_api_collection_id)
            else:
                rootlogger.info('Renaming instances in collection %s previously done', collection.tcia_api_collection_id)


def copy_instances(args):
    client = storage.Client(project=args.project)
    dst_bucket = client.bucket(args.dst_bucket)
    copied = open(args.copied).read().splitlines()
    with Session(sql_engine) as sess:
        stmt = select(Version).distinct()
        result = sess.execute(stmt)
        version = []
        for row in result:
            if row[0].idc_version_number == args.vnext:
                # We've at least started working on vnext
                version = row[0]
                break

        if not version:
            errlogger('Version %s no found', args.vnext)
            exit

        for collection in version.collections:
            idc_collection_id = collection.tcia_api_collection_id.lower()
            if not collection.tcia_api_collection_id in copied:
                rootlogger.info('Copying collection %s',idc_collection_id)
                client.bucket(f'{args.src_bucket_prefix}{idc_collection_id}')
                result = run(["gsutil", "-m", "-q", "cp", "-r", f'gs://{args.src_bucket_prefix}{idc_collection_id}/*', f'gs://{args.dst_bucket}'])
                if result.returncode < 0:
                    errlogger.error('p%s: \tcopy_disk_to_prestaging_bucket failed for series %s', args.id,
                                    series.series_instance_uid)
                    raise RuntimeError('p%s: copy_disk_to_prestaging_bucket failed for series %s', args.id,
                                       series.series_instance_uid)
                with open(args.copied, 'a') as f:
                    f.write(f'{collection.tcia_api_collection_id}\n')


            for patient in collection.patients:
                if not patient.submitter_case_id in copied:
                    submitter_case_id = patient.submitter_case_id
                    rootlogger.info('  For patient %s', submitter_case_id)
                    for study in patient.studies:
                        study_instance_uid = study.study_instance_uid
                        rootlogger.info('    For study %s', study_instance_uid)
                        for series in study.seriess:
                            series_instance_uid = series.series_instance_uid
                            rootlogger.info('      For series %s', series_instance_uid)
                            for instance in series.instances:
                                sop_instance_uid = instance.sop_instance_uid
                                instance_uuid = instance.instance_uuid
                                # blob = src_bucket.blob(f'dicom/{study_instance_uid}/{series_instance_uid}/{sop_instance_uid}.dcm')
                                blob = dst_bucket.blob(f'dicom/{study_instance_uid}/{series_instance_uid}/{sop_instance_uid}.dcm')
                                # new_blob = src_bucket.copy_blob(blob, dst_bucket, new_name=instance_uuid)
                                try:
                                    new_blob = dst_bucket.rename_blob(blob, f'{instance_uuid}.dcm')
                                    rootlogger.debug('        Renamed %s to %s',
                                                    f'dicom/{study_instance_uid}/{series_instance_uid}/{sop_instance_uid}.dcm',
                                                    instance_uuid)
                                except Exception as exc:
                                    # Determine if we have already renamed this instance
                                    if not storage.blob.Blob(f'{instance_uuid}.dcm', dst_bucket).exists:
                                        # No
                                        errlogger.error('Failed to rename instance from %s to %s',f'dicom/{study_instance_uid}/{series_instance_uid}/{sop_instance_uid}.dcm', f'{instance_uuid}.dcm')

                    with open(args.copied, 'a') as f:
                        f.write(f'{patient.submitter_case_id}\n')
                    rootlogger.info('  Renamed instances in patient %s', patient.submitter_case_id)
                else:
                    rootlogger.info('  Renaming instances in patient %s previously done', patient.submitter_case_id)


if __name__ == '__main__':
    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/copy_instances_log.log'.format(os.environ['PWD']))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/copy_instances_err.log'.format(os.environ['PWD']))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    parser = argparse.ArgumentParser()
    parser.add_argument('--vnext', default=1, help='Next version to generate')
    parser.add_argument('--src_bucket_prefix', default='idc-tcia-1-', help='Bucket in which to save instances')
    parser.add_argument('--dst_bucket', default='idc_dev', help='Bucket in which to save instances')
    parser.add_argument('--num_processes', default=8, help="Number of concurrent processes")
    parser.add_argument('--project', default='idc-dev-etl')
    parser.add_argument('--copied', default='./logs/copied_collections' )
    parser.add_argument('--copied_alt', default='./logs/copied_collections_alt' )
    args = parser.parse_args()

    # Directory to which to download files from TCIA/NBIA
    args.dicom = 'dicom'
    print("{}".format(args), file=sys.stdout)

    copy_instances_alt(args)
