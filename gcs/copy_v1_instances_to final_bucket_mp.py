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
# This is intended as a one-time operation. Note that V1 collections are unchanged for V2, so this covers V2 for
# those collections

import sys
import os
import argparse
import logging
from time import time
from datetime import timedelta
from logging import INFO
from google.cloud import storage
from idc.models import Version, Collection, Patient, Study, Series, Instance, sql_engine
from sqlalchemy import select
from sqlalchemy.orm import Session
from subprocess import run, PIPE
from multiprocessing import Process, Queue
from queue import Empty

PATIENT_TRIES=3

def copy_patient(args, src_bucket, dst_bucket, patient, copied):
    submitter_case_id = patient.submitter_case_id
    if not submitter_case_id in copied:

        rootlogger.info('  p%s: For patient %s', args.id, submitter_case_id)
        for study in patient.studies:
            study_instance_uid = study.study_instance_uid
            if not study_instance_uid in copied:
                rootlogger.info('    p%s: For study %s', args.id, study_instance_uid)
                for series in study.seriess:
                    series_instance_uid = series.series_instance_uid
                    rootlogger.info('      p%s: For series %s', args.id, series_instance_uid)
                    for instance in series.instances:
                        sop_instance_uid = instance.sop_instance_uid
                        instance_uuid = instance.instance_uuid
                        blob = src_bucket.blob(
                            f'dicom/{study_instance_uid}/{series_instance_uid}/{sop_instance_uid}.dcm')
                        # blob = dst_bucket.blob(f'dicom/{study_instance_uid}/{series_instance_uid}/{sop_instance_uid}.dcm')
                        new_blob = src_bucket.copy_blob(blob, dst_bucket, new_name=f'{instance_uuid}.dcm')
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
                # with open(args.copied_alt, 'a') as f:
                #     f.write(f'{study_instance_uid}\n')
                donelogger.info('%s', study_instance_uid)
                rootlogger.info('      p%s: Renamed instances in study %s', args.id, study_instance_uid)
            else:
                rootlogger.info('      p%s: Renaming instances in study %s previously done', args.id, study_instance_uid)
        # with open(args.copied_alt, 'a') as f:
        #     f.write(f'{submitter_case_id}\n')
        donelogger.info('%s', submitter_case_id)
        rootlogger.info('  p%s: Renamed instances in patient %s', args.id, submitter_case_id)
    else:
        rootlogger.info('  p%s:Renaming instances in patient %s previously done', args.id, submitter_case_id)


def worker(input, output, args):
    rootlogger.debug('p%s: Worker starting: args: %s', args.id, args)
    client = storage.Client(project=args.project)
    dst_bucket = client.bucket(args.dst_bucket)
    for more_args in iter(input.get, 'STOP'):
        with Session(sql_engine) as sess:
            for attempt in range(PATIENT_TRIES):
                try:
                    copied, idc_version_number, tcia_api_collection_id, submitter_case_id = more_args
                    version = sess.query(Version).filter_by(idc_version_number=idc_version_number).one()
                    collection = next(collection for collection in version.collections if collection.tcia_api_collection_id==tcia_api_collection_id)
                    patient = next(patient for patient in collection.patients if patient.submitter_case_id==submitter_case_id)
                    idc_collection_id = collection.tcia_api_collection_id.lower()
                    src_bucket = client.bucket(f'{args.src_bucket_prefix}{idc_collection_id}')
                    rootlogger.debug("p%s: In worker, sess: %s, submitter_case_id: %s", args.id, sess, submitter_case_id)
                    copy_patient(args, src_bucket, dst_bucket, patient, copied)
                    break
                except Exception as exc:
                    errlogger.error("Worker p%s, exception %s; reattempt %s on patient %s", args.id, exc, attempt, submitter_case_id)
                    sess.rollback()

            if attempt == PATIENT_TRIES:
                errlogger.error("p%s, Failed to process patient: %s", args.id, submitter_case_id)
                sess.rollback()
            output.put(submitter_case_id)


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
                begin = time()
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

                if args.num_processes == 0:
                    # for series in sorted_seriess:
                    begin = time()
                    for patient in collection.patients:
                        args.id = 0
                        patient_index = f'{collection.patients.index(patient) + 1} of {len(collection.patients)}'
                        copy_patient(args, src_bucket, copied, dst_bucket, version, collection, patient)
                        # build_patient(sess, args, patient_index, data_collection_doi, analysis_collection_dois, version,
                        #               collection, patient)
                        duration = str(timedelta(seconds=(time() - begin)))
                        rootlogger.info("Collection %s, %s, completed in %s", collection.tcia_api_collection_id,
                                        duration)
                        donelogger.info('%s', collection.tcia_api_collection_id)

                else:
                    processes = []
                    # Create queues
                    task_queue = Queue()
                    done_queue = Queue()

                    # List of patients enqueued
                    enqueued_patients = []

                    # Start worker processes
                    for process in range(args.num_processes):
                        args.id = process + 1
                        processes.append(
                            Process(target=worker,
                                    args=(task_queue, done_queue, args)))
                        processes[-1].start()

                    # Enqueue each patient in the the task queue
                    for patient in collection.patients:
                        # patient_index = f'{collection.patients.index(patient) + 1} of {len(collection.patients)}'
                        task_queue.put((copied, version.idc_version_number, collection.tcia_api_collection_id,
                                        patient.submitter_case_id))
                        enqueued_patients.append(patient.submitter_case_id)

                    # Collect the results for each patient
                    try:
                        while not enqueued_patients == []:
                            # Timeout if waiting too long
                            results = done_queue.get(True, 90 * 60)
                            enqueued_patients.remove(results)

                        # Tell child processes to stop
                        for process in processes:
                            task_queue.put('STOP')

                        collection.collection_timestamp = min(
                            [patient.patient_timestamp for patient in collection.patients])
                        # copy_prestaging_to_staging_bucket(args, collection)
                        collection.done = True
                        # ************ Temporary code during development********************
                        # duration = str(timedelta(seconds=(time.time() - begin)))
                        # rootlogger.info("Collection %s, %s, completed in %s", collection.tcia_api_collection_id, collection_index, duration)
                        # raise
                        # ************ End temporary code ********************
                        sess.commit()
                        duration = str(timedelta(seconds=(time() - begin)))
                        rootlogger.info("Collection %s, completed in %s", collection.tcia_api_collection_id,
                                        duration)
                        donelogger.info('%s', collection.tcia_api_collection_id)

                    except Empty as e:
                        errlogger.error("Timeout in build_collection %s", collection.tcia_api_collection_id)
                        for process in processes:
                            process.terminate()
                            process.join()
                        sess.rollback()
                        duration = str(timedelta(seconds=(time() - begin)))
                        rootlogger.info("Collection %s, %s, NOT completed in %s", collection.tcia_api_collection_id,
                                        duration)


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

    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/copy_instances_log.log'.format(os.environ['PWD']))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    donelogger = logging.getLogger('done')
    done_fh = logging.FileHandler(args.copied_alt)
    doneformatter = logging.Formatter('%(message)s')
    donelogger.addHandler(done_fh)
    done_fh.setFormatter(doneformatter)
    donelogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/copy_instances_err.log'.format(os.environ['PWD']))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)


    copy_instances_alt(args)
