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

# tcia_ingest.py did not correctly assign 3rd party source DOIs to series.
# This should be a one-use script

import sys
import os
import argparse
import time
from datetime import datetime, timezone, timedelta
import logging
from logging import INFO

import shutil

from idc.models import Version, Collection, Patient, Study, Series, Instance, sql_engine
from sqlalchemy import select
from sqlalchemy.orm import Session
from utilities.get_collection_dois import get_data_collection_doi, get_analysis_collection_dois


def insert_dois(sess, collection, patient, study, data_collection_doi, analysis_collection_dois):
    # rows = get_TCIA_series_per_study(collection.tcia_api_collection_id, patient.submitter_case_id, study.study_instance_uid)
    # If the study is new, then all the studies are new

    for series in study.seriess:
        if series.series_instance_uid in analysis_collection_dois:
            series.source_doi = analysis_collection_dois[series.series_instance_uid]
        else:
            series.source_doi = data_collection_doi
    sess.commit()


def build_study(sess, args, study_index, version, collection, patient, study, data_collection_doi, analysis_collection_dois):
    if True:
        begin = time.time()
        rootlogger.info("    p%s: Study %s, %s, %s series", args.id, study.study_instance_uid, study_index, len(study.seriess))
        insert_dois(sess, collection, patient, study, data_collection_doi, analysis_collection_dois)
        duration = str(timedelta(seconds=(time.time() - begin)))
        rootlogger.info("    p%s: Study %s, %s,  completed in %s", args.id, study.study_instance_uid, study_index, duration)
    else:
        rootlogger.info("    p%s: Study %s, %s, previously built", args.id, study.study_instance_uid, study_index)


def build_patient(sess, args, patient_index, data_collection_doi, analysis_collection_dois, version, collection, patient):
    if True:
        begin = time.time()
        rootlogger.info("  p%s: Patient %s, %s, %s studies", args.id, patient.submitter_case_id, patient_index, len(patient.studies))
        for study in patient.studies:
            study_index = f'{patient.studies.index(study) + 1} of {len(patient.studies)}'
            build_study(sess, args, study_index, version, collection, patient, study, data_collection_doi, analysis_collection_dois)
        patient.patient_timestamp = min([study.study_timestamp for study in patient.studies])

        duration = str(timedelta(seconds=(time.time() - begin)))
        rootlogger.info("  p%s: Patient %s, %s, completed in %s", args.id, patient.submitter_case_id, patient_index, duration)
    else:
        rootlogger.info("  p%s: Patient %s, %s, previously built", args.id, patient.submitter_case_id, patient_index)


def build_collection(sess, args, collection_index, version, collection):
    if True:
    # if collection.tcia_api_collection_id == 'Lung Phantom': # Temporary code for development
        # begin = time.time()
        # args.prestaging_bucket = f"{args.prestaging_bucket_prefix}{collection.tcia_api_collection_id.lower().replace(' ','_').replace('-','_')}"
        # if not collection.expanded:
        #     expand_collection(sess, args, collection)
        # rootlogger.info("Collection %s, %s, %s patients", collection.tcia_api_collection_id, collection_index, len(collection.patients))
        # Get the lists of data and analyis series in this patient
        data_collection_doi = get_data_collection_doi(collection.tcia_api_collection_id)
        pre_analysis_collection_dois = get_analysis_collection_dois(collection.tcia_api_collection_id)
        analysis_collection_dois = {x['SeriesInstanceUID']: x['SourceDOI'] for x in pre_analysis_collection_dois}
        pass
        # for series in sorted_seriess:
        for patient in collection.patients:
            args.id = 0
            patient_index = f'{collection.patients.index(patient)+1} of {len(collection.patients)}'
            build_patient(sess, args, patient_index, data_collection_doi, analysis_collection_dois, version, collection, patient)

    else:
        rootlogger.info("Collection %s, %s, previously built", collection.tcia_api_collection_id, collection_index)


def build_version(sess, args, version):
    # Session = sessionmaker(bind= sql_engine)
    # version = version_is_done(sess, args.vnext)
    if True:
        begin = time.time()
        rootlogger.info("Version %s; %s collections", version.idc_version_number, len(version.collections))
        repairs = open(args.repairs).read().splitlines()
        for collection in version.collections:
            if collection.tcia_api_collection_id in repairs:
                collection_index = f'{version.collections.index(collection)+1} of {len(version.collections)}'
                build_collection(sess, args, collection_index, version, collection)
        # version.idc_version_timestamp = min([collection.collection_timestamp for collection in version.collections])
        # copy_staging_bucket_to_final_bucket(args,version)
        # if all([collection.done for collection in version.collections if not collection.tcia_api_collection_id in skips]):
        #
        #     version.done = True
        #     sess.commit()
        #     duration = str(timedelta(seconds=(time.time() - begin)))
        #     rootlogger.info("Built version %s in %s", version.idc_version_number, duration)
        # else:
        #     rootlogger.info("Not all collections are done. Rerun.")
    # else:
    #     rootlogger.info("    version %s previously built", version.idc_version_number)


def prebuild(args):
    # Basically add a new Version with idc_version_number args.vnext, if it does not already exist
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
        # If we get here, we have not started work on vnext, so add it to Version
            version = Version(idc_version_number=args.vnext,
                              idc_version_timestamp=datetime.datetime.utcnow(),
                              revised=False,
                              done=False,
                              is_new=True,
                              expanded=False)
            sess.add(version)
            sess.commit()
        build_version(sess, args, version)


if __name__ == '__main__':
    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/log.log'.format(os.environ['PWD']))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/err.log'.format(os.environ['PWD']))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    parser = argparse.ArgumentParser()
    parser.add_argument('--vnext', default=2, help='Next version to generate')
    parser.add_argument('--bucket', default='idc_dev', help='Bucket in which to save instances')
    parser.add_argument('--staging_bucket', default='idc_dev_staging', help='Copy instances here before forwarding to --bucket')
    parser.add_argument('--prestaging_bucket_prefix', default=f'idc_v2_', help='Copy instances here before forwarding to --staging_bucket')
    parser.add_argument('--num_processes', default=8, help="Number of concurrent processes")
    parser.add_argument('--repairs', default='{}/logs/repairs.txt'.format(os.environ['PWD']), help='Collections to repair' )
    parser.add_argument('--bq_dataset', default='mvp_wave2', help='BQ dataset')
    parser.add_argument('--bq_aux_name', default='auxilliary_metadata', help='Auxilliary metadata table name')
    parser.add_argument('--project', default='idc-dev-etl')
    args = parser.parse_args()

    # Directory to which to download files from TCIA/NBIA
    args.dicom = 'dicom'
    print("{}".format(args), file=sys.stdout)

    prebuild(args)
