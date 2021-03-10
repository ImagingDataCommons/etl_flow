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

# Fill in missing series source_dois
# This is, ideally, a one time use routine. It was needed
# because NBIA wasn't alway correctly returning DOIs

TRIES=3

import sys
import os
import argparse
from pathlib import Path
import time
from datetime import datetime, timezone, timedelta
import logging
from logging import INFO
from utilities.get_collection_dois import get_data_collection_doi, get_analysis_collection_dois

# from python_settings import settings
# import settings as etl_settings
#
# settings.configure(etl_settings)
# assert settings.configured
from idc.models import Version, Collection, Patient, Study, Series, Instance, sql_engine
from sqlalchemy import select
from sqlalchemy.orm import Session


def fill_study(sess, args, fills, study_index, version, collection, patient, study,
                                data_collection_doi, analysis_collection_dois):
    filled = 0
    if not study.study_instance_uid in fills:
        begin = time.time()

        # cur.execute("""
        #     SELECT * FROM series
        #     WHERE study_id = (%s)""", (study['id'],))
        # seriess = cur.fetchall()
        seriess = study.seriess

        rootlogger.info("    p%s: Study %s, %s, %s series", args.id, study.study_instance_uid, study_index, len(seriess))
        for series in seriess:
            if series.series_instance_uid in analysis_collection_dois:
                series.source_doi = analysis_collection_dois[series.series_instance_uid]
            else:
                series.source_doi = data_collection_doi
        sess.commit()

        duration = str(timedelta(seconds=(time.time() - begin)))
        rootlogger.info("    p%s: Study %s, %s,  completed in %s", args.id, study.study_instance_uid, study_index, duration)
    else:
        rootlogger.info("    p%s: Study %s, %s, previously built", args.id, study.study_instance_uid, study_index)
    donelogger.info('%s%s', '-' if filled else '', study.study_instance_uid)
    return filled


def fill_patient(sess, args, fills, patient_index, version, collection, patient,
                                  data_collection_doi, analysis_collection_dois):
    filled = 0
    if not patient.submitter_case_id in fills:
        begin = time.time()

        # cur.execute("""
        #     SELECT * FROM study
        #     WHERE patient_id = (%s)""", (patient['id'],))
        # studies = cur.fetchall()
        studies = patient.studies

        rootlogger.info("  p%s: Patient %s, %s, %s studies", args.id, patient.submitter_case_id, patient_index, len(studies))
        for study in studies:
            study_index = f'{studies.index(study)+1} of {len(studies)}'
            filled = fill_study(sess, args, fills, study_index, version, collection, patient, study,
                                data_collection_doi, analysis_collection_dois) | filled

        duration = str(timedelta(seconds=(time.time() - begin)))
        rootlogger.info("  p%s: Patient %s, %s, completed in %s", args.id, patient.submitter_case_id, patient_index, duration)
    else:
        rootlogger.info("  p%s: Patient %s, %s, previously built", args.id, patient.submitter_case_id, patient_index)

    donelogger.info('%s%s', '-' if filled else '', patient.submitter_case_id)
    return filled


def fill_collection(sess, args, dones, fills, collection_index, version, collection):
    if not collection.tcia_api_collection_id in dones:
    # if collection['tcia_api_collection_id'] == 'RIDER Breast MRI': # Temporary code for development
        begin = time.time()
        data_collection_doi = get_data_collection_doi(collection.tcia_api_collection_id)
        pre_analysis_collection_dois = get_analysis_collection_dois(collection.tcia_api_collection_id)
        analysis_collection_dois = {x['SeriesInstanceUID']:x['SourceDOI'] for x in pre_analysis_collection_dois}

        if data_collection_doi=="":
            if collection.tcia_api_collection_id == "ICDC-Glioma":
                data_collection_doi = '10.7937/TCIA.SVQT-Q016'
            elif collection.tcia_api_collection_id == 'Breast-Cancer-Screening-DBT':
                data_collection_doi = '10.7937/e4wt-cd02'
            elif collection.tcia_api_collection_id ==  'CPTAC-PDA':
                data_collection_doi = '10.7937/K9/TCIA.2018.SC20FO18'
            elif collection.tcia_api_collection_id ==  'QIN-PROSTATE-Repeatability':
                data_collection_doi = '10.7937/K9/TCIA.2018.MR1CKGND'
            elif collection.tcia_api_collection_id ==  'Vestibular-Schwannoma-SEG':
                data_collection_doi = '10.7937/TCIA.9YTJ-5Q73'
            elif collection.tcia_api_collection_id == 'NSCLC-Radiomics':
                data_collection_doi = '10.7937/K9/TCIA.2015.PF0M9REI'
            else:
                errlogger.error('No data_collection_doi for %s', collection.tcia_api_collection_id)
                filled = -1
                with open(args.dones, 'a') as f:
                    f.write(f"{'-' if filled else ''}{collection.tcia_api_collection_id}\n")
                return

        # cur.execute("""
        #     SELECT * FROM patient
        #     WHERE collection_id = (%s)""", (collection['id'],))
        # patients = cur.fetchall()
        patients = collection.patients

        rootlogger.info("Collection %s, %s, %s patients", collection.tcia_api_collection_id, collection_index, len(patients))

        filled = 0

        for patient in patients:
            args.id = 0
            patient_index = f'{patients.index(patient)+1} of {len(patients)}'
            filled = fill_patient(sess, args, fills, patient_index, version, collection, patient,
                                  data_collection_doi, analysis_collection_dois) | filled

        with open(args.dones, 'a') as f:
            f.write(f"{'-' if filled else ''}{collection.tcia_api_collection_id}\n")
        # Truncate the validations file minimize searches for the next collection
        os.truncate(args.fills, 0)

    else:
        rootlogger.info("Collection %s, %s, previously built", collection.tcia_api_collection_id, collection_index)



def fill_version(sess, fills, args, version):
    # Session = sessionmaker(bind= sql_engine)
    # version = version_is_done(sess, args.vnext)
    skips = open(args.skips).read().splitlines()
    try:
        dones = open(args.dones).read().splitlines()
    except:
        dones = []
    begin = time.time()
    # cur.execute("""
    #     SELECT * FROM collection
    #     WHERE version_id = (%s)""", (version['id'],))
    # # cur.execute("""
    # #     SELECT * FROM collection
    # #     WHERE version_id = (%s) AND tcia_api_collection_id = (%s)""", (version['id'],'RIDER Breast MRI'))
    # collections = cur.fetchall()
    collections = version.collections

    rootlogger.info("Version %s; %s collections", version.idc_version_number, len(collections))
    for collection in collections:
        if not collection.tcia_api_collection_id in skips:
            collection_index = f'{collections.index(collection)+1} of {len(collections)}'
            fill_collection(sess, args, dones, fills, collection_index, version, collection)


def prefill(args):
    fills = open(args.fills).read().splitlines()
    with Session(sql_engine) as sess:
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

            fill_version(sess, fills, args, version)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--vnext', default=2, help='Next version to generate')
    parser.add_argument('--skips', default='./logs/filldoisskips_v1.log', help="Collections to be skipped")
    parser.add_argument('--dones', default='./logs/filldoisdonecollections_v1.log', help="Completed collections")
    parser.add_argument('--fills', default='{}/logs/filldoisdoneobjects_v1.log'.format(os.environ['PWD']), help="Completed patients, studies, series")
    parser.add_argument('--project', default='idc-dev-etl')
    args = parser.parse_args()

    # Directory to which to download files from TCIA/NBIA
    args.dicom = 'dicom'
    print("{}".format(args), file=sys.stdout)

    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/filldoislog_v1.log'.format(os.environ['PWD']))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    donelogger = logging.getLogger('done')
    done_fh = logging.FileHandler(args.fills)
    doneformatter = logging.Formatter('%(message)s')
    donelogger.addHandler(done_fh)
    done_fh.setFormatter(doneformatter)
    donelogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/filldoiserr.log'.format(os.environ['PWD']))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    prefill(args)
