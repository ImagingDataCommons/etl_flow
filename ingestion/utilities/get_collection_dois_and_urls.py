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
import json
from subprocess import run, PIPE
import requests
import logging

logger = logging.getLogger(__name__)
from utilities.tcia_helpers import get_internal_series_ids, series_drill_down
from ingestion.utilities.utils import to_webapp
from idc.models import Non_TCIA_Collection_Metadata
from python_settings import settings


def get_data_collection_doi(collection, server=""):

    dois = []
    count = 0
    # This will get us doi's for one or all patients in a collection
    if server:
        internal_ids = get_internal_series_ids(collection, patient="", third_party="no", size=1, server=server)
    else:
        internal_ids = get_internal_series_ids(collection, patient="", third_party="no", size=1)

    if internal_ids["resultSet"]:
        subject = internal_ids["resultSet"][0]
        study = subject["studyIdentifiers"][0]
        seriesIDs = study["seriesIdentifiers"]
        if server:
            study_metadata = series_drill_down(seriesIDs, server=server)
        else:
            study_metadata = series_drill_down(seriesIDs)
        study = study_metadata[0]
        series = study["seriesList"][0]
        uri = series["descriptionURI"]
        # If it's a doi.org uri, keep just the DOI
        if uri:
           if 'doi.org' in uri:
               uri = uri.split('doi.org/')[1]
        else:
            uri = ''
    else:
        uri = ''

    if uri=='':


        # These collections do not include radiology data. NBIA does not return a DOI for such collections.
        if collection == 'CPTAC-AML':
            uri = '10.7937/tcia.2019.b6foe619'
        elif collection == 'CPTAC-BRCA':
            uri = '10.7937/TCIA.CAEM-YS80'
        elif collection == 'CPTAC-COAD':
            uri = '10.7937/TCIA.YZWQ-ZZ63'
        elif collection == 'CPTAC-OV':
            uri = '10.7937/TCIA.ZS4A-JD58'

        # NBIA does not return DOIs of redacted collections.
        elif collection == 'CPTAC-GBM':
            uri = '10.7937/K9/TCIA.2018.3RJE41Q1'
        elif collection == 'CPTAC-HNSCC':
            uri = '10.7937/K9/TCIA.2018.UW45NH81'
        elif collection == 'TCGA-GBM':
            uri = '10.7937/K9/TCIA.2016.RNYFUYE9'
        elif collection == 'TCGA-HNSC':
            uri = '10.7937/K9/TCIA.2016.LXKQ47MS'
        elif collection == 'TCGA-LGG':
            uri = '10.7937/K9/TCIA.2016.L4LTD3TK'

        # These are non-TCIA TCGA collections. There are no (yet) DOIs for these.
        # If we ever revise them, we'll come here
        elif collection in [
            'TCGA-ACC',
            'TCGA-CHOL',
            'TCGA-DLBC',
            'TCGA-MESO',
            'TCGA-PAAD',
            'TCGA-PCPG',
            'TCGA-SKCM',
            'TCGA-TGCT',
            'TCGA-THYM',
            'TCGA-UCS',
            'TCGA-UVM']:

            # breakpoint()
            # uri = f'{collection}-DOI'
            uri = ''
        # Shouldn't ever get here, because we won't update NLST
        elif collection == 'NLST':
            breakpoint()
            uri = '10.7937/TCIA.hmq8-j677'
        elif collection == 'CT-vs-PET-Ventilation-Imaging':
            uri = '10.7937/3ppx-7s22'
        elif collection == 'CTpred-Sunitinib-panNET':
            uri = '10.7937/spgk-0p94'

    return uri


def get_analysis_collection_dois(collection, patient= "", third_party="yes", server=""):
    third_party_series = []
    try:
        internal_ids = get_internal_series_ids(collection, patient, server=server)
    except Exception as exc:
        print(f'Exception in get_analysis_collection_dois {exc}')
        logger.error('Exception in get_analysis_collection_dois %s', exc)
        raise exc
    for subject in internal_ids["resultSet"]:
        seriesIDs = []
        for study in subject["studyIdentifiers"]:
            seriesIDs.extend(study["seriesIdentifiers"])
        study_metadata = series_drill_down(seriesIDs)
        for study in study_metadata:
            for series in study["seriesList"]:
                uri = series["descriptionURI"]
                # If it's a doi.org uri, keep just the DOI
                if 'doi.org' in uri:
                    uri = uri.split('doi.org/')[1]
                seriesUID = series["seriesUID"]
                third_party_series.append({"SeriesInstanceUID": seriesUID, "SourceDOI": uri})
    return third_party_series

# Return a source_url associated with a collection
def get_data_collection_url(collection, sess):
    try:
        url = sess.query(Non_TCIA_Collection_Metadata.URL).filter(Non_TCIA_Collection_Metadata.idc_webapp_collection_id == to_webapp(collection)).one()
        return url[0]
    except:
        return None


if __name__ == "__main__":
    from sqlalchemy import create_engine
    from sqlalchemy_utils import register_composites
    from sqlalchemy.orm import Session
    from idc.models import Base

    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{settings.CLOUD_DATABASE}'
    # sql_engine = create_engine(sql_uri, echo=True) # Use this to see the SQL being sent to PSQL
    sql_engine = create_engine(sql_uri)
    # args.sql_uri = sql_uri # The subprocesses need this uri to create their own SQL engine

    # Create the tables if they do not already exist
    Base.metadata.create_all(sql_engine)

    # Enable the underlying psycopg2 to deal with composites
    conn = sql_engine.connect()
    register_composites(conn)

    with Session(sql_engine) as sess:

        # access_token = get_access_token()
        # result = get_analysis_collection_dois('DRO-Toolkit')
        result = get_data_collection_doi('UPENN-GBM')
        result = get_data_collection_url('tcga_dlbc', sess)
        result = get_analysis_collection_dois('QIN-PROSTATE-Repeatability')
        from utilities.tcia_helpers import get_collection_values_and_counts
        collections = get_collection_values_and_counts()
        for collection in collections:
            doi = get_data_collection_doi(collection)
            print(f"{collection}: {doi}")
        pass
        # yes=get_internal_collection_series_ids('TCGA-GBM',"yes")
        # result = get_internal_patient_series_ids('TCGA-GBM', 'TCGA-76-6664', "yes")
