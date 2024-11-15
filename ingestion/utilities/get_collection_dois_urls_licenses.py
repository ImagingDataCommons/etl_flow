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
from utilities.logging_config import errlogger

logger = logging.getLogger(__name__)
from utilities.tcia_helpers import get_internal_series_ids, series_drill_down, get_TCIA_series_metadata, \
    get_license_info
from idc.models import IDC_Collection, IDC_Patient, IDC_Study, IDC_Series
from python_settings import settings


def get_dois_tcia(collection, patient="", third_party="no", server=""):
    series_dois = {}
    try:
        internal_ids = get_internal_series_ids(collection, patient, third_party, server=server)
    except Exception as exc:
        print(f'Exception in get_analysis_collection_dois_tcia {exc}')
        logger.error('Exception in get_analysis_collection_dois_tcia %s', exc)
        raise exc
    for subject in internal_ids["resultSet"]:
        seriesIDs = []
        for study in subject["studyIdentifiers"]:
            seriesIDs.extend(study["seriesIdentifiers"])
        study_metadata = series_drill_down(seriesIDs, server=server)
        for study in study_metadata:
            for series in study["seriesList"]:
                uri = series["descriptionURI"]
                if uri:
                    # If it's a doi.org uri, keep just the DOI
                    if 'doi.org' in uri:
                        uri = uri.split('doi.org/')[1].lower()
                    seriesUID = series["seriesUID"]
                    series_dois[seriesUID] = {"source_doi": uri, "versioned_source_doi": ""}
                elif collection == 'NSCLC Radiogenomics':
                    breakpoint()
                    seriesUID = series["seriesUID"]
                    series_dois[seriesUID] = {"source_doi": "10.7937/K9/TCIA.2017.7hs46erv", "versioned_source_doi": ""}
                else:
                    breakpoint()
                    errlogger.error(
                        f'No TCIA DOI for series {collection}/{patient}/{series["seriesUID"]}/{series["seriesUID"]}')
                    return {}
    return series_dois


def get_patient_dois_tcia(collection, patient):
    server = "NLST" if collection=="NLST" else ""
    dois = get_dois_tcia(collection, patient, third_party="no", server=server)
    dois = dois | get_dois_tcia(collection, patient, third_party="yes", server=server)
    return dois


# Get a per-series list of source DOIs for a patient. This routine finds series in
# data sourced from IDC.
def get_patient_dois_idc(sess, collection, patient):
    try:
        query = sess.query(IDC_Series.series_instance_uid.label('SeriesInstanceUID'), \
            IDC_Series.source_doi, IDC_Series.versioned_source_doi). \
            join(IDC_Collection.patients).join(IDC_Patient.studies).join(IDC_Study.seriess). \
            filter(IDC_Collection.collection_id == collection). \
            filter(IDC_Patient.submitter_case_id == patient). \
            filter(IDC_Series.source_doi != None)
        series_dois = {row['SeriesInstanceUID']: {'source_doi': row['source_doi'].lower(), 'versioned_source_doi' :row['versioned_source_doi'].lower()} for row in [row._asdict() for row in query.all()]}
        return series_dois
    except:
        return {}


# Get a per-series list of source URLs for a patient. This routine finds series in
# data sourced from TCIA.
def get_patient_urls_tcia(collection, patient):
    urls= get_patient_dois_tcia(collection, patient)
    for series_instance_uid, doi in urls.items():
        urls[series_instance_uid] =f'https://doi.org/{doi.lower}'
    return urls


# Get a per-series list of source URLs for a patient. This routine finds series in
# data sourced from TCIA.
def get_patient_urls_idc(sess, collection, patient):
    try:
        query = sess.query(IDC_Series.series_instance_uid.label('SeriesInstanceUID'), \
            IDC_Series.source_url.label('SourceURL')). \
            join(IDC_Collection.patients).join(IDC_Patient.studies).join(IDC_Study.seriess). \
            filter(IDC_Collection.collection_id == collection). \
            filter(IDC_Patient.submitter_case_id == patient). \
            filter(IDC_Series.source_url != None)
        series_urls = {row['SeriesInstanceUID']: row['SourceURL'].lower() for row in [row._asdict() \
                     for row in query.all()]}
        return series_urls
    except:
        return {}


# Get a per-series list of licenses for a patient. This routine finds series in
# data sourced from TCIA.
def get_licenses_tcia(collection, patient, third_party="no", server=""):
    license_types = get_license_info()
    series_licenses = {}
    try:
        internal_ids = get_internal_series_ids(collection, patient, third_party, server=server)
    except Exception as exc:
        print(f'Exception in get_analysis_collection_dois_tcia {exc}')
        logger.error('Exception in get_analysis_collection_dois_tcia %s', exc)
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
                series_metadata = \
                    get_TCIA_series_metadata(seriesUID)
                if "License URL" in series_metadata:
                    series_licenses[seriesUID] = {
                        "license_url": series_metadata["License URL"],
                        "license_long_name": series_metadata["License Name"],
                        "license_short_name": license_types[series_metadata["License Name"]]['shortName']
                    }
                elif collection in ['CPTAC-PDA', 'Breast-MRI-NACT-Pilot']:
                    series_licenses[seriesUID] = {
                        "license_url": license_types['Creative Commons Attribution 3.0 Unported License']["licenseURL"],
                        "license_long_name": license_types['Creative Commons Attribution 3.0 Unported License']["longName"],
                        "license_short_name": license_types['Creative Commons Attribution 3.0 Unported License']["shortName"]
                    }
                elif collection == 'Adrenal-ACC-Ki67-Seg':
                    series_licenses[seriesUID] = {
                        "license_url": license_types['Creative Commons Attribution 4.0 International License']['licenseURL'],
                        "license_long_name": license_types['Creative Commons Attribution 4.0 International License']['longName'],
                        "license_short_name": license_types['Creative Commons Attribution 4.0 International License']['shortName']
                    }
                elif collection == 'CT-Phantom4Radiomics':
                    series_licenses[seriesUID] = {
                    "license_url": license_types['Creative Commons Attribution 4.0 International License'][
                        'licenseURL'],
                    "license_long_name": license_types['Creative Commons Attribution 4.0 International License'][
                        'longName'],
                    "license_short_name": license_types['Creative Commons Attribution 4.0 International License'][
                        'shortName']
                }
                elif collection == 'Spine-Mets-CT-SEG':
                    series_licenses[seriesUID] = {
                    "license_url": license_types['Creative Commons Attribution 4.0 International License'][
                        'licenseURL'],
                    "license_long_name": license_types['Creative Commons Attribution 4.0 International License'][
                        'longName'],
                    "license_short_name": license_types['Creative Commons Attribution 4.0 International License'][
                        'shortName']
                }
                else:
                    breakpoint()
                    errlogger.error(f'No license info for {collection}/{patient}')
    return series_licenses


# Get a per-series list of licenses for a patient. This routine finds series in
# data sourced from TCIA.
def get_patient_licenses_tcia(collection, patient, third_party="no", server=""):
    server = "NLST" if collection == "NLST" else ""
    series_licences = get_licenses_tcia(collection, patient, third_party="no", server=server)
    series_licences = series_licences | \
                      get_licenses_tcia(collection, patient, third_party="yes", server=server)
    return series_licences


# Get a per-series list of licenses for a patient. This routine finds series in
# data sourced from IDC.
def get_patient_licenses_idc(sess, collection, patient):
    try:
        query = sess.query(IDC_Series.series_instance_uid.label('SeriesInstanceUID'), \
            IDC_Series.license_url, IDC_Series.license_long_name, IDC_Series.license_short_name). \
            join(IDC_Collection.patients).join(IDC_Patient.studies).join(IDC_Study.seriess). \
            filter(IDC_Collection.collection_id == collection). \
            filter(IDC_Patient.submitter_case_id == patient)
        series_urls = {row['SeriesInstanceUID']:
                            {'license_url': row['license_url'],
                            'license_long_name': row['license_long_name'],
                            'license_short_name': row['license_short_name']}
                       for row in [row._asdict() for row in query.all()]}
        return series_urls

    except:
        return {}

    pass


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
        result = get_patient_licenses_tcia('CPTAC-PDA', 'C3L-05049', third_party="no", server="")
        result = get_dois_tcia('CC-Tumor-Heterogeneity')
        from utilities.tcia_helpers import get_collection_values_and_counts
        collections = get_collection_values_and_counts()
        pass
        # yes=get_internal_collection_series_ids('TCGA-GBM',"yes")
        # result = get_internal_patient_series_ids('TCGA-GBM', 'TCGA-76-6664', "yes")
