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


# from BQ.collection_ids_file.gen_collection_id_table import build_collections_id_table

# For a specified collection, generate a list of series that came from some third party analysis
def get_access_token():
    data = dict(
        username="nbia_guest",
        password="",
        client_id="nbiaRestAPIClient",
        client_secret="ItsBetweenUAndMe",
        grant_type="password"
    )
    result = requests.post(
        "https://public.cancerimagingarchive.net/nbia-api/oauth/token",
        data = data
    )
    return result.json()['access_token']


# Get NBIAs internal ID for all the series in a collection/patient
def get_internal_series_ids(collection, patient, third_party="yes", size=100000):
    access_token = get_access_token()

    headers = dict(
        Authorization=f"Bearer {access_token}",
    )
    if not patient=="":
        data = dict(
            criteriaType0="ThirdPartyAnalysis",
            value0=third_party,
            criteriaType1="CollectionCriteria",
            value1=collection,
            criteriaType2="PatientCriteria",
            value2=patient,
            sortField="subject",
            sortDirection="descending",
            start=0,
            size=size)
    else:
        data = dict(
            criteriaType0="ThirdPartyAnalysis",
            value0=third_party,
            criteriaType1="CollectionCriteria",
            value1=collection,
            sortField="subject",
            sortDirection="descending",
            start=0,
            size=size)

    result = requests.post(
        "https://public.cancerimagingarchive.net/nbia-api/services/getSimpleSearchWithModalityAndBodyPartPaged",
        headers=headers,
        data=data
    )
    return result.json()


def drill_down(series_ids):
    access_token = get_access_token()
    headers = dict(
        Authorization=f"Bearer {access_token}",
    )
    data = "&".join(['list={}'.format(id) for id in series_ids])

    url = "https://public.cancerimagingarchive.net/nbia-api/services/getStudyDrillDown"

    # result = requests.post(
    #     url,
    #     headers=headers,
    #     data=data
    # )

    try:
        result = run(['curl', '-v', '-H', "Authorization:Bearer {}".format(access_token), '-k',  url, '-d', data],
                     stdout=PIPE, stderr=PIPE)
    except:
        pass
    return json.loads(result.stdout.decode())

def get_data_collection_doi(collection):

    dois = []
    count = 0
    # This will get us doi's for a one or all patients in a collection
    internal_ids = get_internal_series_ids(collection, patient="", third_party="no", size=1)
    subject = internal_ids["resultSet"][0]
    study = subject["studyIdentifiers"][0]
    seriesIDs = study["seriesIdentifiers"]
    study_metadata = drill_down(seriesIDs)
    study = study_metadata[0]
    series = study["seriesList"][0]
    uri = series["descriptionURI"]
    # If it's a doi.org uri, keep just the DOI
    if 'doi.org' in uri:
        uri = uri.split('doi.org/')[1]

    return uri


def get_analysis_collection_dois(collection, patient= "", third_party="yes"):
    third_party_series = []
    try:
        internal_ids = get_internal_series_ids(collection, patient)
    except Exception as exc:
        print(f'Exception in get_analysis_collection_dois {exc}')
        logger.error('Exception in get_analysis_collection_dois %s', exc)
        raise exc
    for subject in internal_ids["resultSet"]:
        seriesIDs = []
        for study in subject["studyIdentifiers"]:
            seriesIDs.extend(study["seriesIdentifiers"])
        study_metadata = drill_down(seriesIDs)
        for study in study_metadata:
            for series in study["seriesList"]:
                uri = series["descriptionURI"]
                # If it's a doi.org uri, keep just the DOI
                if 'doi.org' in uri:
                    uri = uri.split('doi.org/')[1]
                seriesUID = series["seriesUID"]
                third_party_series.append({"SeriesInstanceUID": seriesUID, "SourceDOI": uri})
    return third_party_series


if __name__ == "__main__":
    # access_token = get_access_token()
    result = get_data_collection_doi('NSCLC-Radiomics')
    result = get_analysis_collection_dois('QIN-PROSTATE-Repeatability')
    pass
    # yes=get_internal_collection_series_ids('TCGA-GBM',"yes")
    # result = get_internal_patient_series_ids('TCGA-GBM', 'TCGA-76-6664', "yes")
