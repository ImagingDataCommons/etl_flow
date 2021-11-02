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

from python_settings import settings


def get_data_collection_doi(collection, server=""):

    dois = []
    count = 0
    # This will get us doi's for a one or all patients in a collection
    if server:
        internal_ids = get_internal_series_ids(collection, patient="", third_party="no", size=1, server=server)
    else:
        internal_ids = get_internal_series_ids(collection, patient="", third_party="no", size=1)

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


if __name__ == "__main__":
    # access_token = get_access_token()
    # result = get_analysis_collection_dois('DRO-Toolkit')
    result = get_data_collection_doi('APOLLO-5-LSCC')
    result = get_analysis_collection_dois('QIN-PROSTATE-Repeatability')
    pass
    # yes=get_internal_collection_series_ids('TCGA-GBM',"yes")
    # result = get_internal_patient_series_ids('TCGA-GBM', 'TCGA-76-6664', "yes")
