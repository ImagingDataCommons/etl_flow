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

# We have now populated a DICOM store with all data in the version, and exported that data to BQ.
# We now need to delete all the data from redacted collections so that the DICOM store can be
# used for viewing, etc.

import argparse
import logging
from utilities.logging_config import successlogger, progresslogger, errlogger
import settings
import google
from google.cloud import bigquery
from google.auth.transport import requests
from sqlalchemy import create_engine
from sqlalchemy_utils import register_composites
from sqlalchemy.orm import Session

def get_limited_series(args):
    client = bigquery.Client()
    query = f"""
    WITH
      -- Get all radiology collection/study/series
      tcia_series AS (
      SELECT
        DISTINCT 
            replace(replace(lower(collection_id), '-', '_'), ' ', '_') as idc_webapp_collection_id, 
            study_instance_uid as StudyInstanceUID, 
            series_instance_uid as SeriesInstanceUID
      FROM
        `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_included`
      WHERE
        -- Pathology is all public access
        se_sources.tcia=TRUE
        AND idc_version={settings.CURRENT_VERSION} )
    SELECT
      tcia_series.*
    FROM
      `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_included_collections` aic
    JOIN
      tcia_series
    ON
      tcia_series.idc_webapp_collection_id = aic.idc_webapp_collection_id
    WHERE
      aic.tcia_access ='Limited'
    ORDER BY tcia_series.SeriesInstanceUID
    """
    # query = f"""
    # select distinct idc_webapp_collection_id, StudyInstanceUID, SeriesInstanceUID
    # from `{settings.DEV_PROJECT}.{settings.BQ_DEV_EXT_DATASET}.auxiliary_metadata`
    # WHERE access = 'Limited'
    # """
    limited_series = client.query(query).result()
    return limited_series


def delete_series(args, dicomweb_session, study_instance_uid, series_instance_uid):
    # URL to the Cloud Healthcare API endpoint and version
    base_url = "https://healthcare.googleapis.com/v1"
    url = f"{base_url}/projects/{settings.GCH_PROJECT}/locations/{settings.GCH_REGION}"
    dicomweb_path = f"{url}/datasets/{settings.GCH_DATASET}/dicomStores/{settings.GCH_DICOMSTORE}/dicomWeb/studies/{study_instance_uid}/series/{series_instance_uid}"

    # Set the required application/dicom+json; charset=utf-8 header on the request
    headers = {"Content-Type": "application/dicom+json; charset=utf-8"}

    response = dicomweb_session.delete(dicomweb_path, headers=headers)
    # response.raise_for_status()
    if response.status_code == 200:
        successlogger.info(series_instance_uid)
    else:
        errlogger.error(series_instance_uid)


def delete_all_series(args):
    scoped_credentials, project = google.auth.default(
        ["https://www.googleapis.com/auth/cloud-platform"]
    )
    # Creates a requests Session object with the credentials.
    dicomweb_sess = requests.AuthorizedSession(scoped_credentials)

    try:
        # Get the previously copied blobs
        done_series = set(open(f'{settings.LOG_DIR}/success.log').read().splitlines())
    except:
        done_series = set()

    # We need to remove all limited access series
    limited_series = get_limited_series(args)

    n=0
    for row in limited_series:
        if not row.SeriesInstanceUID in done_series:
            delete_series(args, dicomweb_sess, row.StudyInstanceUID, row.SeriesInstanceUID)
            progresslogger.info(f"{n}: {row.idc_webapp_collection_id}/{row.StudyInstanceUID}/{row.SeriesInstanceUID}  deleted")
        else:
            progresslogger.info(f"{n}: {row.idc_webapp_collection_id}/{row.StudyInstanceUID}/{row.SeriesInstanceUID} previously deleted")
        n+=1
    pass

def delete_redacted(args):
    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{settings.CLOUD_DATABASE}'
    sql_engine = create_engine(sql_uri, echo=True) # Use this to see the SQL being sent to PSQL
    # sql_engine = create_engine(sql_uri)
    args.sql_uri = sql_uri # The subprocesses need this uri to create their own SQL engine

    # Enable the underlying psycopg2 to deal with composites
    conn = sql_engine.connect()
    register_composites(conn)

    scoped_credentials, project = google.auth.default(
        ["https://www.googleapis.com/auth/cloud-platform"]
    )
    # Creates a requests Session object with the credentials.
    dicomweb_sess = requests.AuthorizedSession(scoped_credentials)

    with Session(sql_engine) as sess:
        delete_all_series(args, sess, dicomweb_sess)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    # if not os.path.exists('{}'.format(args.log_dir)):
    #     os.mkdir('{}'.format(args.log_dir))
    #     st = os.stat('{}'.format(args.log_dir))
    #
    # successlogger = logging.getLogger('root.success')
    # successlogger.setLevel(INFO)
    #
    # errlogger = logging.getLogger('root.err')

    delete_all_series(args)