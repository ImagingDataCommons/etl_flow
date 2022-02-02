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

# This script looks for collections for which TCIA has changed the ID.
# It uses the source DOI to map between TCIA collections and IDC
# collections names. Collection IDs that are only different in casing
# are ignored

import sys
import argparse
from idc.models import Base, Version, Patient, Study, Series, Collection
from utilities.tcia_scrapers import scrape_tcia_data_collections_page, scrape_tcia_analysis_collections_page
import settings as etl_settings
from python_settings import settings
if not settings.configured:
    settings.configure(etl_settings)
import google
from google.auth.transport import requests

from sqlalchemy import create_engine, distinct
from sqlalchemy_utils import register_composites
from sqlalchemy.orm import Session


def compare_dois(args):
    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{args.db}'
    # sql_engine = create_engine(sql_uri, echo=True) # Use this to see the SQL being sent to PSQL
    sql_engine = create_engine(sql_uri)
    args.sql_uri = sql_uri # The subprocesses need this uri to create their own SQL engine

    # Create the tables if they do not already exist
    Base.metadata.create_all(sql_engine)

    # Enable the underlying psycopg2 to deal with composites
    conn = sql_engine.connect()
    register_composites(conn)

    scoped_credentials, project = google.auth.default(
        ["https://www.googleapis.com/auth/cloud-platform"]
    )
    # Creates a requests Session object with the credentials.
    dicomweb_sess = requests.AuthorizedSession(scoped_credentials)

    with Session(sql_engine) as sess:
        rows = sess.query(Collection.collection_id,Series.source_doi).distinct(). \
            join(Version.collections).join(Collection.patients).join(Patient.studies).join(\
            Study.seriess).filter(Version.version == args.version).all()
        idc_dois = {row.source_doi: row.collection_id for row in rows }

        tcia_original_dois = {item['DOI']: collection_id for collection_id, item in scrape_tcia_data_collections_page().items()}
        tcia_analysis_dois = {item['DOI']: collection_id for collection_id, item in scrape_tcia_analysis_collections_page().items()}
        for doi in idc_dois:
            if doi in tcia_original_dois:
                if idc_dois[doi].lower() != tcia_original_dois[doi].lower():
                    print(f'Collection ID mismatch, IDC: {idc_dois[doi]}, TCIA: {tcia_original_dois[doi]}')
            elif not doi in tcia_analysis_dois:
                print(f'Collection {idc_dois[doi]} DOI {doi} not in TCIA DOIs')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=7, help='IDC version for which to build the table')
    parser.add_argument('--db', default='idc_v7', help='IDC version for which to build the table')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    compare_dois(args)





