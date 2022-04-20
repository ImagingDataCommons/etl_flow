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
# If there is no output, then no name changes were detected.

from idc.models import Base, Version, Patient, Study, Series, Collection
from utilities.tcia_scrapers import scrape_tcia_data_collections_page, scrape_tcia_analysis_collections_page
from python_settings import settings
import google
from google.auth.transport import requests

from sqlalchemy import create_engine, distinct
from sqlalchemy_utils import register_composites
from sqlalchemy.orm import Session



def compare_dois():
    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{settings.CLOUD_DATABASE}'
    # sql_engine = create_engine(sql_uri, echo=True) # Use this to see the SQL being sent to PSQL
    sql_engine = create_engine(sql_uri)

    # Enable the underlying psycopg2 to deal with composites
    conn = sql_engine.connect()
    register_composites(conn)


    with Session(sql_engine) as sess:
        # Get the source_dois across all series in each collection. This can
        # include both original collection dois and analysis results dois
        rows = sess.query(Collection.collection_id,Series.source_doi).distinct(). \
            join(Version.collections).join(Collection.patients).join(Patient.studies).join(\
            Study.seriess).filter(Version.version == settings.PREVIOUS_VERSION).all()
        idc_dois = {row.source_doi: row.collection_id for row in rows if row.source_doi }

        # Scrape TCIA pages to get a list of dois mapped to IDs
        tcia_original_dois = {item['DOI']: collection_id for collection_id, item in scrape_tcia_data_collections_page().items()}
        tcia_analysis_dois = {item['DOI']: collection_id for collection_id, item in scrape_tcia_analysis_collections_page().items()}

        # Looks for each doi that have in the latter two lists, if found compare IDs.
        for doi in idc_dois:
            if doi in tcia_original_dois:
                if idc_dois[doi].lower() != tcia_original_dois[doi].lower():
                    print(f'Collection ID mismatch, IDC: {idc_dois[doi]}, TCIA: {tcia_original_dois[doi]}')
            elif not doi in tcia_analysis_dois:
                print(f'Collection {idc_dois[doi]} DOI {doi} not in TCIA DOIs')

if __name__ == '__main__':
    compare_dois()





