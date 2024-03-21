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


from idc.models import Patient, Study, Series, Collection, All_Collections
from utilities.tcia_helpers import get_all_tcia_metadata
from sqlalchemy import and_, or_
from utilities.sqlalchemy_helpers import sa_session
from utilities.logging_config import successlogger, progresslogger, errlogger, save_log_dirs
import argparse
import json

def compare_dois():
    with sa_session(echo=False) as sess:
        # Get the source_dois across all series in each 'included' collection... collections
        # that are not redacted or excluded. This can
        # include both original collection dois and analysis results dois
        rows = sess.query(Collection.collection_id,Series.source_doi).distinct().join(Collection.patients). \
            join(Patient.studies).join(Study.seriess). \
            join(All_Collections, Collection.collection_id==All_Collections.tcia_api_collection_id). \
            filter(and_(or_(Series.sources == [True, False], Series.sources == [True, True]), All_Collections.tcia_access=="Public")).all()
        idc_dois = {row.source_doi.lower(): row.collection_id for row in rows if row.source_doi }

        # Scrape TCIA pages to get a list of dois mapped to IDs
        # tcia_analysis_dois = {item['DOI']: collection_id for collection_id, item in scrape_tcia_analysis_collections_page().items()}
        # tcia_original_dois = {item['DOI']: collection_id for collection_id, item in scrape_tcia_data_collections_page().items()}
        tcia_original_dois = {row['collection_doi'].lower(): row['collection_short_title'] for row in get_all_tcia_metadata(type="collections", query_param="&_fields=collection_short_title,collection_doi")}
        tcia_analysis_dois = {row['result_doi'].lower(): row['result_short_title'] for row in get_all_tcia_metadata(type="analysis-results", query_param="&_fields=result_short_title,result_doi")}

        # Look for each doi that we have in the latter two lists; if found compare IDs.
        for doi in idc_dois:
            if doi in tcia_original_dois:
                if idc_dois[doi].lower() != tcia_original_dois[doi].lower():
                    if idc_dois[doi] in args.ignored:
                        progresslogger.info(f'Ignoring collection ID mismatch, IDC: {idc_dois[doi]}, TCIA: {tcia_original_dois[doi]}')
                    else:
                        errlogger.error(f'####Collection ID mismatch, IDC: {idc_dois[doi]}, TCIA: {tcia_original_dois[doi]}')
            elif not doi in tcia_analysis_dois:
                if idc_dois[doi] in args.ignored:
                    progresslogger.info(f'Ignoring collection {idc_dois[doi]}, DOI {doi}, not in TCIA DOIs')
                else:
                    errlogger.error(f'####Collection {idc_dois[doi]}, DOI {doi}, not in TCIA DOIs')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--ignored', default=['APOLLO', 'APOLLO-5-THYM', 'APOLLO-5-LSCC', 'APOLLO-5-LUAD', 'APOLLO-5-ESCA', 'APOLLO-5-PAAD'])

    args = parser.parse_args()
    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')


    try:
        compare_dois()
    finally:
        save_log_dirs()





