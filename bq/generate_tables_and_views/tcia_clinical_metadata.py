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

# Generate a table of TCIA clinical metadata packages

import argparse
import sys
import json
from utilities.tcia_helpers import get_all_tcia_metadata, get_url
from google.cloud import bigquery
from utilities.bq_helpers import load_BQ_from_json
from bq.generate_tables_and_views.original_collections_metadata.schema import data_collections_metadata_schema
from utilities.logging_config import errlogger
from python_settings import settings


clinical_data_schema = [
    bigquery.SchemaField('idc_collection_id', 'STRING', mode='NULLABLE', description='Associated IDC collection_id'),
    bigquery.SchemaField('download_id', 'STRING', mode='NULLABLE', description='Collection manager id of this download'),
    bigquery.SchemaField('download_slug', 'STRING', mode='NULLABLE', description='Collection manager slug of this download'),
    bigquery.SchemaField('parent_type', 'STRING', mode='NULLABLE', description='"collection" or "analysis_result"'),
    bigquery.SchemaField('parent_id', 'STRING', mode='NULLABLE', description='Collection manager id of this parent object'),
    bigquery.SchemaField('parent_slug', 'STRING', mode='NULLABLE', description='Collection manager of this parent object'),
    bigquery.SchemaField('parent_doi', 'STRING', mode='NULLABLE', description='Collection manager doi of parent object'),
    bigquery.SchemaField('parent_browse_title', 'STRING', mode='NULLABLE', description='Collection manager browse title of parent object'),
    bigquery.SchemaField('date_updated', 'DATE', mode='NULLABLE', description='?'),
    bigquery.SchemaField('download_title', 'STRING', mode='NULLABLE', description='Download title'),
    bigquery.SchemaField('file_type', 'STRING', mode='NULLABLE', description='File type'),
    bigquery.SchemaField('download_size', 'STRING', mode='NULLABLE', description='Download size'),
    bigquery.SchemaField('download_size_unit', 'STRING', mode='NULLABLE', description='Download size units'),
    bigquery.SchemaField('download_url', 'STRING', mode='NULLABLE', description='URL from which to download clinical data'),
    bigquery.SchemaField('download_type', 'STRING', mode='NULLABLE', description='type of download'),
]


def likely_clinical(download):
    try:
        download_type = download['download_type']
        if not (
            (type(download_type) == str and download_type.lower() in ['other', 'clinical data', 'image annotations']) or
            type(download_type) == list and (set(['clinical data', 'image annotations', 'other']) & set([d.lower() for d in download_type]))
        ):
            return False
    except Exception as exc:
        errlogger.error(f'Likely clinical error: {exc}')
        return False
    try:
        if (download['download_requirements']):
            return False
    except Exception as exc:
        errlogger.error(f'Likely clinical error: {exc}')
        return False
    try:
        file_type = download['file_type']
        if not (('XLSX' in file_type) or ('XLS' in file_type) or ('CSV' in file_type)):
            return False
    except Exception as exc:
        errlogger.error(f'Likely clinical error: {exc}')
        return False
    return True


def get_raw_data():
    client = bigquery.Client()
    # Get collections and source_dois that we have in IDC
    all_idc_collections = client.list_rows(client.get_table(f'{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_collections')).to_dataframe()
    all_idc_source_dois = all_idc_collections['source_doi']

    # Get all TCIA collections which we also have
    public_tcia_collections = [ c for c in get_all_tcia_metadata("collections") if \
                                c['collection_page_accessibility'] == "Public" and \
                                c['collection_doi'].lower() in list(all_idc_source_dois)]

    # Get all TCIA analysis results which we also have
    public_analysis_results = [ ar for ar in get_all_tcia_metadata('analysis-results') \
                                if ar['result_page_accessibility'] == "Public" and \
                                ar['result_doi'].lower() in list(all_idc_source_dois)]

    # Get TCIA clinical downloads
    downloads = {d['id']: d for d in get_all_tcia_metadata("downloads")}
    clinical_downloads = {id: data for id, data in downloads.items() if likely_clinical(data)}

    for collection in public_tcia_collections:
        for id in collection['collection_downloads']:
            if id in clinical_downloads:
                clinical_downloads[id]['collection_id'] = collection['id']
                clinical_downloads[id]['collection_slug'] = collection['slug']
                clinical_downloads[id]['collection_doi'] = collection['collection_doi']
                clinical_downloads[id]['collection_browse_title'] = collection['collection_browse_title']

    # Associate 0 or more analysis results with each clinical download
    for result in public_analysis_results:
        for id in result['result_downloads']:
            if id in clinical_downloads:
                clinical_downloads[id]['result_id'] = result['id']
                clinical_downloads[id]['result_slug'] = result['slug']
                clinical_downloads[id]['result_doi'] = result['result_doi']
                clinical_downloads[id]['result_browse_title'] = result['result_browse_title']

    clinical_data = []
    for id, data in clinical_downloads.items():
        if 'collection_slug' in data:
            download = dict(
                idc_collection_id = data["collection_slug"].replace('-','_'),
                download_id = id,
                download_slug = data['slug'],
                parent_type = 'collection',
                parent_id = data["collection_id"],
                parent_slug = data["collection_slug"],
                parent_doi = data['collection_doi'],
                parent_browse_title=data['collection_browse_title'],
                date_updated = data["date_updated"],
                download_title = str(data["download_title"]),
                file_type = str(data["file_type"]),
                download_size = str(data["download_size"]),
                download_size_unit = data["download_size_unit"],
                download_url = data["download_url"] if data["download_url"].startswith('https') else \
                   get_url(f'https://cancerimagingarchive.net/api/wp/v2/media/{data["download_file"]["ID"]}').json()['source_url'],
                download_type = str(data["download_type"])
            )
            clinical_data.append(download)
        elif 'result_slug' in data:
            for collection_id in list(all_idc_collections.loc[all_idc_collections['source_doi']==data['result_doi'].lower()]['collection_id']):
                download = dict(
                    idc_collection_id = collection_id,
                    download_id = id,
                    download_slug = data['slug'],
                    parent_type = 'analysis_result',
                    parent_id = data["result_id"],
                    parent_slug = data["result_slug"],
                    parent_doi = data["result_doi"],
                    parent_browse_title=data['result_browse_title'],
                    date_updated = data["date_updated"],
                    download_title = str(data["download_title"]),
                    file_type = str(data["file_type"]),
                    download_size = str(data["download_size"]),
                    download_size_unit = data["download_size_unit"],
                    download_url = data["download_url"] if data["download_url"].startswith('https') else \
                       get_url(f'https://cancerimagingarchive.net/api/wp/v2/media/{data["download_file"]["ID"]}').json()['source_url'],
                    download_type = str(data["download_type"])
                )
                clinical_data.append(download)


    metadata_json = '\n'.join([json.dumps(row) for row in
                        sorted(clinical_data, key=lambda d: d['download_slug'])])
    try:
        BQ_client = bigquery.Client(project=settings.DEV_PROJECT)
        load_BQ_from_json(BQ_client,
                    settings.DEV_PROJECT,
                    settings.BQ_DEV_INT_DATASET, args.bqtable_name, metadata_json,
                                clinical_data_schema, write_disposition='WRITE_TRUNCATE')
        pass
    except Exception as exc:
        errlogger.error(f'Table creation failed: {exc}')
        exit

    return




if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--bqtable_name', default='tcia_clinical_and_related_metadata', help='BQ table name')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)

    get_raw_data()