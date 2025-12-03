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

# Build the analysis_results_metadata BQ table

import argparse
import sys
import os
import json
import time
from re import split as re_split
from google.cloud import bigquery
from utilities.bq_helpers import load_BQ_from_json
# from bq.generate_tables_and_views.analysis_results_metadata.schema import analysis_results_metadata_schema
from utilities.tcia_helpers import get_all_tcia_metadata
from utilities.logging_config import successlogger, progresslogger
# from python_settings import settings
import settings
import requests

analysis_results_metadata_schema = [
    bigquery.SchemaField('ID', 'STRING', mode='REQUIRED', description='Results ID'),
    bigquery.SchemaField('Title', 'STRING', mode='REQUIRED', description='Descriptive title'),
    bigquery.SchemaField('Access', 'STRING', mode='REQUIRED', description='Limited or Public'),
    bigquery.SchemaField('source_doi','STRING', mode='NULLABLE', description='DOI that can be resolved at doi.org to a wiki page'),
    bigquery.SchemaField('source_url','STRING', mode='REQUIRED', description='URL of a wiki page'),
    bigquery.SchemaField('CancerTypes','STRING', mode='REQUIRED', description='Type(s) of cancer analyzed'),
    bigquery.SchemaField('TumorLocations', 'STRING', mode='REQUIRED', description='Body location that was analyzed'),
    bigquery.SchemaField('Subjects', 'INTEGER', mode='REQUIRED', description='Number of subjects whose data was analyzed'),
    bigquery.SchemaField('Collections', 'STRING', mode='REQUIRED', description='collection_names of original data collections analyzed'),
    bigquery.SchemaField('Modalities', 'STRING', mode='REQUIRED', description='Modalities of this analysis result'),
    bigquery.SchemaField('Updated', 'DATE', mode='REQUIRED', description='Most recent update reported by TCIA'),
    bigquery.SchemaField('license_url', 'STRING', mode='REQUIRED', description='URL of license of this analysis result'),
    bigquery.SchemaField('license_long_name', 'STRING', mode='REQUIRED', description='Long name of license of this analysis result'),
    bigquery.SchemaField('license_short_name', 'STRING', mode='REQUIRED', description='Short name of license of this analysis result'),
    bigquery.SchemaField('Description', 'STRING', mode='REQUIRED',
                         description='Analysis result description'),
    bigquery.SchemaField('Citation', 'STRING', mode='NULLABLE',
                         description='Citation to be used for this source'),
]


def get_descriptions(client,args):
    query = f"""
    SELECT ID, Description
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.analysis_results_descriptions_end_user` 
    """
    descriptions = {c.ID: c.Description for c in client.query(query).result()}
    return descriptions


def get_idc_sourced_analysis_metadata(client):
    query = f"""
    SELECT DISTINCT ID, Title, Access, source_doi, Updated
    FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.analysis_results_metadata_idc_source`
    """
    results = [dict(row) for row in client.query(query).result()]
    metadata = {row["source_doi"]:row for row in results}
    return metadata


# Get a list of subjects per source_doi
def get_citation(source_url):
    temp_citations = {

        "https://doi.org/10.5281/zenodo.15643312": "Krishnaswamy, D., Bridge, C., Clunie, D., &amp; Fedorov, A. (2025). <i>PROSTATEx-Targets: Point annotations of lesion targets for the PROSTATEx collection</i>[Dataset]. Zenodo. https://doi.org/10.5281/zenodo.15643312",
        "https://doi.org/10.5281/zenodo.15643334": "Krishnaswamy, D., Bridge, C., Clunie, D., &amp; Fedorov, A. (2025). <i>NLST-Sybil: Expert annotations of tumor regions in the NLST CT images</i>[Dataset]. Zenodo. https://doi.org/10.5281/zenodo.15643334",
        "https://doi.org/10.5281/zenodo.16989819": "Krishnaswamy, D., Bridge, C., Clunie, D., &amp; Fedorov, A. (2025). <i>Lung-PET-CT-Dx-Annotations: Expert annotation of lung tumors for the Lung-PET-CT-Dx collection</i>[Dataset]. Zenodo. https://doi.org/10.5281/zenodo.16989819",

        "https://doi.org/10.5281/zenodo.17362624": "Krishnaswamy, D., Clunie, D., &amp; Fedorov, A. (2025). <i>NLSTSeg: Expert lesion segmentations and radiomics features for NLST CT images</i>[Dataset]. Zenodo. https://doi.org/10.5281/zenodo.17362624",
        "https://doi.org/10.5281/zenodo.17470190": "Bridge, C., Zheng, Y., Gevaert, O., Clunie, D., &amp; Fedorov, A. (2025). <i>TCGA-GBM360: GBM360 aggressiveness maps for a subset of TCGA pathology slides</i>[Dataset]. Zenodo. https://doi.org/10.5281/zenodo.17470190",
        "https://doi.org/10.5281/zenodo.16966285": "Bridge, C., Abousamra, S., Saltz, J., Gupta, R., Kurc, T., Zhang, Y., Zhao, T., Batiste, R., Samaras, D., Bremer, E., Shroyer, K. R., Nguyen, V., Singh, P., Hou, L., Le, H., Van Arnam, J., Shmulevich, I., \
Rao, A. U. K., Lazar, A. J., Sharma, A., Thorsson, V., Shankar, A., Chen, C., Clunie, D., &amp; Fedorov, A. (2025). <i>TCGA-SBU-TIL-Maps: AI-derived Tumor Infiltrating Lymphocyte maps for the TCGA collections</i>[Dataset]. Zenodo. https://doi.org/10.5281/zenodo.16966285"

    }

    # header = {"Accept": "text/x-bibliography; style=apa"}
    header = {"Accept": "text/x-bibliography; style=elsevier-vancouver-no-et-al"}
    citation = requests.get(source_url, headers=header).text
    if citation.startswith("<!DOCTYPE html>"):
        try:
            citation = temp_citations[source_url]
        except:
            breakpoint()

    return citation


# Get collection-level metadata: CancerTypes, TumorLocations, modalities, Subjects, CollectionsAnalysed, for
# each analysis result
# analysis_metadata includes all TCIA analysis results, not just those that are in IDC. We need to deal with that.
# 1. Load analysis_metadata, loaded from spreadsheet, into a temp BQ table
# 2. Remove analysis results that IDC does not have
# 3. Get cumulative metadata for each remaining AR
def get_analysis_results_metadata(client, analysis_metadata):
    schema = [
        bigquery.SchemaField("ID", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Title", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Access", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("source_doi", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Updated", "STRING", mode="REQUIRED"),
    ]
    table_id = 'gen_analysis_results_metadata'
    table_ref = client.dataset('whc_dev').table(table_id)
    table = bigquery.Table(table_ref, schema=schema)
    client.delete_table(table, not_found_ok=True)
    table = client.create_table(table,["schema"])
    client.update_table(table,["schema"])
    try:
        errors = client.insert_rows_json(table_ref, [v for k,v in analysis_metadata.items()])

    except:
        try:
            # Known bug. Need to try to insert twice.
            errors = client.insert_rows_json(table_ref, [v for k, v in analysis_metadata.items()])
        except Exception as exc:
            print("Encountered errors while inserting rows:", exc)
            exit(1)
    if errors != []:
        print("Encountered errors while inserting rows:")
        exit(1)
    query = f"""
WITH ocm AS (
  SELECT * except(Updated, sources, CancerTypes, TumorLocations, Subjects)
  FROM `idc-dev-etl.{settings.BQ_DEV_EXT_DATASET}.original_collections_metadata` ocm, 
  UNNEST(sources) AS srcs, UNNEST(SPLIT(CancerTypes, ',')) as CTypes, 
  UNNEST(SPLIT(TumorLocations, ',')) TLocations, 
  UNNEST(SPLIT(srcs.ImageTypes, ',')) ITypes
)
,
s1 AS (
      SELECT ocm.* except(Access), garm.* except(source_doi) 
      FROM ocm
      JOIN `idc-dev-etl.whc_dev.{table_id}` garm
      ON ocm.source_doi = garm.source_doi
 )

SELECT DISTINCT ID, Title, s1.Access Access, s1.source_doi source_doi, CONCAT("https://doi.org/",s1.source_doi) source_url,  
    STRING_AGG(DISTINCT TRIM(s1.CTypes, ' '), ", " ORDER BY TRIM(CTypes, ' ')) CancerTypes,
    STRING_AGG(DISTINCT TRIM(TLocations, ' '), ", " ORDER BY TRIM(TLocations, ' ')) TumorLocations,
    COUNT(DISTINCT da.PatientID) Subjects,
    STRING_AGG( DISTINCT TRIM(s1.collection_id, ' '), ", " ORDER BY TRIM(s1.collection_id, ' ')) Collections,
    STRING_AGG( DISTINCT TRIM(Modality, ' '), ", " ORDER BY TRIM(Modality, ' ')) Modalities,
    Updated,
    license_url, license_long_name, license_short_name
    FROM s1
    JOIN `idc-dev-etl.{settings.BQ_DEV_EXT_DATASET}.dicom_all` da
    ON s1.source_doi = da.Source_DOI
    GROUP BY ID, Title, Access, source_doi, source_url, Updated, license_url, license_long_name, license_short_name
    ORDER BY ID
    """

    results = {row['source_doi']:dict(row) for row in client.query(query)}
    return results
        
def get_tcia_sourced_analysis_metadata(BQ_client):
    tcia_ars = get_all_tcia_metadata('analysis-results')
    ar_metadata = {}
    for ar in tcia_ars:
        ar_metadata[ar['result_doi'].lower()] = dict(
            ID = ar['result_short_title'],
            Title = ar['result_title'],
            Access = ar['result_page_accessibility'],
            source_doi = ar['result_doi'].lower(),
            Updated = ar['date_updated'],
        )
    return ar_metadata

def build_metadata(args, BQ_client):
    tcia_analysis_metadata = get_tcia_sourced_analysis_metadata(BQ_client)
    idc_analysis_metadata = get_idc_sourced_analysis_metadata(BQ_client)
    all_analysis_results = idc_analysis_metadata | tcia_analysis_metadata

    analysis_results_metadata = get_analysis_results_metadata(BQ_client, all_analysis_results)
    # Get analysis results descriptions
    descriptions = get_descriptions(BQ_client, args)

    rows = []
    for source_doi, analysis_data in analysis_results_metadata.items():
        # analysis_data['source_url'] = f'https://doi.org/{analysis_data["source_doi"]}'
        analysis_data['Description'] = descriptions[analysis_data['ID']]
        analysis_data['Citation'] = get_citation(analysis_data['source_url'])
        rows.append(json.dumps(analysis_data))
    metadata = '\n'.join(rows)
    return metadata

def gen_collections_table(args):
    BQ_client = bigquery.Client(project=settings.DEV_PROJECT)
    metadata = build_metadata(args, BQ_client)
    try:
        job = load_BQ_from_json(BQ_client, settings.DEV_PROJECT, settings.BQ_DEV_EXT_DATASET, args.bqtable_name, metadata, analysis_results_metadata_schema,
            write_disposition='WRITE_TRUNCATE',
            table_description='Metadata of Analysis Results. These are the results of analysis performed against Original Collections hosted by IDC.')
    except Exception as exc:
        print(f'Error {exc}')
        exit
    while not job.state == 'DONE':
        progresslogger.info('Status: {}'.format(job.state))
        time.sleep(args.period * 60)
    successlogger.info(f"{time.asctime()}: Completed {args.bqtable_name}")

if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--bqtable_name', default='analysis_results_metadata', help='BQ table name')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    gen_collections_table(args)