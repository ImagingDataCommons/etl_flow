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
import json
import time
from google.cloud import bigquery
from utilities.bq_helpers import load_BQ_from_json
from bq.gen_original_data_collections_table.schema import data_collections_metadata_schema
from utilities.tcia_helpers import get_collection_descriptions_and_licenses, get_collection_license_info
from utilities.tcia_scrapers import scrape_tcia_data_collections_page

from python_settings import settings

import psycopg2
from psycopg2.extras import DictCursor, RealDictCursor

def get_collections_programs(client, args):
    query = f"""
        SELECT * 
        FROM `{args.src_project}.{args.bqdataset_name}.{args.bq_program_table}`"""
    programs = {row['tcia_wiki_collection_id']: row['program'] for row in client.query(query).result()}

    return programs
    # programs = {collection: program for cur.fetchall()

def get_collections_in_version(client, args):
    if args.gen_excluded:
        # Only excluded collections
        query = f"""
        SELECT c.* 
        FROM `{args.src_project}.{args.bqdataset_name}.{args.bq_collection_table}` as c
        JOIN `{args.src_project}.{args.bqdataset_name}.{args.bq_excluded_collections}` as ex
        ON LOWER(c.collection_id) = LOWER(ex.tcia_api_collection_id)
        ORDER BY c.collection_id
        """
    else:
        # Only included collections
        query = f"""
        SELECT c.* 
        FROM `{args.src_project}.{args.bqdataset_name}.{args.bq_collection_table}` as c
        LEFT JOIN `{args.src_project}.{args.bqdataset_name}.{args.bq_excluded_collections}` as ex
        ON LOWER(c.collection_id) = LOWER(ex.tcia_api_collection_id)
        WHERE ex.tcia_api_collection_id IS NULL
        ORDER BY c.collection_id
        """
    result = client.query(query).result()
    collection_ids = [collection['collection_id'] for collection in result]
    return collection_ids

def get_cases_per_collection(client, args):
    query = f"""
    SELECT
      c.collection_id,
      COUNT(DISTINCT p.submitter_case_id ),
    FROM
      `{args.src_project}.{args.bqdataset_name}.collection` as c
    JOIN
      `{args.src_project}.{args.bqdataset_name}.patient` as p 
    ON c.collection_id = p.collection_id
    GROUP BY
        c.collection_id
    """

    case_counts = {c.values()[0].lower(): c.values()[1] for c in client.query(query).result()}
    return case_counts

def get_access_status(client, args):
    query = f"""
    SELECT o.tcia_api_collection_id, o.access
    FROM `{args.src_project}.{args.bqdataset_name}.open_collections` o
    UNION ALL
    SELECT cr.tcia_api_collection_id, cr .access
    FROM `{args.src_project}.{args.bqdataset_name}.cr_collections` cr
    UNION ALL
    SELECT r.tcia_api_collection_id, r.access
    FROM `{args.src_project}.{args.bqdataset_name}.redacted_collections` r
    UNION ALL
    SELECT ex.tcia_api_collection_id, ex.access
    FROM `{args.src_project}.{args.bqdataset_name}.excluded_collections` ex
    UNION ALL
    SELECT de.tcia_api_collection_id, de.access
    FROM `{args.src_project}.{args.bqdataset_name}.defaced_collections` de
    """

    access_status = {c.values()[0]: c.values()[1] for c in client.query(query).result()}
    return access_status

def build_metadata(client, args, idc_collection_ids, programs):
    # Get collection descriptions and license IDs from TCIA
    collection_descriptions = get_collection_descriptions_and_licenses()

    # We report our case count rather than counts from the TCIA wiki pages.
    case_counts = get_cases_per_collection(client, args)

    # Get the access status of each collection
    access_status = get_access_status(client,args)

    # Get a list of the licenses used by data collections
    licenses = get_collection_license_info()

    # try:
    #     # a=1/0
    #     collection_metadata = json.load(open('collection_metadata.txt'))
    # except:
    #     # Scrape the TCIA Data Collections page for collection metadata
    #     collection_metadata = scrape_tcia_data_collections_page()
    #     with open('collection_metadata.txt', 'w') as f:
    #         json.dump(collection_metadata, f)
    collection_metadata = scrape_tcia_data_collections_page()

    rows = []
    found_ids = []
    # lowered_idc_collection_ids = {collection_id.lower():collection_id for collection_id in collection_ids}
    lowered_collection_description_ids = {collection_id.lower():collection_id for collection_id in collection_descriptions}
    lowered_license_ids = {collection_id.lower():collection_id for collection_id in licenses}
    lowered_collection_metadata_ids = {collection_id.lower():collection_id for collection_id in collection_metadata}

    # for collection_id, collection_data in collection_metadata.items():
    #     if collection_id.lower() in lowered_collection_ids:
    for idc_collection_id in idc_collection_ids:
        if idc_collection_id.lower() in lowered_collection_metadata_ids:
            try:
                tcia_collection_id = lowered_collection_metadata_ids[idc_collection_id.lower()]
                found_ids.append(idc_collection_id)
                collection_data = collection_metadata[tcia_collection_id]
                collection_data['tcia_api_collection_id'] = idc_collection_id
                collection_data['idc_webapp_collection_id'] = collection_data['tcia_api_collection_id'].lower().replace(' ','_').replace('-','_')
                collection_data['Program'] = programs[collection_data['tcia_wiki_collection_id']]
                collection_data['Access'] = access_status[collection_data['tcia_api_collection_id']]
                # if collection_id.lower() in lowered_collection_description_ids:
                # mapped_collection_id = lowered_collection_ids[collection_id.lower()]
                try:
                    collection_description_id = lowered_collection_description_ids[idc_collection_id.lower()]
                    collection_data['Description'] = collection_descriptions[collection_description_id]['description']
                except:
                    collection_data['Description'] = ""
                collection_data['Subjects'] = case_counts[idc_collection_id.lower()]
                # mapped_license_id = lowered_license_ids[collection_id.lower()]
                try:
                    license_id = lowered_license_ids[idc_collection_id.lower()]
                    collection_data['license_url'] = licenses[license_id]['licenseURL']
                    collection_data['license_long_name'] = licenses[license_id]['longName']
                    collection_data['license_short_name'] = licenses[license_id]['shortName']
                except:
                    collection_data['license_url'] = ''
                    collection_data['license_long_name'] = ''
                    collection_data['license_short_name'] = ''
            except Exception as exc:
                print(f'Exception building metadata {exc}')

            if idc_collection_id == 'NLST':
                collection_data["Description"] = \
"""<p>
   The National Lung Screening Trial (NLST) was a randomized controlled clinical trial of screening tests for lung cancer. Approximately 54,000 participants were enrolled between August 2002 and April 2004. Participants were randomly assigned to two study arms in equal proportions. One arm received low-dose helical computed tomography (CT), while the other received single-view chest radiography. Participants were offered three exams (T0, T1, and T2) at one-year intervals, with the first (T0) performed soon after entry. The goal of the study was to assess whether low-dose CT screening reduces lung cancer mortality relative to chest radiography among high-risk individuals. Data were collected on cancer diagnoses and deaths that occurred through December 31, 2009. Median follow-up time was 6.5 years. The <a href="http://ClinicalTrials.gov" class="external-link" rel="nofollow">ClinicalTrials.gov</a> number for NLST is NCT00047385.
</p>
<p>
   NLST was a collaborative effort of the National Cancer Institute's Division of Cancer Prevention (DCP) and Division of Cancer Treatment and Diagnosis (DCTD). DCP funded and administered the Lung Screening Study network (LSS, 10 centers, ~35,000 participants) while DCTD funded and administered the American College of Radiology Imaging Network (ACRIN, 23 centers, ~19,000 participants). The study protocols and data collection forms used by LSS and ACRIN were harmonized. A trial-wide database of harmonized data elements was built during the trial for reporting to the Data and Safety Monitoring Board (DSMB). Some additional data elements were collected only by ACRIN or by LSS, as described in the final section of this document.</p>
<p>
    Please see the <a href="https://wiki.cancerimagingarchive.net/display/NLST/National+Lung+Screening+Trial" target="_blank">NLST</a> wiki page to learn more about the images and to obtain any supporting metadata for this collection.
</p>"""

            rows.append(json.dumps(collection_data))
        else:
            print(f'{idc_collection_id} not in collection metadata')

    # Make sure we found metadata for all our collections
    for idc_collection in idc_collection_ids:
        if not idc_collection in found_ids:
            print(f'****No metadata for {idc_collection}')
            if idc_collection == 'APOLLO':
                collection_data = {
                    "tcia_wiki_collection_id": "APOLLO-1-VA",
                    "DOI": "",
                    "CancerType": "Non-small Cell Lung Cancer",
                    "Location": "Lung",
                    "Species": "Human",
                    "Subjects": 7,
                    "ImageTypes": "CT, PT",
                    "SupportingData": "",
                    "Access": "Public",
                    "Status": "Complete",
                    "Updated": "2018-03-08",
                    "tcia_api_collection_id": "APOLLO",
                    "idc_webapp_collection_id": "apollo",
                    "Program": "APOLLO",
                    "Description": \
"""<p>	This data collection consists of images and associated data acquired from the <a class=""external-link"" href=""https://proteomics.cancer.gov/programs/apollo-network""> APOLLO Network</a> and may be subject to usage restrictions.</p>
<p>	The <strong><em>A</em></strong>pplied <strong><em>P</em></strong>roteogenomics <strong><em>O</em></strong>rganizationa<strong><em>L</em></strong> <strong><em>L</em></strong>earning and <strong><em>O</em></strong>utcomes (APOLLO) network is a collaboration between NCI, the Department of Defense (DoD), and the Department of Veterans Affairs (VA) to incorporate proteogenomics into patient care as a way of looking beyond the genome, to the activity and expression of the proteins that the genome encodes. The emerging field of proteogenomics aims to better predict how patients will respond to therapy by screening their tumors for both genetic abnormalities and protein information, an approach that has been made possible in recent years due to advances in proteomic technology.</p>
<p>	Please see the <a href=""https://wiki.cancerimagingarchive.net/display/Public/APOLLO-1-VA"">APOLLO</a> wiki page to learn more.</p>""",
                    "license_url": "https://proteomics.cancer.gov/data-portal",
                    "license_long_name": "APOLLO Network Data Use Agreement",
                    "license_short_name": "APOLLO"}
                # collection_data["Description"] = collection_descriptions['APOLLO']['description']
                rows.append(json.dumps(collection_data))

    metadata = '\n'.join(rows)
    return metadata

def gen_collections_table(args):
    BQ_client = bigquery.Client(project=args.src_project)
    programs = get_collections_programs(BQ_client, args)
    collection_ids = get_collections_in_version(BQ_client, args)

    metadata = build_metadata(BQ_client, args, collection_ids, programs)
    job = load_BQ_from_json(BQ_client, args.dst_project, args.bqdataset_name, args.bqtable_name, metadata,
                            data_collections_metadata_schema, write_disposition='WRITE_TRUNCATE')
    while not job.state == 'DONE':
        print('Status: {}'.format(job.state))
        time.sleep(args.period * 60)
    print("{}: Completed collections metatdata upload \n".format(time.asctime()))

