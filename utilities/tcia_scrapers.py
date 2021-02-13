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

import difflib

import requests
from bs4 import BeautifulSoup
import backoff

@backoff.on_exception(backoff.expo,
                      requests.exceptions.RequestException,
                      max_time=60)
def get_url(url):  # , headers):
    return requests.get(url)  # , headers=headers)

def scrape_tcia_analysis_collections_page():
    URL = 'https://www.cancerimagingarchive.net/tcia-analysis-results/'
    page = get_url(URL)

    soup = BeautifulSoup(page.content, "html.parser")

    table = soup.find(id="tablepress-10")

    # print(table.prettify())

    rows = table.find_all("tr")

    table = {}
    header = "Collection,DOI,CancerType,Location,Subjects,Collections,AnalysisArtifactsonTCIA,Updated".split(",")

    for row in rows:
        trow = {}
        cols = row.find_all("td")
        for cid, col in enumerate(cols):
            if cid == 0:
                trow[header[0]] = col.find("a").text
                trow[header[1]] = col.find("a")["href"]
            else:
                trow[header[cid + 1]] = col.text
        if len(trow):
            # Strip off the http server prefix
            trow['DOI'] = trow['DOI'].split('doi.org/')[1]

            collection = trow.pop('Collection')
            table[collection] = trow
            # table = table + [trow]

    # print(tabulate(table, headers=header))

    print(len(rows))

    # with open("output/analysis_collections.json", "w") as f:
    #     f.write(json.dumps(table, indent=2))

    return table

def scrape_tcia_data_collections_page():
    URL = 'http://www.cancerimagingarchive.net/collections/'
    page = get_url(URL)

    soup = BeautifulSoup(page.content, "html.parser")

    table = soup.find(id="tablepress-9")

    # print(table.prettify())

    rows = table.find_all("tr")

    table = {}
    header = "Collection,DOI,CancerType,Location,Species,Subjects,ImageTypes,SupportingData,Access,Status,Updated".split(
        ",")

    for row in rows:
        trow = {}
        cols = row.find_all("td")
        for cid, col in enumerate(cols):
            if cid == 0:
                trow[header[0]] = col.find("a").text
                trow[header[1]] = col.find("a")["href"]
                if not trow[header[1]].startswith("http"):
                    trow[header[1]] = "http:" + col.find("a")["href"]
            else:
                trow[header[cid + 1]] = col.text
        if len(trow):
            # Strip off the http server prefix
            try:
                trow['DOI'] = trow['DOI'].split('doi.org/')[1]
                collection = trow.pop('Collection')
                table[collection] = trow
            except:
                # Some collections do not have doi.org DOIs
                collection = trow.pop('Collection')
                table[collection] = trow


    return table



def build_TCIA_to_Description_ID_Table(collections, descriptions):
    '''
    Build a table that maps collections ids from scraped TCIA collection data to collection ids in NBIA collection
    descriptions. The mapping is empirical.
    collections is a dictionary of TCIA metadata, indexeded by collection name scraped from TCIA collections page
    descriptions is a dictionary of NBIA collection names indexed by collection name
    '''

    table = {}
    # Create a table of normalized to original description ids
    description_ids = {id.lower().replace(' ', '-').replace('_', '-'):id for id, data in descriptions.items()}

    for id,data in collections.items():
        if data['Access'] == 'Public' and data['ImageTypes'] != 'Pathology':
            best_guess = difflib.get_close_matches(id.split('(')[0].lower().replace(' ','-').replace('_','-'),
                                                                   list(description_ids.keys()), 1, 0.5)
            if len(best_guess) > 0:
                table[id] = description_ids[best_guess[0]]

    # Yuch!!. Do some fix ups
    table["AAPM RT-MAC Grand Challenge 2019"] = "AAPM-RT-MAC"
    if "CT-ORG" in table:
        table.pop("CT-ORG")
    table["APOLLO-1-VA"] = "APOLLO"
    # table["Medical Imaging Data Resource Center - RSNA International COVID Radiology Database Release 1a - Chest CT Covid+"] = \
    #     "MIDRC-RICORD-1a"

    return table

if __name__ == "__main__":
    collections_pages = scrape_tcia_data_collections_page()
    pass