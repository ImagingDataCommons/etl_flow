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

# Get the collection ID that TCIA/NBIA APIs accept
def get_collection_id(doi):
    if doi.startswith('http'):
        URL = doi
    else:
        URL = f'https://doi.org/{doi}'
    page = get_url(URL)

    soup = BeautifulSoup(page.content, "html.parser")

    links = soup.find_all('a', class_='external-link')
    collection_id = ""
    for link in links:
        if 'CollectionCriteria' in link.get('href'):
            collection_id = link.get('href').rsplit('CollectionCriteria=')[-1].replace('%20',' ').split('&')[0]
            break
    return collection_id

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
                trow['DOI'] = trow['DOI'].split('doi.org/doi:')[1].strip()
            except:
                try:
                    trow['DOI'] = trow['DOI'].split('doi.org/')[1].strip()
                except:
                    # Probably an http: URL not a DOI.
                    trow['DOI'] = trow['DOI'].replace('http', 'https').strip()
                    pass
            collection = get_collection_id(trow['DOI'])
            if collection == "":
                collection = trow['Collection']
            trow.pop('Collection')
            table[collection] = trow

    return table


if __name__ == "__main__":
    m =scrape_tcia_data_collections_page()
    s = get_collection_id('https://wiki.cancerimagingarchive.net/x/N4NyAQ')
