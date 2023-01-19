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
from utilities.tcia_helpers import get_license_info

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
    elif doi.startswith(('//')):
        URL = f'https:{doi}'
    else:
        URL = f'https://doi.org/{doi}'
    try:
        page = get_url(URL)
    except Exception as exc:
        print(f'Exception in get_collection_id: {exc}')

    soup = BeautifulSoup(page.content, "html.parser")

    links = soup.find_all('a', class_='external-link')
    collection_id = ""
    for link in links:
        if 'CollectionCriteria' in link.get('href'):
            collection_id = link.get('href').rsplit('CollectionCriteria=')[-1].replace('%20',' ').split('&')[0]
            break
    return collection_id


def get_license_from_wiki(doi):

    # Get a list of the licenses used by collections
    licenses = get_license_info()

    # Get the wiki page for some collection/analysis result
    URL = f'https://doi.org/{doi}'
    page = get_url(URL)

    soup = BeautifulSoup(page.content, "html.parser")

    for link in soup.find_all('div'):
        if link.get('name') == "Citations BITVOODOO_ANDamp; Data Usage Policy":
            for att in link.find('p').find_all('a'):
                if att.text != 'TCIA Data Usage Policy' and att.text != 'TCIA Data Usage Policy and Restrictions':
                    licenseURL = att.get('href')
                    longName = att.text
                    # We have to the shortname from the licenses list. It's not in the page.
                    license = next((item for item in licenses if item['longName'] == longName), None)
                    shortName = license["shortName"] if license else ""
                    return (licenseURL, longName, shortName)
    # License info not found in the Citations tab. Look in the Data Access tab
    for link in soup.find_all('div'):
        if link.get('name') == "Data Access":
            for att in link.find('p').find_all('a'):
                if att.text == 'TCIA Restricted License Agreement':
                    licenseURL = att.get('href')
                    longName = att.text
                    shortName = 'TCIA'
                    return (licenseURL, longName, shortName)

    return ("", "")


def scrape_tcia_analysis_collections_page():
    URL = 'https://www.cancerimagingarchive.net/tcia-analysis-results/'
    page = get_url(URL)

    soup = BeautifulSoup(page.content, "html.parser")

    table = soup.find(id="tablepress-10")

    # print(table.prettify())

    rows = table.find_all("tr")

    table = {}
    header = "Collection,DOI,CancerType,Location,Subjects,Collections,AnalysisArtifactsonTCIA,Updated,LicenseURL,LicenseName".split(",")

    for row in rows:
        trow = {}
        cols = row.find_all("td")
        for cid, col in enumerate(cols):
            if cid == 0:
                try:
                    trow[header[0]] = col.find("a").text
                except Exception as exc:
                    print('Empty row in TCIA Analysis Results table')
                    break
                trow[header[1]] = col.find("a")["href"]
            else:
                trow[header[cid + 1]] = col.text
        if len(trow):
            # Strip off the http server prefix
            trow['DOI'] = trow['DOI'].split('doi.org/')[1].lower()
            collection = trow.pop('Collection')
            table[collection] = trow
            # table = table + [trow]


    # print(tabulate(table, headers=header))

    return table

def scrape_tcia_data_collections_page():
    URL = 'http://www.cancerimagingarchive.net/collections/'
    page = get_url(URL)

    soup = BeautifulSoup(page.content, "html.parser")

    table = soup.find(id="tablepress-9")

    # print(table.prettify())

    rows = table.find_all("tr")

    table = {}
    header = "tcia_wiki_collection_id,DOI,CancerType,Location,Species,Subjects,ImageTypes,SupportingData,Access,Status,Updated".split(
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
                trow['DOI'] = trow['DOI'].split('https://doi.org/https://doi.org/')[1].strip().lower()
            except:
                try:
                    trow['DOI'] = trow['DOI'].split('doi.org/doi:')[1].strip().lower()
                except:
                    try:
                        trow['DOI'] = trow['DOI'].split('doi.org/')[1].strip().lower()
                    except:
                        # Probably an http: URL not a DOI.
                        if trow['DOI'].lower().startswith('http:'):
                            trow['DOI'] = trow['DOI'].replace('http', 'https').strip().lower()
                        # trow['DOI'] = trow['DOI'].replace('http', 'https').strip()
                        pass
            collection = get_collection_id(trow['DOI'])
            if collection == "":
                if trow['tcia_wiki_collection_id'] == 'National Lung Screening Trial (NLST)':
                    collection = 'NLST'
                    trow['DOI'] = '10.7937/TCIA.hmq8-j677'.lower()
                    trow['Access'] = 'Public'

                elif trow['tcia_wiki_collection_id'] == 'ACRIN 6698/I-SPY2 Breast DWI (ACRIN 6698)':
                    collection = 'ACRIN 6698'
                    # trow['DOI'] = '10.7937/tcia.kk02-6d95'
                    # trow['Access'] = 'Public'
                else:
                    collection = trow['tcia_wiki_collection_id']
            # print(f'{collection}')
            table[collection] = trow

    return table


if __name__ == "__main__":
    # m =scrape_tcia_data_collections_page()
    # s = get_collection_id('https://wiki.cancerimagingarchive.net/x/N4NyAQ')
    # url, longName, shortName = get_license_from_wiki('10.7937/tcia.2019.of2w8lxr')
    table = scrape_tcia_analysis_collections_page()
    table = scrape_tcia_data_collections_page()
    pass
