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

def TCIA_API_request(endpoint, parameters="", nbia_server=True):
    server_url = NBIA_URL if nbia_server else TCIA_URL
    url = f'{server_url}/{endpoint}?{parameters}'
    results = get_url(url)
    results.raise_for_status()
    return results.json()


def TCIA_API_request_to_file(filename, endpoint, parameters="", nbia_server=True):
    server_url = NBIA_URL if nbia_server else TCIA_URL
    url = f'{server_url}/{endpoint}?{parameters}'
    begin = time.time()
    results = get_url(url)
    results.raise_for_status()
    with open(filename, 'wb') as f:
        f.write(results.content)
    duration = str(datetime.timedelta(seconds=(time.time() - begin)))
    logging.debug('File %s downloaded in %s',filename, duration)
    return 0


def get_collections(nbia_server=True):
    server_url = NBIA_URL if nbia_server else TCIA_URL
    url = f'{server_url}/getCollectionValues'
    results = get_url(url)
    collections = results.json()
    # collections = [collection['Collection'].replace(' ', '_') for collection in results.json()]
    return collections


def get_TCIA_instance(seriesInstanceUID, sopInstanceUID, nbia_server=True):
    server_url = NBIA_URL if nbia_server else TCIA_URL
    url = f'{server_url}/getSingleImage?SeriesInstanceUID={seriesInstanceUID}&SOPInstanceUID={sopInstanceUID}'
    results = get_url(url)
    instances = results.json()
    return instances


def get_TCIA_series(nbia_server=True):
    results = TCIA_API_request('getSeries', nbia_server)
    # We only need a few values
    # We create a revision date field, filled with today's date (UTC +0), until TCIA returns a revision date
    # in the response to getSeries
    today = datetime.date.today().isoformat()
    data = [{'CollectionID': result['Collection'],
             'StudyInstanceUID': result['StudyInstanceUID'],
             'SeriesInstanceUID': result['SeriesInstanceUID'],
             "SeriesInstanceUID_RevisionDate": today}
            for result in results]

    return data

# def get_TCIA_instances_per_series(dicom, series_instance_uid, nbia_server=True):
#     # Get a zip of the instances in this series to a file and unzip it
#     # result = TCIA_API_request_to_file("{}/{}.zip".format(dicom, series_instance_uid),
#     #             "getImage", parameters="SeriesInstanceUID={}".format(series_instance_uid),
#     #             nbia_server=nbia_server)
#     server_url = NBIA_URL if nbia_server else TCIA_URL
#     url = f'{server_url}/{"getImage"}?SeriesInstanceUID={series_instance_uid}'
#
#     # _bytes=0
#     begin = time.time()
#     with open("{}/{}.zip".format(dicom, series_instance_uid), 'wb') as f:
#         r = session().get(url, stream=True, timeout=TIMEOUT)
#         for chunk in r.iter_content(chunk_size=None):
#             if chunk:
#                 f.write(chunk)
#                 f.flush()
#                 # _bytes += len(chunk)
#     # elapsed = time.time() - begin
#     # print(f'{_bytes} in {elapsed}s: {_bytes/elapsed}B/s; CHUNK_SIZE: {CHUNK_SIZE}')
#     # Now try to extract the instances to a directory DICOM/<series_instance_uid>
#     try:
#         with zipfile.ZipFile("{}/{}.zip".format(dicom, series_instance_uid)) as zip_ref:
#             zip_ref.extractall("{}/{}".format(dicom, series_instance_uid))
#         return
#     except :
#         logging.error("\tZip extract failed for series %s with error %s,%s,%s ", series_instance_uid,
#                       sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
#         raise


def get_collection_sizes_in_bytes(nbia_server=True):
    sizes = {}
    collections = get_collections(nbia_server)
    collections.sort(reverse=True)
    for collection in collections:
        sizes[collection] = get_collection_size(collection)
    return sizes


def get_collection_sizes(nbia_server=True):
    collections = get_collections(nbia_server)
    counts = {collection:0 for collection in collections}
    serieses=TCIA_API_request('getSeries', nbia_server)
    for aseries in serieses:
        counts[aseries['Collection']] += int(aseries['ImageCount'])
    sorted_counts = [(k, v) for k, v in sorted(counts.items(), key=lambda item: item[1])]
    return sorted_counts


def get_series_info(storage_client, project, bucket_name):
    series_info = {}
    blobs = storage_client.bucket(bucket_name, user_project=project).list_blobs()
    series_info = {blob.name.rsplit('.dcm',1)[0]: {"md5_hash":blob.md5_hash, "size":blob.size} for blob in blobs}
    return series_info

