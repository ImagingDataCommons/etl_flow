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

# Validate that the PDP DICOM store has the expected instances
import argparse
import json

import requests
import settings
from utilities.logging_config import successlogger, progresslogger, errlogger
import google
from google.cloud import storage, bigquery
from google.auth.transport import requests
from multiprocessing import  Queue, Process

def get_idc_series_data(args):
    client = bigquery.Client()
    query = f"""
    SELECT aj.collection_id, study_instance_uid StudyInstanceUID,  series_instance_uid SeriesInstanceUID, ARRAY_AGG(DISTINCT sop_instance_uid) SOPInstanceUIDs
    FROM `idc-dev-etl.idc_v14_dev.all_joined` aj
    JOIN `idc-dev-etl.idc_v14_dev.all_collections` ac
    ON aj.idc_collection_id=ac.idc_collection_id
    GROUP BY aj.collection_id, study_instance_uid, series_instance_uid, idc_version, i_source, pub_gcs_tcia_url, pub_gcs_idc_url, st_uuid, tcia_metadata_sunset, idc_metadata_sunset, se_uuid
    HAVING idc_version = {args.version} # AND se_uuid LIKE '0%'
    AND ((i_source='tcia' AND pub_gcs_tcia_url='public-datasets-idc') OR (i_source='idc' AND  pub_gcs_idc_url='public-datasets-idc'))
    AND (
        (i_source='tcia' AND (tcia_metadata_sunset=0 OR ({args.version} <= tcia_metadata_sunset))) OR 
        (i_source='idc' AND  (idc_metadata_sunset=0  OR ({args.version} <= idc_metadata_sunset)))
    )
    """

    result = client.query(query)
    series = [dict(row) for row in result]
    return series

def get_idc_study_data(args):
    client = bigquery.Client()
    query = f"""
    SELECT aj.collection_id, study_instance_uid StudyInstanceUID, ARRAY_AGG(DISTINCT series_instance_uid) SeriesInstanceUIDs
    FROM `idc-dev-etl.idc_v14_dev.all_joined` aj
    JOIN `idc-dev-etl.idc_v14_dev.all_collections` ac
    ON aj.idc_collection_id=ac.idc_collection_id
    GROUP BY aj.collection_id, study_instance_uid, idc_version, i_source, pub_gcs_tcia_url, pub_gcs_idc_url, st_uuid, tcia_metadata_sunset, idc_metadata_sunset
    HAVING idc_version = {args.version}
    AND ((i_source='tcia' AND pub_gcs_tcia_url='public-datasets-idc') OR (i_source='idc' AND  pub_gcs_idc_url='public-datasets-idc'))
    AND (
        (i_source='tcia' AND (tcia_metadata_sunset=0 OR ({args.version} <= tcia_metadata_sunset))) OR 
        (i_source='idc' AND  (idc_metadata_sunset=0  OR ({args.version} <= idc_metadata_sunset)))
    )
    """

    result = client.query(query)
    series = [dict(row) for row in result]
    return series


def get_instances_in_series(args, dicomweb_session, collection_id, study_instance_uid, series_instance_uid):
    # URL to the Cloud Healthcare API endpoint and version
    base_url = "https://healthcare.googleapis.com/v1"
    url = "{}/projects/{}/locations/{}".format(base_url, args.dst_project, args.dataset_region)
    # Set the required application/dicom+json; charset=utf-8 header on the request
    headers = {"Content-Type": "application/dicom+json; charset=utf-8"}

    # response.raise_for_status()
    instances = set()
    offset = 0
    limit = 1000
    while True:
        dicomweb_path = f"{url}/datasets/{args.gch_dataset_name}/dicomStores/{args.dicomstore}/dicomWeb/studies/{study_instance_uid}/series/{series_instance_uid}/instances?offset={offset}&limit={limit}"
        response = dicomweb_session.get(dicomweb_path, headers=headers)
        if response.status_code == 200:
            new_instances = set([row['00080018']['Value'][0] for row in response.json()])
            if new_instances:
                instances = instances.union(new_instances)
                if len(new_instances) == limit:
                    offset += limit
                    continue
            return 0, instances
        elif response.status_code == 204:
            return 0, instances
        else:
            errlogger.error(f'Series {collection_id}/{study_instance_uid}/{series_instance_uid}: not found in DICOM store ')
            instances = set([])
            return -1, instances


def get_series_in_study(args, dicomweb_session, collection_id, study_instance_uid):
    # URL to the Cloud Healthcare API endpoint and version
    base_url = "https://healthcare.googleapis.com/v1"
    url = "{}/projects/{}/locations/{}".format(base_url, args.dst_project, args.dataset_region)
    dicomweb_path = "{}/datasets/{}/dicomStores/{}/dicomWeb/studies/{}/series".format(
        url, args.gch_dataset_name, args.dicomstore, study_instance_uid
    )
    # Set the required application/dicom+json; charset=utf-8 header on the request
    headers = {"Content-Type": "application/dicom+json; charset=utf-8"}

    response = dicomweb_session.get(dicomweb_path, headers=headers)
    # response.raise_for_status()
    if response.status_code == 200:
        series = set([row['0020000E']['Value'][0] for row in response.json()])
        return 0, series
    else:
        errlogger.error(f'Study {collection_id}/{study_instance_uid}: not found in DICOM store ')
        series = set([])
        return -1, series

def validate_series(args, dicomweb_sess, row, n):
    error, instances = get_instances_in_series(args, dicomweb_sess, row['collection_id'], row['StudyInstanceUID'], row['SeriesInstanceUID'])
    if not error:
        if set(row['SOPInstanceUIDs']) == instances:
            successlogger.info(f"{row['SeriesInstanceUID']}")
            progresslogger.info(f"p{args.id} {n} {row['SeriesInstanceUID']}")
        else:
            errlogger.error(f"p{args.id} Series {row['collection_id']}/{row['StudyInstanceUID']}/{row['SeriesInstanceUID']}: instance mismatch ")
            errlogger.error(f'p{args.id}   Missing from IDC: {len(instances-set(row["SOPInstanceUIDs"]))} of {len(instances)}')
            # for instance in instances - set(row['SOPInstanceUIDs']):
            #     errlogger.error(f'\t{instance}')
            errlogger.error(f'p{args.id}   Missing from PDP: {len(set(row["SOPInstanceUIDs"])-instances)} of {len(set(row["SOPInstanceUIDs"]))}')
            # for instance in set(row['SOPInstanceUIDs']) - instances:
            #     errlogger.error(f'\t{instance}')


def worker(input, args, scoped_credentials):
    dicomweb_sess = requests.AuthorizedSession(scoped_credentials)

    for row, n in iter(input.get, 'STOP'):
        validate_series(args, dicomweb_sess, row, n)

def validate_dicom_store(args):
    dones = set(open(f'{successlogger.handlers[0].baseFilename}').read().splitlines())

    scoped_credentials, project = google.auth.default(
        ["https://www.googleapis.com/auth/cloud-platform"]
    )
    # Creates a requests Session object with the credentials.
    dicomweb_sess = requests.AuthorizedSession(scoped_credentials)

    # study_data = get_idc_study_data(args)
    # n=0
    # for row in study_data:
    #     if not row['StudyInstanceUID'] in dones:
    #         error, series = get_series_in_study(args, dicomweb_sess, row['collection_id'], row['StudyInstanceUID'])
    #         if not error:
    #             if set(row['SeriesInstanceUIDs']) == series:
    #                 successlogger.info(f"{row['StudyInstanceUID']}")
    #             else:
    #                 errlogger.error(f"Study {row['StudyInstanceUID']}: series mismatch ")
    #         n += 1

    try:
        series_data = json.load(open('series_data'))
    except:
        series_data = get_idc_series_data(args)
        json.dump(series_data, open('series_data', 'w'))

    num_processes = args.processes
    processes = []
    # Create a pair of queue for each process

    task_queue = Queue()

    # Start worker processes
    for process in range(num_processes):
        args.id = process + 1
        processes.append(
            Process(group=None, target=worker, args=(task_queue, args, scoped_credentials)))
        processes[-1].start()

    # Distribute the work across the task_queues
    n = 0
    for row in series_data:
        if not row['SeriesInstanceUID'] in dones:
            task_queue.put((row, n))
        # print(f'Queued {n}:{n+args.batch-1}')
        n += 1
    print('Primary work distribution complete; {} blobs'.format(n))

    # Tell child processes to stop
    for i in range(num_processes):
        task_queue.put('STOP')

    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=12, help='Version to work on')
    parser.add_argument('--client', default=storage.Client())
    args = parser.parse_args()
    parser.add_argument('--dst_project', default='chc-tcia')
    parser.add_argument('--dataset_region', default='us-central1')
    parser.add_argument('--gch_dataset_name', default='idc')
    parser.add_argument('--dicomstore', default=f'idc-store')
    parser.add_argument('--processes', default=1)
    # parser.add_argument('--dicomstore', default=f'v{args.version}')
    args = parser.parse_args()

    validate_dicom_store(args)

