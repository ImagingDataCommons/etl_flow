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

import requests
import json
import re
import argparse
from operator import itemgetter
from utilities.logging_config import successlogger, progresslogger, errlogger
from google.cloud import storage
from base64 import b64decode
from multiprocessing import Process, Queue

# url = "https://api.gdc.cancer.gov/files"
# filters = {
#     "op": "and",
#     "content": [
#         # {"op": "in", "content": {"field": "file_type", "value": ["svs"]}}
#         # ,
#         # {"op": "in", "content": {"field": "data_category", "value": ["image"]}}
#         {"op": "in", "content": {"field": "experimental_strategy", "value": ["Tissue Slide"]}}
#     ]
# }
# params = {
#     "filters": json.dumps(filters),
#     "fields": "file_id,file_name,access",
#     "format": "json",
#     "size": 1000 # Adjust size as needed
# }
#
# response = requests.get(url, params=params)
# if response.status_code == 200:
#     results = response.json()
#     print(f"Found {len(results['data']['hits'])} .svs files.")
#     # You can now process results['data']['hits'] to get file_ids
# else:
#     print(f"Error: {response.status_code}")


# Get a list of the projects that have an "experimental strategy" == "Tissue Slide"
def get_projects():
    # First we get a list of projects
    url = "https://api.gdc.cancer.gov/projects"
    filters = {}
    params = {
        "filters": json.dumps(filters),
        # "fields": "project_id, releasable",
        "format": "json",
        "size": 1000  # Adjust size as needed
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        projects = {}
        all_projects = response.json()["data"]["hits"]
        progresslogger.info(f"Found {len(all_projects)} projects.")
        # You can now process results['data']['hits'] to get file_ids
    else:
        errlogger.error(f"Error: {response.status_code}")
        exit(1)
    return all_projects

def get_pathology_projects(all_projects):
    # Get a list of the projects which have "experimental_strategy"=="Tissue Slide"
    pathology_projects = []
    for project in all_projects:
        url = f'https://api.gdc.cancer.gov/projects/{project["project_id"]}'

        params = {
            "expand": "summary,summary.experimental_strategies",
            # "filters": json.dumps(filters),
            # "fields": "project_id, releasable",
            "format": "json",
            "size": 1000  # Adjust size as needed
        }
        response = requests.get(url, params=params)
        if response.status_code == 200:
            summary = response.json()
            if experimental_strategy := next((experimental_strategy for experimental_strategy in summary['data']['summary']['experimental_strategies'] if experimental_strategy['experimental_strategy'] == 'Tissue Slide'), False):
                summary['data']['experimental_strategy'] = experimental_strategy
                pathology_projects.append(summary)
                progresslogger.info(f'Project {summary["data"]["project_id"]} has {experimental_strategy["file_count"]} files in {experimental_strategy["case_count"]} cases')

            # print(f"Found {len(projects['data']['hits'])} projects.")
            # You can now process results['data']['hits'] to get file_ids
        else:
            errlogger.error(f"Error: {response.status_code}")
            exit(1)
    return sorted(pathology_projects, key=lambda project: project['data']['project_id'])


# Get a list of per-file metadata fpr tissue slide files in a specified project
def get_pathology_files(project_id):
    url = f'https://api.gdc.cancer.gov/files'
    filters = {
        "op": "and",
        "content": [
            {
                "op": "=",
                "content": {
                    "field": "files.experimental_strategy",
                    "value": ["Tissue Slide"]
                }
            },
            {
                "op": "=",
                "content": {
                    "field": "cases.project.project_id",
                    "value": [project_id]
                }
            }
        ]
    }
    params = {
        "expand": "summary,summary.experimental_strategies",
        "filters": json.dumps(filters),
        # "fields": "project_id, releasable",
        "format": "json",
        "from": 0,
        "size": 100000  # Adjust size as needed
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        pagination = response.json()['data']['pagination']
        files = response.json()['data']['hits']
        if pagination['count'] != pagination['total']:
            print("There's more")
    return files





def copy_file_to_gcs(client, file, content, dst_bucket, gdc_version):
    blob = dst_bucket.blob(f'{gdc_version}/{file["file_name"]}')
    result = blob.upload_from_string(content)
    try:
        assert file["md5sum"] == b64decode(blob.md5_hash).hex()
        successlogger.info(f'{file["file_name"]}')
        progresslogger.info(f'p{args.id}  {file["file_id"]}')
    except Exception as exc:
        errlogger.error(f'Incorrect or no hash for {gdc_version}/{file["file_name"]}; {exc}')
        # Need to delete the new blob
        blob.delete()
    return


def download_file(args, file):
    MAXTRIES = 3
    _try = 1

    file_id = file["file_id"]
    data_endpt = "https://api.gdc.cancer.gov/data/{}".format(file_id)

    while _try <= MAXTRIES:
        try:
            response = requests.get(data_endpt, headers={"Content-Type": "application/json"})
            return response.content
        except Exception as exc:
            progresslogger.info(f'p{args.id}  Download failure {_try}: {file["file_id"]}')
            _try += 1
    errlogger.error(f'p{args.id}  Download failure: {file["file_id"]}')
    return None


def worker(input, args, dst_bucket_name):
    try:
        client = storage.Client()
        dst_bucket = client.bucket(bucket_name=dst_bucket_name)
    except Exception as exc:
        errlogger.error(f'p{args.id}:  {exc}')
        exit(2)
    for file, n  in iter (input.get, 'STOP'):
        content = download_file(args, file)
        if content:
            # gdc_versions = get_gdc_version_of_file(file)
            # gdc_version = gdc_versions[-1]['data_release']
            history = file['history']
            gdc_version = max(history, key=lambda version: version['data_release'])['data_release']
            copy_file_to_gcs(client, file, content, dst_bucket, gdc_version)


def get_new_files(client, new_files, dst_bucket):
    num_processes = args.processes
    processes = []
    task_queue = Queue()
    # Start worker processes
    for process in range(num_processes):
        args.id = process + 1
        processes.append(
            Process(group=None, target=worker, args=(task_queue, args, dst_bucket.name)))
        processes[-1].start()

    # Distribute the work across the task_queues
    n = 0
    for file in new_files:
        task_queue.put((file, n))
        n += 1
    print('Primary work distribution complete; {} blobs'.format(n))

    # Tell child processes to stop
    for i in range(num_processes):
        task_queue.put('STOP')


    # Wait for process to terminate
    for process in processes:
        print(f'Joining process: {process.name}, {process.is_alive()}')
        process.join()

    return


def download_files_to_dst_bucket(client, project_id, files, dst_bucket):
    existing_blobs = {blob.name.rsplit('/',1)[-1]: blob  for blob in dst_bucket.list_blobs() if not blob.name.endswith('manifest.json')}
    files_needing_to_be_downloaded = []
    for file in files:
        try:
            blob = existing_blobs[file["file_name"]]
            blob_md5_hash = b64decode(blob.md5_hash).hex()
            if blob_md5_hash == file['md5sum']:
                progresslogger.debug(f'Found current version of {file["file_name"]}')
                continue
            else:
                progresslogger.info(f'New version of {file["file_name"]}')
                pass
        except Exception as exc:
            progresslogger.info(f'\t\tNew file: {file["file_name"]}')
        files_needing_to_be_downloaded.append(file)

    if files_needing_to_be_downloaded:
        get_new_files(client, files_needing_to_be_downloaded, dst_bucket)

    return


def get_gdc_version_of_file(file):
    MAXTRIES = 3
    _try = 1

    url = f'https://api.gdc.cancer.gov/history/{file["file_id"]}'
    params = {
        "format": "json"
    }

    while _try <= MAXTRIES:
        try:
            response = requests.get(url, params=params)
            history = response.json()
            return history
        except Exception as exc:
            progresslogger.info(f'p{args.id}  Failure to get gdc version  {_try}: {file["file_id"]}')
            _try += 1
    errlogger.error(f'p{args.id}  Failed to get gdc version of: {file["file_id"]}')
    exit(2)


def download_files(project_id, files):
    client = storage.Client()
    bucket_name = f'{project_id.lower().replace("-", "_")}_pathology_data{args.dst_bucket_suffix}'
    dst_bucket = client.bucket(bucket_name)
    new_files = []
    max_version = "1.0"
    # Accept any files whose GDC version is later than some threshold version
    for file in files:
        history = get_gdc_version_of_file(file)
        gdc_version = max(history, key=lambda version: version['data_release'])['data_release']
        file['history'] = history
        if gdc_version >= args.min_gdc_version or not args.min_gdc_version:
            if file['access'] == 'open':
                new_files.append(file)
            else:
                progresslogger.info(f'File {file["file_name"]} not accessible')
        else:
            progresslogger.info(f'File {file["file_name"]} most recent release prior to release threshold')
        max_version = max(max_version, gdc_version)

    if new_files:
        try:
            dst_bucket.create(project='idc-source-data', location="us-central1")
            progresslogger.info(f'Created bucket {dst_bucket.name}')
        except Exception as exc:
            # dst_bucket already exists
            pass
        progresslogger.info(f'Downloading {len(new_files)} files to bucket {dst_bucket.name}')
        download_files_to_dst_bucket(client, project_id, new_files, dst_bucket)


    else:
        progresslogger.info(f'No new files in {project_id}')

    progresslogger.info(f'Latest release for {project_id}: {max_version}')
    if max_version >= args.min_gdc_version or not args.min_gdc_version:
        blob = dst_bucket.blob(f'{max_version}/manifest.json')
        result = blob.upload_from_string(json.dumps(files))
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--processes', default=8)
    parser.add_argument('--min_gdc_version', default = "37.0", help="Skip files from previous versions")
    parser.add_argument("--ignore_dones", default=False, help="If True, process project even if previously processed")
    parser.add_argument("--projects", default=["CGCI-BLGSP"], help="List of projects to process. Process all procents if empty")
    parser.add_argument("--dst_bucket_suffix", default='_dev', help='Suffix added to destination bucket name. Mostly for development work to avoid writing to default dest bucket')
    args = parser.parse_args()
    args.id = 0 # Default process ID
    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    dones = set(open(successlogger.handlers[0].baseFilename).read().splitlines())

    # project_id = 'CDDP_EAGLE-1'
    # files = get_pathology_files(project_id)
    # download_files(project_id, files)
    all_projects = get_projects()
    pathology_projects = get_pathology_projects(all_projects)
    for project in pathology_projects:
        project_id = project["data"]["project_id"]
        if args.projects==[] or project_id in args.projects:
            # We already have TCGA pathology
            if not args.ignore_dones and project_id in dones:
                progresslogger.info(f'Project {project_id} previously completed')
            else:
                args.min_gdc_version = "37.0" if project_id.startswith('TCGA') else "1.0"
                progresslogger.info(f'Processing project {project_id}')
                files = get_pathology_files(project_id)
                download_files(project_id, files)
                successlogger.info(project_id)