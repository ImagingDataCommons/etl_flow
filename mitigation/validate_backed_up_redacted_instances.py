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

"""
Validate that a bucket holds the correct set of instance blobs
"""
import settings
import builtins
import json
# Noramlly the progresslogger file is trunacated. The following causes it to be appended.
# builtins.APPEND_PROGRESSLOGGER = True
from utilities.logging_config import successlogger, progresslogger, errlogger
from google.cloud import storage, bigquery
from multiprocessing import Process, Queue
import argparse


# Get all the blobs that are expected to be in the public bucket

def get_expected_series_in_bucket(args):
    client = bigquery.Client()
    query = f"""
        SELECT distinct se_uuid 
        FROM `{settings.DEV_MITIGATION_PROJECT}.m{settings.MITIGATION_VERSION}.redactions`
        ORDER BY se_uuid
    """

    query_job = client.query(query)
    series = set(query_job.result().to_dataframe()['se_uuid'].to_list())
    return series


def get_expected_blobs_in_bucket(args):
    client = bigquery.Client()
    query = f"""
        SELECT distinct concat(se_uuid,'/', i_uuid, '.dcm') as blob_name
        FROM `{settings.DEV_MITIGATION_PROJECT}.m{settings.MITIGATION_VERSION}.redactions`
        ORDER BY blob_name
    """

    query_job = client.query(query)
    blob_names = set(query_job.result().to_dataframe()['blob_name'].to_list())
    return blob_names

def worker(input, args):
    RETRIES=3
    client = storage.Client()
    bucket = client.bucket(args.bucket_name)
    for series_uuids, n in iter(input.get, 'STOP'):
            for series_uuid in series_uuids:
                try:
                    instance_iterator = client.list_blobs(bucket, versions=False, page_size=args.batch, \
                                     prefix=series_uuid)
                    for page in instance_iterator.pages:
                        for blob in page:
                            successlogger.info(blob.name)
                    progresslogger.info(series_uuid)
                except Exception as exc:
                    errlogger.error(f'p{args.id}: Error {exc}')

def get_found_series_in_bucket(args):
    client = storage.Client()
    parser.add_argument('--bucket', default='idc-dev-cr')
    bucket = client.bucket(args.bucket_name)

    try:
        # Assume we've already got the list of expected series
        with open(f"{settings.LOG_DIR}/found_series.json") as f:
            found_series = json.load(f)
    except:
        # Get found series
        iterator = client.list_blobs(bucket, delimiter='/', page_size=args.batch)
        found_series = []
        for page in iterator.pages:
            # if page.num_items:
            if len(page.prefixes):
                series = [aseries.split('/')[0] for aseries in page.prefixes]
                found_series.extend(series)
        with open(f"{settings.LOG_DIR}/found_series.json", "w") as f:
            json.dump(found_series, f)

    return set(found_series)


def get_found_blobs_in_bucket(args, found_series):
    client = storage.Client()
    bucket = client.bucket(args.bucket_name)
    progresslogger.info(f'Finding blobs in bucket')

    done_series = open(f'{progresslogger.handlers[0].baseFilename}').read().splitlines()

    undone_series = list(set(found_series) - set(done_series))
    undone_series.sort()

    # Start worker processes
    num_processes = args.processes
    processes = []
    task_queue = Queue()
    for process in range(num_processes):
        args.id = process + 1
        processes.append(
            Process(group=None, target=worker, args=(task_queue, args)))
        processes[-1].start()

    # iterator = client.list_blobs(bucket, page_token=page_token, max_results=args.batch)
    batch_size = 100
    n = 0
    with open(args.found_blobs, 'w') as f:
        while undone_series:
            batch = undone_series[:batch_size]
            undone_series = undone_series[batch_size:]
            task_queue.put((batch, n))


    # Tell child processes to stop
    for i in range(num_processes):
        task_queue.put('STOP')
        task_queue.put('STOP')
    # Wait for process to terminate
    for process in processes:
        print(f'Joining process: {process.name}, {process.is_alive()}')
        process.join()

    # Verify that all series are done
    # Get the completed series
    done_series = open(f'{progresslogger.handlers[0].baseFilename}').read().splitlines()

    undone_series = list(set(found_series) - set(done_series))
    undone_series.sort()
    if undone_series:
        errlogger.error("Some series not done:")
        for series in undone_series:
            print(series)

    return



def check_all_instances_mp(args=settings.CURRENT_VERSION):

    found_series = get_found_series_in_bucket(args)
    expected_series = get_expected_series_in_bucket(args)

    if found_series == expected_series:
        progresslogger.info(f"Bucket {args.bucket_name} has the correct set of series")
    else:
        errlogger.error(f"Bucket {args.bucket_name} does not have the correct set of series")
        unexpected_series = list(found_series - expected_series)
        unfound_series = list(expected_series - found_series)
        # Release memory
        del found_series
        del expected_series
        if unexpected_series:
            unexpected_series.sort()
            errlogger.error(f"Unexpected series in bucket: {len(unexpected_series)}")
            for series in unexpected_series:
                errlogger.error(series)
            with open(f"{settings.LOG_DIR}/unexpected_series.json", "w") as f:
                json.dump(unexpected_series, f)
        if unfound_series:
            unfound_series.sort()
            errlogger.error(f"Expected series not found in bucket: {len(unfound_series)}")
            for series in unfound_series:
                errlogger.error(series)
            with open(f"{settings.LOG_DIR}/unfound_series.json") as f:
                json.dump(unfound_series, f)

    found_blobs = set(open(args.found_blobs).read().splitlines())
    if not found_blobs or args.find_blobs:
        progresslogger.info(f'Getting found blobs')
        get_found_blobs_in_bucket(args, found_series)
        found_blobs = set(open(args.found_blobs).read().splitlines())
    else:
        progresslogger.info(f'Already have found blobs')

    found_blobs = set(open(args.found_blobs).read().splitlines())
    if args.find_blobs:
        progresslogger.info(f'Getting found blobs')
        get_found_blobs_in_bucket(args, found_series)
        found_blobs = set(open(args.found_blobs).read().splitlines())
    else:
        try:
            found_blobs = set(open(args.found_blobs).read().splitlines())
        except:
            progresslogger.info(f'Getting found blobs')
            get_found_blobs_in_bucket(args, found_series)
            found_blobs = set(open(args.found_blobs).read().splitlines())

    expected_blobs = get_expected_blobs_in_bucket(args)

    if found_blobs == expected_blobs:
        successlogger.info(f"Bucket {args.bucket_name} has the correct set of blobs")
    else:
        errlogger.error(f"Bucket {args.bucket_name} does not have the correct set of blobs")
        unexpected_blobs = list(found_blobs - expected_blobs)
        unfound_blobs = list(expected_blobs - found_blobs)
        # Release memory
        del found_blobs
        del expected_blobs
        if unexpected_blobs:
            unexpected_blobs.sort()
            errlogger.error(f"Unexpected blobs in bucket: {len(unexpected_blobs)}")
            for blob in unexpected_blobs:
                errlogger.error(blob)
            with open(f"{settings.LOG_DIR}/unexpected_blobs.json", "w") as f:
                json.dump(unexpected_blobs, f)
        if unfound_blobs:
            unfound_blobs.sort()
            errlogger.error(f"Expected blobs not found in bucket: {len(unfound_blobs)}")
            for blob in unfound_blobs:
                errlogger.error(blob)
            with open(f"{settings.LOG_DIR}/unfound_blobs.json", 'w') as f:
                json.dump(unfound_blobs, f)

    return


def get_bucket_names():
    client = bigquery.Client()

    query = f"""
    SELECT DISTINCT pub_aws_bucket bucket_name
    FROM `{settings.DEV_MITIGATION_PROJECT}.m{settings.MITIGATION_VERSION}.redactions`
    """

    try:
        results = [dict(row) for row in client.query(query).result()]
    except Exception as exc:
        errlogger.error(f'Error querying redactions table: {exc} ')
        exit(-1)

    return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # parser.add_argument('--version', default=f'{settings.CURRENT_VERSION}')
    parser.add_argument('--version', default=settings.CURRENT_VERSION)
    parser.add_argument('--processes', default=1)
    parser.add_argument('--expected_blobs', default=f'{settings.LOG_DIR}/expected_blobs.txt', help='List of blobs names expected to be in above collections')
    parser.add_argument('--found_blobs', default=f'{settings.LOG_DIR}/success.log', help='List of blobs names found in bucket')
    parser.add_argument('--batch', default=1000, help='Size of batch assigned to each process')
    parser.add_argument('--log_dir', default=f'/mnt/disks/idc-etl/logs/validate_open_buckets')
    parser.add_argument('--bucket_name', default=f'm{settings.MITIGATION_VERSION}_redacted_instances', help='Bucket to which to backup redacted instances')
    parser.add_argument("--find_blobs", default=False, action="store_true", help="Always find blobs in bucket anew")
    args = parser.parse_args()
    print(f'args: {json.dumps(args.__dict__, indent=2)}')

    check_all_instances_mp(args)

