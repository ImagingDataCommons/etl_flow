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

# Validate that redacted blobs were deleted from both dev (idc-dev-etl) and public bucket
import argparse
from multiprocessing import Process, Queue
from google.cloud import storage, bigquery
import settings

from utilities.logging_config import successlogger, progresslogger, errlogger



def get_redactions(version):
    client = bigquery.Client()
    query = f"""
    SELECT DISTINCT CONCAT(pub_gcs_bucket, '/', se_uuid, '/', i_uuid, '.dcm') instance_id
    FROM `{settings.DEV_MITIGATION_PROJECT}.m{settings.MITIGATION_VERSION}.redactions`
    """

    try:
        results = [dict(row) for row in client.query(query).result()]
    except Exception as exc:
        errlogger.error(f'Error querying redactions table: {exc} ')
        exit(-1)

    return results

def worker(input):
    RETRIES=3
    client = storage.Client()
    for instances, n in iter(input.get, 'STOP'):
        # Delete the blob from the public GCS bucket
        for instance in instances:
            # blob_name = f'{instance["se_uuid"]}/{instance["i_uuid"]}.dcm'
            # bucket_name = instance['pub_gcs_bucket']
            bucket_name = instance.split('/',1)[0]
            blob_name = instance.split('/',1)[1]
            bucket = client.bucket(bucket_name)
            if bucket.blob(blob_name).exists():
                bucket.blob(blob_name).delete()
                successlogger.info(f'{bucket_name}/{blob_name}')
            else:
                successlogger.info(f'{bucket_name}/{blob_name}')
                progresslogger.info(f'{bucket_name}/{blob_name} not found')

def delete_redactions(args):
    client = storage.Client()
    # Get list of previously deleted blobs
    dones = set(open(f'{successlogger.handlers[0].baseFilename}').read().splitlines())
    instances = get_redactions(args)
    undone_instances = list(set([instance["instance_id"] for instance in instances]) - set(dones))



    num_processes = args.processes
    processes = []
    task_queue = Queue()
    for process in range(num_processes):
        args.id = process + 1
        processes.append(
            Process(group=None, target=worker, args=(task_queue,)))
        processes[-1].start()

    # iterator = client.list_blobs(bucket, page_token=page_token, max_results=args.batch)
    batch_size = 1000
    n = 0
    while undone_instances:
        batch = undone_instances[:batch_size]
        undone_instances = undone_instances[batch_size:]
        task_queue.put((batch, n))
        n += len(batch)


    # Tell child processes to stop
    for i in range(num_processes):
        task_queue.put('STOP')
    # Wait for process to terminate
    for process in processes:
        print(f'Joining process: {process.name}, {process.is_alive()}')
        process.join()

    # Verify that all series are done
    # Get the completed series
    dones = open(f'{progresslogger.handlers[0].baseFilename}').read().splitlines()

    # undone_instances = list(set(found_series) - set(dones))
    # undone_series.sort()
    # if undone_series:
    #     errlogger.error("Some series not done:")
    #     for series in undone_series:
    #         print(series)




if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()
    parser.add_argument('--processes', default=1)
    args = parser.parse_args()

    delete_redactions(args)


