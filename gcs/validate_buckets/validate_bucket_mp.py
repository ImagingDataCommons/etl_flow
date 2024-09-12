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


# Get all the blobs that are expected to be in the public bucket
# Limited to series having a rev_idc_version LTE max_version

def get_expected_blobs_in_bucket(args, max_version, premerge=False):
    client = bigquery.Client()
    query = f"""
      SELECT distinct concat(se_uuid,'/', i_uuid, '.dcm') as blob_name
      FROM `idc-dev-etl.idc_v{args.version}_dev.all_joined_public` aj
      WHERE se_rev_idc_version <= {max_version}
      AND ((i_source='tcia' and {"dev" if args.dev_or_pub=="dev" else "pub_gcs"}_tcia_url="{args.bucket}")
      OR (i_source='idc' and {"dev" if args.dev_or_pub=="dev" else "pub_gcs"}_idc_url="{args.bucket}"))

      ORDER BY blob_name
  """

    query_job = client.query(query)
    blob_names = set(query_job.result().to_dataframe()['blob_name'].to_list())
    return blob_names

# def get_expected_blobs_in_bucket(args, max_version, premerge=False):
#     client = bigquery.Client()
#     query = f"""
#       SELECT distinct concat(se_uuid,'/', i_uuid, '.dcm') as blob_name
#       FROM `idc-dev-etl.idc_v{args.version}_dev.all_joined` aj
#       JOIN `idc-dev-etl.idc_v{args.version}_dev.all_collections` aic
#       ON aj.idc_collection_id = aic.idc_collection_id
#       WHERE ((i_source='tcia' and aic.{"dev" if args.dev_or_pub=="dev" else "pub_gcs"}_tcia_url="{args.bucket}")
#       OR (i_source='idc' and aic.{"dev" if args.dev_or_pub=="dev" else "pub_gcs"}_idc_url="{args.bucket}"))
#       AND i_excluded = False and i_redacted = False
#       AND if({premerge}, i_rev_idc_version < {args.version}, i_rev_idc_version <= {args.version})
#       AND se_rev_idc_version <= max_version
#       ORDER BY blob_name
#   """
#
#     query_job = client.query(query)
#     blob_names = set(query_job.result().to_dataframe()['blob_name'].to_list())
#     return blob_names



# def get_found_series_in_bucket(args):
#     client = bigquery.Client()
#     query = f"""
#       SELECT distinct concat(se_uuid,'/') as series_uuid
#       FROM `idc-dev-etl.idc_v{args.version}_dev.all_joined` aj
#       JOIN `idc-dev-etl.idc_v{args.version}_dev.all_collections` aic
#       ON aj.idc_collection_id = aic.idc_collection_id
#       WHERE ((i_source='tcia' and aic.{"dev" if args.dev_or_pub=="dev" else "pub_gcs"}_tcia_url="{args.bucket}")
#       OR (i_source='idc' and aic.{"dev" if args.dev_or_pub=="dev" else "pub_gcs"}_idc_url="{args.bucket}"))
#       AND i_excluded = False i_redacted = False
#       ORDER BY series_uuid
#   """
#
#     query_job = client.query(query)  # Make an API request.
#     series_uuids = [row.series_uuid for row in query_job.result()]
#
#     return series_uuids

def worker(input, args):
    RETRIES=3
    client = storage.Client()
    bucket = client.bucket(args.bucket)
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


def get_found_blobs_in_bucket(args):
    client = storage.Client()
    bucket = client.bucket(args.bucket)

    try:
        # Assume we've already got the list of expected series
        with open(f"{settings.LOG_DIR}/found_series.json") as f:
            found_series = json.load(f)
    except:
        # Get expect series
        iterator = client.list_blobs(bucket, delimiter='/', page_size=args.batch)
        found_series = []
        for page in iterator.pages:
            if page.num_items:
                series = [aseries.split('/')[0] for aseries in page.prefixes]
                found_series.extend(series)
        with open(f"{settings.LOG_DIR}/found_series.json", "w") as f:
            json.dump(found_series, f)

    # Get the completed series
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


def check_all_instances_mp(args, premerge=False, max_version=settings.CURRENT_VERSION):
    try:
        found_blobs = set(open(args.found_blobs).read().splitlines())
        assert len(found_blobs) > 0
        progresslogger.info(f'Already have found blobs')
    except:
        progresslogger.info(f'Getting found blobs')
        get_found_blobs_in_bucket(args)
        found_blobs = set(open(args.found_blobs).read().splitlines())

    expected_blobs = get_expected_blobs_in_bucket(args, max_version, premerge)


    if found_blobs == expected_blobs:
        successlogger.info(f"Bucket {args.bucket} has the correct set of blobs")
    else:
        errlogger.error(f"Bucket {args.bucket} does not have the correct set of blobs")
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
            with open(f"{settings.LOG_DIR}/unfound_blobs.json") as f:
                json.dump(unfound_blobs, f)

    return


