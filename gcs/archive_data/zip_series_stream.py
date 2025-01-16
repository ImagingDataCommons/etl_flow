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

# Goal of this script was to zip instances in a series for all series in some specified bucket, copying resulting zip to
# a specified GCS bucket. Unworkable because stream_zip doesn't actually stream when not compressing.

import argparse
import json

import pandas
import logging
from utilities.logging_config import successlogger, progresslogger, errlogger, warninglogger
from multiprocessing import Queue, Process, Lock, Condition, shared_memory
import settings
import sys
from google.cloud import storage, bigquery, exceptions

import zlib
from stream_zip import stream_zip, NO_COMPRESSION_64, ZIP_64
from stream_unzip import stream_unzip
from datetime import datetime
from stat import S_IFREG
import time
from pandas import Series
import crc32c
import base64
import psutil
import os


# Get a list of the UUIDs of the series which have not been zipped and copied to GCS.
def get_undone_series(args: object) -> object:
    client = bigquery.Client()
    query = f"""
    WITH series_data AS (
    SELECT DISTINCT collection_id, replace(replace(source_doi,'/','_'),'.','_')  source_doi, se_rev_idc_version, se_uuid, i_size, i_uuid, i_source, 
        access,
        dev_bucket src_bucket
    FROM `idc-dev-etl.idc_v{args.version}_dev.all_joined`
    WHERE se_redacted = False
    )
    SELECT collection_id, source_doi, access, se_rev_idc_version version, se_uuid, SUM(i_size) se_size, 
        COUNT(i_uuid) instances, src_bucket, 
        REPLACE(src_bucket, 'dev', 'arch') dst_bucket
    FROM series_data
    GROUP BY collection_id, source_doi, access, version, se_uuid,  src_bucket
    having src_bucket = '{args.src_bucket}'
    ORDER by collection_id, source_doi, version, se_uuid """

    done_series = open(successlogger.handlers[0].baseFilename).read().splitlines()
    all_series = client.query(query).to_dataframe()
    done_uuids = Series(done_series, name='se_uuid')
    undone_series = pandas.merge(all_series, done_uuids, how='left', on="se_uuid", indicator='_merge').query('_merge == "left_only"')
    undone_series = undone_series.drop(columns='_merge')
    return(all_series['se_uuid'].to_list(), undone_series)


# Return a dataframe of the instance UUIDs in a series
def get_instances_in_series(args: object, se_uuid: str) -> object:
    client = bigquery.Client()
    query = f"""
    SELECT DISTINCT i.uuid
    FROM `idc-dev-etl.idc_v{args.version}_dev.instance` i
    JOIN `idc-dev-etl.idc_v{args.version}_dev.series_instance` s_i
    ON s_i.instance_uuid = i.uuid
    WHERE s_i.series_uuid = '{se_uuid}'
    ORDER by uuid"""

    undone_instances = client.query(query).to_dataframe()
    return(undone_instances)

def validate_zip(args, series, src_bucket, dst_bucket):
    chunk_size = pow(2,26)
    start_time = time.time()
    blobs = {}
    for blob in src_bucket.list_blobs(prefix=series.se_uuid):
        blobs[blob.name] = blob.crc32c
    def zipped_chunks():
        with dst_bucket.blob(f'{series.se_uuid}.zip').open(mode="rb") as archive:
            # yield from archive.read(chunk_size)
            while True:
                chunk = archive.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    unzipped_blobs ={}
    for file_name, file_size, unzipped_chunks in stream_unzip(zipped_chunks()):
        # unzipped_chunks must be iterated to completion or UnfinishedIterationError will be raised
        hash = crc32c.CRC32CHash()
        for chunk in unzipped_chunks:
            # print(f'p{args.pid}  Val: {psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2}')
            hash.update(chunk)
        unzipped_blobs[file_name.decode()] = base64.b64encode(hash.digest()).decode()
    try:
        assert set(blobs.keys()) == set(unzipped_blobs.keys())
        assert set(blobs.values()) == set(unzipped_blobs.values())
        elapsed_time = time.time() - start_time
        # progresslogger.info(
        #     f'p{args.pid:03}:      Validated {round(elapsed_time, 2)}s')
        return 0
    except Exception as exc:
        errlogger.error(f'Validation failure on {series.se_uuid}')
        return 1


def gen_zip_stream(se_uuid, src_bucket, dst_bucket):
    # directory = pathlib.Path(src_directory)
    chunk_size = pow(2,26)
    blobs = src_bucket.list_blobs(prefix=se_uuid)

    def local_files(blobs):
        now = datetime.now()
        def contents(blob):
            with blob.open('rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    yield chunk

        def return_value(blob):
            return (blob.name, now, S_IFREG | 0o600, ZIP_64, contents(blob))

        return (
            return_value(blob)
            for blob in blobs
        )

    get_compressobj = lambda: zlib.compressobj(wbits=-zlib.MAX_WBITS, level=0)
    with dst_bucket.blob(f'{se_uuid}.zip').open(mode="wb", chunk_size=chunk_size) as archive:
        for chunk in stream_zip(local_files(blobs), chunk_size=chunk_size, get_compressobj=get_compressobj):
            # print(f'p{args.pid}  Zip: {psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2}')
            archive.write(chunk)


def zip_worker(zip_queue, args):

    client = storage.Client()
    prev_src_bucket_name = ""

    for more_args in iter(zip_queue.get, 'STOP'):
        series_index, series, src_bucket_name, dst_bucket_name = more_args
        try:
            if src_bucket_name != prev_src_bucket_name:
                src_bucket = client.bucket(src_bucket_name)
                dst_bucket = client.bucket(dst_bucket_name)
                prev_src_bucket_name = src_bucket_name
            start_time = time.time()
            progresslogger.info(
                f'p{args.pid:03}:    Start: {series_index}:{series.se_uuid}, {round(series.se_size / pow(2, 20), 2)}MB')
            # src_directory = f'{src_mount_point}/{series.se_uuid}/'
            # zip_name = f'{dst_mount_point}/{series.se_uuid}.zip'
            gen_zip_stream(series.se_uuid, src_bucket, dst_bucket)
            elapsed_time = time.time() - start_time
            start_time = time.time()
            rate = round(series.se_size / elapsed_time / 10 ** 6, 1)
            if validate_zip(args, series, src_bucket, dst_bucket) == 0:
                progresslogger.info(
                    f'p{args.pid:03}:    {series_index}:{series.se_uuid}, {round(series.se_size / pow(2, 20), 2)}MB, {rate}MB/s, zip:{round(elapsed_time, 2)}s , val:{round((time.time()-start_time), 2)}')
                successlogger.info(series.se_uuid)

        except Exception as exc:
            errlogger.error(f'zip{args.pid:03}:    zip{args.pid}: {series.se_uuid}: {exc}')


    return


def main(args):
    dst_client = storage.Client(args.dst_project)
    # Clean up any lingering files from previous executions
    # all_series is a list of the all series uuids
    # undones is dataframe of metadata on the series which have not yet been zipped
    all_series, undones = get_undone_series(args)

    # Zip generation is performed by a pipeline of process
    # Create queues
    zip_queue = Queue(args.num_processes)



    args.pid = 0

    try:
        zip_processes = []
        for process in range(args.num_processes):
            args.pid = process + 1
            zip_processes.append(Process(target=zip_worker, args=(
                zip_queue, args)))
            zip_processes[-1].start()

        with open(f'{settings.LOG_DIR}/totals') as f:
            totals = json.load(f)

        total_gb = totals['gb']
        total_series = totals['series']
        total_instances = totals['instances']
        total_elapsed_time = totals['elapsed_time']
        for collection_id in undones['collection_id'].unique():
            # if collection_id != 'ACRIN-NSCLC-FDG-PET':
            #     continue
            collection_undones = undones[undones['collection_id'] == collection_id]
            for source_doi in collection_undones['source_doi'].unique():
                # if source_doi.find('zenodo') == -1:
                #     continue
                doi_undones = collection_undones[collection_undones['source_doi']==source_doi]
                public_access = doi_undones.iloc[0]['access'] == 'Public'
                for version in doi_undones['version'].unique():
                    version_undones = doi_undones[doi_undones['version']==version]

                    src_bucket = version_undones.iloc[0]['src_bucket']
                    dst_bucket = version_undones.iloc[0]['dst_bucket']
                    init_index = all_series.index(version_undones.iloc[0].se_uuid)
                    final_index = all_series.index(version_undones.iloc[len(version_undones)-1].se_uuid)
                    s_size = round(version_undones['se_size'].sum()/pow(10,9),1)
                    s_count = len(version_undones)
                    i_count = round(version_undones['instances'].sum()/pow(10,3),1)
                    doilogger.info(f'p0:     Processing {collection_id}/{source_doi}/v{version}, {s_size}GB, {s_count} series, {i_count}K instances, {init_index}:{final_index}')
                    start_time = time.time()
                    for _, series in version_undones.iterrows():
                        series_index = f'{init_index}:{all_series.index(series.se_uuid)}:{final_index}:{len(all_series)}'
                        zip_queue.put((series_index, series, src_bucket, dst_bucket))

                    elapsed_time = time.time() - start_time
                    total_gb += s_size
                    total_series += s_count
                    total_instances += i_count
                    total_elapsed_time += elapsed_time
                    doilogger.info(f'p0:     Completed {collection_id}/{source_doi}/v{version} in {elapsed_time}s, {s_size} GB, {pow(10,3)*s_size/elapsed_time }MB/s, {s_count/elapsed_time} series/s, {1000*i_count/elapsed_time} instances/s')
                    doilogger.info(f'p0:     Totals: {total_gb} GB, {total_series} series, {total_instances} K instances, {total_gb/total_elapsed_time} GB/s, {total_series/total_elapsed_time} series/s, {total_instances/total_elapsed_time}/K instances/s')
                    doilogger.info('')
                    totals['gb'] = total_gb
                    totals['series'] = total_series
                    totals['instances'] = total_instances
                    totals['elapsed_time'] = total_elapsed_time
                    with open(f'{settings.LOG_DIR}/totals', 'w') as f:
                        json.dump(totals, f)

        # Stop the processes
        try:
            for process in zip_processes:
                zip_queue.put('STOP')
                # Wait for them to stop
            for process in zip_processes:
                process.join()
        except Exception as exc:
            pass



    finally:
        for process in zip_processes:
            if process.is_alive():
                process.kill()
        totals['gb'] = total_gb
        totals['series'] = total_series
        totals['instances'] = total_instances
        totals['elapsed_time'] = total_elapsed_time
        with open(f'{settings.LOG_DIR}/totals', 'w') as f:
            json.dump(totals,f)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # parser.add_argument('--version', default=settings.CURRENT_VERSION, help='Version to work on')
    parser.add_argument('--version', default=20, help='Version to work on')
    parser.add_argument('--num_processes', default=64)
    parser.add_argument('--local_disk_dir', default='/mnt/disks/idc-etl/series_zips')
    parser.add_argument('--src_bucket', default='idc-dev-excluded', help='Source bucket containing instances')
    parser.add_argument('--dst_project', default='idc-archive', help='Project of the dst_bucket')
    # parser.add_argument('--src_mount_point', default='/mnt/disks/idc-etl/src_mount_point', help='Directory on which to mount the src bucket.\
    #             The script will create this directory if necessary.')
    # parser.add_argument('--dst_mount_point', default='/mnt/disks/idc-etl/idc-arch-open', help='Directory on which to mount the dst bucket.\
    #              The script will create this directory if necessary.')
    args = parser.parse_args()

    print("{}".format(args), file=sys.stdout)

    doilogger = logging.getLogger('root.series')
    doilogger.setLevel(logging.INFO)
    for hdlr in doilogger.handlers[:]:
        doilogger.removeHandler(hdlr)
    # #The series log file is usually truncated (the mode='w' does that.)
    # if not hasattr(builtins, "APPEND_seriesLOGGER") or builtins.APPEND_seriesLOGGER==False:
    #     success_fh = logging.FileHandler('{}/series.log'.format(settings.LOG_DIR), mode='w')
    # else:
    doi_fh = logging.FileHandler('{}/doi.log'.format(settings.LOG_DIR))
    doilogger.addHandler(doi_fh)
    successformatter = logging.Formatter('%(message)s')
    doi_fh.setFormatter(successformatter)

    main(args)


