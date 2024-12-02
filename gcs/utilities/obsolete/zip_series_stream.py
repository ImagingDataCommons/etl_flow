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
import pandas
import logging
from utilities.logging_config import successlogger, progresslogger, errlogger, warninglogger
from multiprocessing import Queue, Process, Lock, Condition, shared_memory
import settings
import sys
from google.cloud import storage, bigquery, exceptions

from stream_zip import stream_zip, NO_COMPRESSION_64, ZIP_64
from datetime import datetime
from stat import S_IFREG
import time
from pandas import Series
import psutil





# Get a list of the UUIDs of the series which have not been zipped and copied to GCS.
def get_undone_series(args: object) -> object:
    client = bigquery.Client()
    query = f"""
    WITH series_data AS (
    SELECT DISTINCT collection_id, replace(replace(source_doi,'/','_'),'.','_')  source_doi, se_rev_idc_version, se_uuid, i_size, i_uuid, i_source, 
        if(i_source='tcia', tcia_access, idc_access) access,
        if(i_source='tcia', 
--             if(pub_gcs_tcia_url is NULL, dev_tcia_url, pub_gcs_tcia_url),
--             if(pub_gcs_idc_url is NULL, dev_idc_url, pub_gcs_idc_url)
            if(pub_gcs_tcia_url = 'public-datasets-idc', 'public-datasets-idc', dev_tcia_url),
            if(pub_gcs_idc_url = 'public-datasets-idc', 'public-datasets-idc', dev_idc_url)
            ) src_bucket
    FROM `idc-dev-etl.idc_v{args.version}_dev.all_joined`
    )
    SELECT collection_id, source_doi, access, se_rev_idc_version version, se_uuid, SUM(i_size) se_size, 
        COUNT(i_uuid) instances, src_bucket, 
        if(src_bucket='public-datasets-idc', 'idc-arch-open', replace(src_bucket, 'dev', 'arch')) dst_bucket
    FROM series_data
    GROUP BY collection_id, source_doi, access, version, se_uuid,  src_bucket
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



def gen_zip_stream(se_uuid, src_bucket, dst_bucket):
    # directory = pathlib.Path(src_directory)
    chunk_size = pow(2,25)
    def local_files(blobs):
        now = datetime.now()
        def contents(blob):
            with blob.open('rb') as f:
                # progresslogger.info(f'{name.parts[-2]}/{name.parts[-1]}')
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
    blobs = src_bucket.list_blobs(prefix=se_uuid)

    with dst_bucket.blob(f'{se_uuid}.zip').open(mode="wb", chunk_size=chunk_size) as archive:
        for chunk in stream_zip(local_files(blobs)):
            archive.write(chunk)


def zip_worker(zip_queue, args, freespace_condition, freespace_name):
    freespace = shared_memory.ShareableList(name=freespace_name)
    client = storage.Client()
    prev_src_bucket_name = ""

    for more_args in iter(zip_queue.get, 'STOP'):
        series_index, series, src_bucket_name, dst_bucket_name = more_args
        try:
            # progresslogger.info(
            #     f'zip{args.pid}:    Starting {series_index}:{series.source_doi}-v{series.version}/{series.se_uuid}')
            if src_bucket_name != prev_src_bucket_name:
                src_bucket = client.bucket(src_bucket_name)
                dst_bucket = client.bucket(dst_bucket_name)
                prev_src_bucket_name = src_bucket_name
            start_time = time.time()
            # src_directory = f'{src_mount_point}/{series.se_uuid}/'
            # zip_name = f'{dst_mount_point}/{series.se_uuid}.zip'
            gen_zip_stream(series.se_uuid, src_bucket, dst_bucket)
            with freespace_condition:
                freespace[0] += series.se_size
                freespace_now = freespace[0]
                warninglogger.warning(f'p{args.pid:03}:    free {series_index}:se_size:{round(series.se_size/pow(2,30),2)}GiB, freespace:{round(freespace_now/pow(2,30),2)}GiB, actual:{round(psutil.virtual_memory().free/pow(2,30),2)}GiB')
                freespace_condition.notify()
            elapsed_time = time.time() - start_time
            rate = round(series.se_size / elapsed_time / 10 ** 6, 1)
            progresslogger.info(f'p{args.pid:03}:    {series_index}:{series.se_uuid}, {rate}MB/s, {round(elapsed_time,2)}s, {round(freespace_now/pow(2,30),2)}GiB')
            successlogger.info(series.se_uuid)
        except Exception as exc:
            errlogger.error(f'zip{args.pid:03}:    zip{args.pid}: {series.se_uuid}: {exc}')
            with freespace_condition:
                freespace[0] += series.se_size
                freespace_condition.notify()

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
    freespace_condition = Condition()


    zip_processes = []
    freespace = shared_memory.ShareableList([0])
    for process in range(args.num_processes):
        args.pid = process + 1
        zip_processes.append(Process(target=zip_worker, args=(
            zip_queue, args, freespace_condition, freespace.shm.name)))
        zip_processes[-1].start()

    # Determine the initial free memory space
    with freespace_condition:
        freespace[0] = int(psutil.virtual_memory().free/2)
        initial_freespace = freespace[0]
    args.pid = 0

    try:
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
                        # Wait until there is room on disk for the next series
                        with freespace_condition:
                            while freespace[0] < series.se_size:
                                # warninglogger.warning(f'p0:    Waiting on freespace, freespace={freespace[0]/pow(2,30)}GiB, se_size:{series.se_size/pow(2,30)}GiB, actual: {psutil.virtual_memory().free/pow(2,30)}GiB')
                                warninglogger.warning(f'p000:    Waiting on freespace: se_size={round(series.se_size/pow(2,30),2)}GiB, freespace={round(freespace_now/pow(2,30),2)}GiB, actual: {round(psutil.virtual_memory().free/pow(2,30),2)}GiB')
                                freespace_condition.wait()
                            freespace[0] -= series.se_size
                            freespace_now = freespace[0]

                        warninglogger.warning(f'p000:    Queued se_size={round(series.se_size/pow(2,30),2)}GiB, freespace={round(freespace_now/pow(2,30),2)}GiB, actual: {round(psutil.virtual_memory().free/pow(2,30),2)}GiB')

                        series_index = f'{init_index}:{all_series.index(series.se_uuid)}:{final_index}:{len(all_series)}'
                        zip_queue.put((series_index, series, src_bucket, dst_bucket))
                    elapsed_time = time.time() - start_time
                    doilogger.info(f'p0:     Completed {collection_id}/{source_doi}/v{version} in {elapsed_time}s, {pow(10,3)*s_size/elapsed_time}MB/s, {s_count/elapsed_time} series/s, {1000*i_count/elapsed_time} instances/s')


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


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='Version to work on')
    parser.add_argument('--num_processes', default=1)
    parser.add_argument('--local_disk_dir', default='/mnt/disks/idc-etl/series_zips')
    parser.add_argument('--src_bucket', default='idc-open-data', help='Source bucket containing instances')
    parser.add_argument('--dst_project', default='idc-archive', help='Project of the dst_bucket')
    parser.add_argument('--src_mount_point', default='/mnt/disks/idc-etl/src_mount_point', help='Directory on which to mount the src bucket.\
                The script will create this directory if necessary.')
    parser.add_argument('--dst_mount_point', default='/mnt/disks/idc-etl/dst_mount_point', help='Directory on which to mount the dst bucket.\
                 The script will create this directory if necessary.')
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

    start_time = time.time()
    main(args)
    print("--- %s seconds ---" % (time.time() - start_time))
