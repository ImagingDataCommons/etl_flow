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

# This script zips each series in a pre-staging bucket and copies the zip to a prestaging bucket in idc-archive
# according to the data type...Open, Cr, etc.
import argparse
import json

import pandas
import logging
from utilities.logging_config import successlogger, progresslogger, errlogger, warninglogger
from multiprocessing import Queue, Process, Lock, Condition, shared_memory
import settings
import sys
from google.cloud import storage, bigquery, exceptions
from google.api_core.exceptions import Conflict

import zlib
from stream_zip import stream_zip, NO_COMPRESSION_64, ZIP_64
from stream_unzip import stream_unzip
from datetime import datetime
from stat import S_IFREG
import time
from pandas import Series
import crc32c
import base64


# Get a list of the UUIDs of the series which have not been zipped and copied to GCS.
def get_undone_series(args: object) -> object:
    client = bigquery.Client()


    query = f"""
        SELECT DISTINCT se_uuid, SUM(i_size) se_size, pub_gcs_bucket src_bucket, CONCAT(dev_bucket, '-prestaging') dst_bucket
        FROM `idc-dev-etl.idc_v{args.version}_dev.all_joined_public_and_current` ajpac
        GROUP BY se_uuid, dev_bucket, pub_gcs_bucket, se_rev_idc_version
        HAVING se_rev_idc_version = {args.version}
        ORDER BY src_bucket, se_uuid
    """

    done_series = open(successlogger.handlers[0].baseFilename).read().splitlines()
    all_series = client.query(query).to_dataframe()
    done_uuids = Series(done_series, name='se_uuid')
    undone_series = pandas.merge(all_series, done_uuids, how='left', on="se_uuid", indicator='_merge').query('_merge == "left_only"')
    undone_series = undone_series.drop(columns='_merge')
    return(list(all_series['se_uuid'].unique()), undone_series)


def validate_zip(args, se_uuid, src_bucket, dst_bucket):
    chunk_size = pow(2,26)
    start_time = time.time()
    blobs = {}
    for blob in src_bucket.list_blobs(prefix=se_uuid):
        blobs[blob.name] = blob.crc32c
    def zipped_chunks():
        with dst_bucket.blob(f'{se_uuid}.zip').open(mode="rb") as archive:
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
        errlogger.error(f'Validation failure on {se_uuid}')
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
        series_index, se_uuid, src_bucket_name, dst_bucket_name, se_size = more_args
        try:
            if src_bucket_name != prev_src_bucket_name:
                src_bucket = client.bucket(src_bucket_name)
                dst_bucket = client.bucket(dst_bucket_name)
                prev_src_bucket_name = src_bucket_name
            start_time = time.time()
            gen_zip_stream(se_uuid, src_bucket, dst_bucket)
            elapsed_time = time.time() - start_time
            start_time = time.time()
            rate = round(se_size / elapsed_time / 10 ** 6, 1)
            if validate_zip(args, se_uuid, src_bucket, dst_bucket) == 0:
                progresslogger.info(
                    f'p{args.pid:03}:    {series_index}:{se_uuid}, {round(se_size / pow(10, 6), 2)}MB, {rate}MB/s, zip time:{round(elapsed_time, 2)}s, val time:{round((time.time()-start_time), 2)}s')
                successlogger.info(se_uuid)

        except Exception as exc:
            errlogger.error(f'zip{args.pid:03}:    zip{args.pid}: {se_uuid}: {exc}')
    return


def main(args):
    dst_client = storage.Client(args.dst_project)

    # Clean up any lingering files from previous executions
    # all_series is a list of all series uuids
    # undones is a dataframe of metadata of the series which have not yet been zipped
    all_series, undones = get_undone_series(args)

    # Zip generation is performed by a pipeline of process
    # Create a queue, one slot for each process
    zip_queue = Queue(args.num_processes)

    args.pid = 0

    try:
        zip_processes = []
        for process in range(args.num_processes):
            args.pid = process + 1
            zip_processes.append(Process(target=zip_worker, args=(
                zip_queue, args)))
            zip_processes[-1].start()

        try:
            with open(f'{settings.LOG_DIR}/totals') as f:
                totals = json.load(f)
                total_gb = totals['gb']
                total_series = totals['series']
                total_instances = totals['instances']
                total_elapsed_time = totals['elapsed_time']

        except:
            totals = {}
            total_gb = 0
            total_series = 0
            total_instances = 0
            total_elapsed_time = 0

        for src_bucket in undones['src_bucket'].unique():
            src_bucket_undones = undones[undones['src_bucket'] == src_bucket]

            dst_bucket_name = src_bucket_undones.iloc[0]['dst_bucket']

            # Try to create the destination bucket
            new_bucket = dst_client.bucket(dst_bucket_name)
            new_bucket.iam_configuration.uniform_bucket_level_access_enabled = True
            new_bucket.versioning_enabled = False
            try:
                result = dst_client.create_bucket(new_bucket, location='US-CENTRAL1', project=settings.DEV_PROJECT)
                # return(0)
            except Conflict:
                # Bucket exists
                pass
            except Exception as e:
                # Bucket creation failed somehow
                errlogger.error("Error creating bucket %s: %s", dst_bucket_name, e)
                return (-1)

            init_index = all_series.index(src_bucket_undones.iloc[0].se_uuid)
            final_index = all_series.index(src_bucket_undones.iloc[len(src_bucket_undones)-1].se_uuid)
            # bucket_size = round(src_bucket_undones['i_size'].sum()/pow(10,9),1)
            bucket_size = round(src_bucket_undones['se_size'].sum()/pow(10,9),1)
            series_count = len(src_bucket_undones['se_uuid'].unique())
            instance_count = len(src_bucket_undones)
            doilogger.info(f'p0:     Processing {src_bucket}, {bucket_size}GB, {series_count} series, {instance_count} instances, {init_index}:{final_index}')
            start_time = time.time()
            # for _, series in src_bucket_undones.iterrows():
            for se_uuid in src_bucket_undones['se_uuid'].unique():
                series_data = src_bucket_undones[src_bucket_undones['se_uuid'] == se_uuid]
                # series_size = series_data['i_size'].sum()
                series_size = series_data.at[series_data.index[0],'se_size']
                series_index = f'{init_index}:{all_series.index(se_uuid)}:{final_index}:{len(all_series)}'
                zip_queue.put((series_index, se_uuid, src_bucket, dst_bucket_name, series_size))

            elapsed_time = time.time() - start_time
            total_gb += bucket_size
            total_series += series_count
            total_instances += instance_count
            total_elapsed_time += elapsed_time
            doilogger.info(f'p0:     Completed {src_bucket} in {elapsed_time}s, {bucket_size} GB, {pow(10,3)*bucket_size/elapsed_time }MB/s, {series_count/elapsed_time} series/s, {1000*instance_count/elapsed_time} instances/s')
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
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='Version to work on')
    parser.add_argument('--num_processes', default=1)
    parser.add_argument('--dst_project', default='idc-archive', help='Project of the dst_bucket')
    args = parser.parse_args()

    print("{}".format(args), file=sys.stdout)

    doilogger = logging.getLogger('root.series')
    doilogger.setLevel(logging.INFO)
    for hdlr in doilogger.handlers[:]:
        doilogger.removeHandler(hdlr)
    doi_fh = logging.FileHandler('{}/doi.log'.format(settings.LOG_DIR))
    doilogger.addHandler(doi_fh)
    successformatter = logging.Formatter('%(message)s')
    doi_fh.setFormatter(successformatter)

    main(args)


