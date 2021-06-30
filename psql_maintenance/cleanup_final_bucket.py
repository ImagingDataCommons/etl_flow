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

# One time use script to remove blobs from final bucket that have
# "hex" format (32 packed hex characters) names instead of tandard 8-4-4-4-12 format.

import sys
import os
import argparse
from google.cloud import storage,bigquery
from google.cloud.exceptions import NotFound
from logging import INFO
from multiprocessing import Process, Queue
from utilities.tcia_helpers import  get_TCIA_patients_per_collection, \
    get_collection_values_and_counts, get_TCIA_studies_per_collection, get_TCIA_series_per_collection
import logging
from python_settings import settings
import settings as etl_settings

import psycopg2
from psycopg2.extras import DictCursor


def worker(input, output, args):
    rootlogger.debug('p%s: Worker starting: args: %s', args.id, args)
    client = storage.Client()
    src_bucket = client.bucket(args.final_bucket)

    for instance_blobs in iter(input.get, 'STOP'):

        for instance in instance_blobs:
            try:
                src_bucket.delete_blob(instance)
                donelogger.info(instance)
            except NotFound:
                errlogger.error('%s: Instance %s not found in bucket',args.id,  instance)
                donelogger.info(instance)
            except Exception as exc:
                errlogger.error('%s: Instance %s error %s', args.id, instance, exc)
        rootlogger.info('p%s: Completed batch', args.id)


def cleanup_bucket(args, undones, psql_instances ):
    client = storage.Client()
    src_bucket = client.bucket(args.final_bucket)


    n=len(undones)
    increment=500
    instance_blobs = []
    if args.num_processes == 0:
        for instance in undones:
            if not instance in psql_instances:
                if "-" in instance:
                    if len(instance.split("-")[0]) == 8:
                        errlogger.error('Instance %s not in PSQL', instance)
                    else:
                        # blob_copy = src_bucket.copy_blob(src_bucket.blob(instance), dst_bucket, instance)
                        instance_blobs.append(instance)

                else:
                    # blob_copy = src_bucket.copy_blob(src_bucket.blob(instance), dst_bucket, instance)
                    instance_blobs.append(instance)

                if len(instance_blobs) == increment:
                    for instance in instance_blobs:
                        try:
                            src_bucket.delete_blob(instance)
                            donelogger.info(instance)
                        except NotFound:
                            errlogger.error('Instance %s not found in bucket', instance)
                            donelogger.info(instance)
                        except Exception as exc:
                            errlogger.error('Instance %s error %s', instance, exc)

                    instance_blobs = []

        if len(instance_blobs) > 0:
            for instance in instance_blobs:
                try:
                    src_bucket.delete_blob(instance)
                    donelogger.info(instance)
                except NotFound:
                    errlogger.error('Instance %s not found in bucket', instance)
                except Exception:
                    errlogger.error('Instance %s error %s', instance, Exception)

    else:
        processes = []
        # Create queues
        task_queue = Queue()
        done_queue = Queue()

        # Start worker processes
        for process in range(args.num_processes):
            args.id = process + 1
            processes.append(
                Process(target=worker,
                        args=(task_queue, done_queue, args)))
            processes[-1].start()

        for instance in undones:
            if not instance in psql_instances:
                if "-" in instance:
                    if len(instance.split("-")[0]) == 8:
                        errlogger.error('Instance %s not in PSQL', instance)
                    else:
                        # blob_copy = src_bucket.copy_blob(src_bucket.blob(instance), dst_bucket, instance)
                        instance_blobs.append(instance)

                else:
                    # blob_copy = src_bucket.copy_blob(src_bucket.blob(instance), dst_bucket, instance)
                    instance_blobs.append(instance)

                if len(instance_blobs) == increment:
                    task_queue.put((instance_blobs))
                    instance_blobs = []

        if len(instance_blobs) > 0:
            task_queue.put((instance_blobs))

        # Tell child processes to stop
        for process in processes:
            task_queue.put('STOP')

        #Wait for them to stop
        for process in processes:
            process.join()


def get_psql_instances(args):

    try:
        psql_instances = open(args.psql_instances).read().splitlines()
    except:
        conn = psycopg2.connect(dbname=args.db, user=settings.DATABASE_USERNAME,
                                password=settings.DATABASE_PASSWORD, host=settings.DATABASE_HOST)
        with conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                query = f"""
                SELECT * 
                FROM instance
                WHERE idc_version_number = 2"""
                cur.execute(query)

                howmany = 10000
                many = 0
                with open(args.dones, 'w') as f:
                    while True:
                        psql_instances = cur.fetchmany(howmany)
                        if len(psql_instances) == 0:
                            break

                        for instance in psql_instances:
                            f.write(f'{instance["instance_uuid"]}.dcm' \
                                                              '\n')
                        many += howmany
                        rootlogger.info('Got %s', many)

    return set(psql_instances)


def cleanup(args):
    psql_instances  = get_psql_instances(args)
    gcs_instances = open(args.gcs_instances).read().splitlines()
    dones = set(open(args.dones).read().splitlines())
    undones = set(gcs_instances) - dones
    cleanup_bucket(args, undones, psql_instances )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=2)
    parser.add_argument('--log_base', default='cleanup_final_bucket')
    args = parser.parse_args()
    parser.add_argument('--db', default='idc')
    parser.add_argument('--bqdataset', default='idc_v2')
    parser.add_argument('--table', default='instance')
    parser.add_argument('--final_bucket', default='idc_dev')
    parser.add_argument('--num_processes', default=32, help="Number of concurrent processes")
    parser.add_argument('--gcs_instances', default = '{}/logs/final_bucket_instances.log'.format(os.environ['PWD']))
    parser.add_argument('--psql_instances', default = '{}/logs/final_psql_instances.log'.format(os.environ['PWD']))
    parser.add_argument('--skips', default='{}/logs/{}_skips.log'.format(os.environ['PWD'], args.log_base))
    parser.add_argument('--dones', default='{}/logs/{}_dones.log'.format(os.environ['PWD'], args.log_base), help="Blobs that have been deleted")
    args = parser.parse_args()

    # Directory to which to download files from TCIA/NBIA
    args.dicom = 'dicom'
    print("{}".format(args), file=sys.stdout)

    rootlogger = logging.getLogger('root')
    root_fh = logging.FileHandler('{}/logs/{}_log.log'.format(os.environ['PWD'], args.log_base))
    rootformatter = logging.Formatter('%(levelname)s:root:%(message)s')
    rootlogger.addHandler(root_fh)
    root_fh.setFormatter(rootformatter)
    rootlogger.setLevel(INFO)

    donelogger = logging.getLogger('done')
    done_fh = logging.FileHandler(args.dones)
    doneformatter = logging.Formatter('%(message)s')
    donelogger.addHandler(done_fh)
    done_fh.setFormatter(doneformatter)
    donelogger.setLevel(INFO)

    errlogger = logging.getLogger('root.err')
    err_fh = logging.FileHandler('{}/logs/{}_err.log'.format(os.environ['PWD'], args.log_base))
    errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
    errlogger.addHandler(err_fh)
    err_fh.setFormatter(errformatter)

    cleanup(args)
