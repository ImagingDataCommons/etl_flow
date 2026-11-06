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

# This script copies BQ table that have been revised, as part of the mitigation,
# from idc-pdp-staging to nci-idc-bigquery-data
import sys
import copy
import settings
import argparse
import json
from utilities.logging_config import successlogger, progresslogger, errlogger
from bq.copy_tables.copy_dataset import copy_dataset
from mitigation_utilities import get_version_range
from multiprocessing import Queue, Process


def copy_tables(task_queue, args):
    for version in range(args.range[0], args.range[1]+1):
        if version in (1,2,3,4,5,6,7):
            table_ids = {
                "auxiliary_metadata": "TABLE",
                "dicom_derived_all": "TABLE",
                "dicom_metadata": "TABLE",
                "original_collections_metadata": "TABLE"
            }

        elif version in (8,9):
            table_ids = {
                "auxiliary_metadata": "TABLE",
                "dicom_derived_all": "TABLE",
                "dicom_metadata": "TABLE",
                "original_collections_metadata": "TABLE"
            }

        elif version == 10:
            table_ids = {
                "auxiliary_metadata": "TABLE",
                "dicom_all": "TABLE",
                "dicom_derived_all": "TABLE",
                "dicom_metadata": "TABLE",
                "dicom_metadata_curated": "TABLE",
                "measurement_groups": "TABLE",
                "original_collections_metadata": "TABLE",
                "qualitative_measurements": "TABLE",
                "quantitative_measurements": "TABLE",
                "segmentations": "TABLE",
            }

        elif version in (11,12):
            table_ids = {
                "auxiliary_metadata": "TABLE",
                "dicom_all": "TABLE",
                "dicom_derived_all": "TABLE",
                "dicom_metadata": "TABLE",
                "dicom_metadata_curated": "TABLE",
                "measurement_groups": "TABLE",
                "original_collections_metadata": "TABLE",
                "qualitative_measurements": "TABLE",
                "quantitative_measurements": "TABLE",
                "segmentations": "TABLE",
            }

        elif version == 13:
            table_ids = {
                "auxiliary_metadata": "TABLE",
                "dicom_all": "TABLE",
                "dicom_derived_all": "TABLE",
                "dicom_metadata": "TABLE",
                "dicom_metadata_curated": "TABLE",
                "measurement_groups": "TABLE",
                "original_collections_metadata": "TABLE",
                "qualitative_measurements": "TABLE",
                "quantitative_measurements": "TABLE",
                "segmentations": "TABLE",
            }

        elif version == 14:
            table_ids = {
                "auxiliary_metadata": "TABLE",
                "dicom_all": "TABLE",
                "dicom_derived_all": "TABLE",
                "dicom_metadata": "TABLE",
                "dicom_metadata_curated": "TABLE",
                "measurement_groups": "TABLE",
                "original_collections_metadata": "TABLE",
                "qualitative_measurements": "TABLE",
                "quantitative_measurements": "TABLE",
                "segmentations": "TABLE",
            }

        elif version in (15, 16):
            table_ids = {
                "auxiliary_metadata": "TABLE",
                "dicom_all": "TABLE",
                "dicom_derived_all": "TABLE",
                "dicom_metadata": "TABLE",
                "dicom_metadata_curated": "TABLE",
                "measurement_groups": "TABLE",
                "mutable_metadata": "TABLE",
                "original_collections_metadata": "TABLE",
                "qualitative_measurements": "TABLE",
                "quantitative_measurements": "TABLE",
                "segmentations": "TABLE",
            }

        elif version in range(17,25+1):
            table_ids = {
                "auxiliary_metadata": "TABLE",
                "dicom_all": "TABLE",
                "dicom_metadata": "TABLE",
                "dicom_metadata_curated": "TABLE",
                "dicom_pivot": "TABLE",
                "measurement_groups": "TABLE",
                "mutable_metadata": "TABLE",
                "original_collections_metadata": "TABLE",
                "qualitative_measurements": "TABLE",
                "quantitative_measurements": "TABLE",
                "segmentations": "TABLE",
            }
        else:
            errlogger.error(f'This script may need to be extended for version {version}')

        args.src_project = settings.PDP_PROJECT
        if version <=16:
            args.trg_project = 'bigquery-public-data'
        else:
            args.trg_project = settings.AH_PROJECT

        args.src_dataset = f'idc_v{version}'
        args.trg_dataset = f'idc_v{version}'

        task_queue.put((copy.deepcopy(args), table_ids, version))

    return

def worker(input):
    for args, table_ids, version in iter(input.get, 'STOP'):
        # Delete the blob from the public GCS bucket
        copy_dataset(args, table_ids, skipped_table_ids={}, copy_views=False)


if __name__ == '__main__':
    version_range = get_version_range()
    parser = argparse.ArgumentParser()
    parser.add_argument('--processes', default=16, help='Number of processes to spawn')
    args = parser.parse_args()

    args.range = version_range
    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    num_processes = args.processes
    processes = []
    task_queue = Queue()
    for process in range(num_processes):
        processes.append(
            Process(group=None, target=worker, args=(task_queue,)))
        processes[-1].start()

    copy_tables(task_queue, args)

    # Tell child processes to stop
    for i in range(num_processes):
        task_queue.put('STOP')
    # Wait for process to terminate
    for process in processes:
        print(f'Joining process: {process.name}, {process.is_alive()}')
        process.join()

    sys.exit(open(f'{errlogger.handlers[0].baseFilename}').read() != "")


