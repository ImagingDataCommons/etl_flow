#!/usr/bin/env
#
# Copyright 2020, Institute for Systems Biology
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

from subprocess import run, PIPE
import os
import sys
import shutil
import argparse

def main(logs_path):
    print(os.environ['PWD'])
    path = '{}/results/series_statistics'.format(os.environ['PWD'])
    if os.path.isdir(path):
        shutil.rmtree(path)
    os.mkdir(path)

    results = run(['gsutil', '-m', 'ls', logs_path], stdout=PIPE, stderr=PIPE)

    collections = [collection.split('/')[-1] for collection in results.stdout.decode().split('/\n')]
    collections.remove('')
    for collection in collections:
        if os.path.isdir('{}/{}'.format(path, collection)):
            shutil.rmtree('{}/{}'.format(path, collection))
        os.mkdir('{}/{}'.format(path, collection))
        results = run(['gsutil', '-m', '-q', 'cp',
             '{}/{}/series_statistics.*.log'.format(logs_path, collection),
             '{}/{}'.format(path, collection)])

if __name__ == '__main__':
    parser =argparse.ArgumentParser()
    parser.add_argument('--path','-p', default='gs://idc_peewee-logs/1/app', help='Path to log files')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    main(args.path)