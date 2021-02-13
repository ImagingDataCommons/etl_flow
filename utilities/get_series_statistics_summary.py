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
from os import listdir
from os.path import join
import argparse

def main(filename):
    path = join(os.environ['PWD'],'results')
    print(os.environ['PWD'])

    collections = [f for f in listdir(join(path, 'series_statistics'))]
    collections.sort()
    with open(join(path,filename),'w') as summary:
        for collection in collections:
            files = [f for f in listdir(join(path,'series_statistics',collection))]
            files.sort()
            summary.write('***{}***************************************************************\n'.format(collection))
            for report in files[-1:]:
                with open(join(path,'series_statistics',collection,report)) as stats:
                    summary.write('{}\n'.format(report))
                    while True:
                        line = stats.readline()
                        if line == '\n' or 'study' in line or line == '':
                            break
                        summary.write(line)
                    summary.write('\n')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', '-f', default='summary.txt')
    argz = parser.parse_args()
    print(argz)
    main(argz.file)