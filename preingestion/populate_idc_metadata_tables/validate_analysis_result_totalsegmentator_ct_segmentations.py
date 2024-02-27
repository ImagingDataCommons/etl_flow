#
# Copyright 2015-2024, Institute for Systems Biology
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

# Validate that the IDC DB contains the instances of some analysis result

from idc.models import Base, IDC_Collection, IDC_Patient, IDC_Study, IDC_Series, IDC_Instance, Collection, Patient
from utilities.logging_config import successlogger, errlogger, progresslogger
from python_settings import settings

from sqlalchemy.orm import Session
from sqlalchemy import create_engine, update
from google.cloud import storage

from validate_analysis_result import validate_analysis_result


if __name__ == '__main__':
    import argparse
    import sys

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--version', default=settings.CURRENT_VERSION)
    parser.add_argument('--src_bucket', default='012624-nlst-126k-cohort', help='Bucket containing WSI instances')
    parser.add_argument('--subdir', default='', help="Subdirectory of mount_point at which to start walking directory")
    parser.add_argument('--source_url', default='https://doi.org/10.5281/zenodo.8347012',\
                        help='Info page URL')
    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    args.client=storage.Client()

    validate_analysis_result(args)


