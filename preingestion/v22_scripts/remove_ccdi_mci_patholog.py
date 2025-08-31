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

# Completely remove ccdi-mci collection because all instances in
# batches 1-4 have been revised

import sys
import argparse
from utilities.sqlalchemy_helpers import sa_session
from google.cloud import storage

from idc.models import IDC_Collection, IDC_Patient, IDC_Study, IDC_Series
from preingestion.preingestion_code.remove_source_doi_elements import  remove_collections

def prebuild(args):
    with sa_session() as sess:
        remove_collections(args, sess)
        sess.commit()
    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--source_doi', default='10.5281/zenodo.11099086', \
                        help='Only delete instances having this source_url')

    args = parser.parse_args()
    print("{}".format(args), file=sys.stdout)
    args.client=storage.Client()

    prebuild(args)
