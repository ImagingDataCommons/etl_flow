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

import argparse
import json
from document_and_download_unconverted_tcia_pathology import main

parser = argparse.ArgumentParser()
parser.add_argument('--processes', default=1)
parser.add_argument('--mode', default='gen_manifest')

args = parser.parse_args()
print(f'args: {json.dumps(args.__dict__, indent=2)}')

# main(args, download_slugs=['nlst-da-path'])
main(args)

