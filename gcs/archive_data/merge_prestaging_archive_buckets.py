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

from subprocess import run
from utilities.logging_config import successlogger, progresslogger, errlogger
import argparse
from google.cloud import storage

def move_bucket(src_bucket, dst_bucket):
    client = storage.Client()
    if not client.bucket(src_bucket).exists():
        errlogger.error(f'Storage bucket {src_bucket} does not exist')
        return
    if not client.bucket(dst_bucket).exists():
        errlogger.error(f'Storage bucket {dst_bucket} does not exist')
        return
    if list(client.bucket(src_bucket).list_blobs(max_results=1)):
        try:
            result = run(['gcloud', 'storage', 'mv', f'gs://{src_bucket}/*', f'gs://{dst_bucket}'], check=True)
            successlogger.info(f'{src_bucket} merged into {dst_bucket}')
        except Exception as exc:
            errlogger.error(f'{src_bucket} to {dst_bucket} merge failed: {exc}')
            exit(1)
        return


if __name__ == "__main__":
    for src_bucket, dst_bucket in [
        ("idc-arch-cr-prestaging", "idc-arch-cr" ),
        ("idc-arch-defaced-prestaging", "idc-arch-defaced"),
        ("idc-arch-open-prestaging", "idc-arch-open")
    ]:
        move_bucket(src_bucket, dst_bucket)






