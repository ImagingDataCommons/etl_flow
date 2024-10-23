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

import os
import logging
from logging import INFO, ERROR, WARNING
import settings
from google.cloud import storage
from google.cloud.storage import blob

# Suppress logging from request module
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("google.auth").setLevel(logging.WARNING)

print(f'Logging to {settings.LOG_DIR}')

if not os.path.exists(settings.LOGGING_BASE):
    os.mkdir(settings.LOGGING_BASE)
if not os.path.exists(settings.LOG_DIR):
    os.mkdir(settings.LOG_DIR)

logging.basicConfig()

rootlogger = logging.getLogger('root')
rootlogger.setLevel(INFO)

successlogger = logging.getLogger('root.success')
successlogger.setLevel(INFO)
for hdlr in successlogger.handlers[:]:
    successlogger.removeHandler(hdlr)
progress_fh = logging.FileHandler('{}/success.log'.format(settings.LOG_DIR))
successlogger.addHandler(progress_fh)
successformatter = logging.Formatter('%(message)s')
progress_fh.setFormatter(successformatter)

progresslogger = logging.getLogger('root.progress')
progresslogger.setLevel(INFO)
for hdlr in progresslogger.handlers[:]:
    progresslogger.removeHandler(hdlr)
# #The progress log file is usually truncated (the mode='w' does that.)
# if not hasattr(builtins, "APPEND_PROGRESSLOGGER") or builtins.APPEND_PROGRESSLOGGER==False:
#     success_fh = logging.FileHandler('{}/progress.log'.format(settings.LOG_DIR), mode='w')
# else:
progress_fh = logging.FileHandler('{}/progress.log'.format(settings.LOG_DIR))
progresslogger.addHandler(progress_fh)
successformatter = logging.Formatter('%(message)s')
progress_fh.setFormatter(successformatter)

errlogger = logging.getLogger('root.err')
errlogger.setLevel(ERROR)
for hdlr in errlogger.handlers[:]:
    errlogger.removeHandler(hdlr)
err_fh = logging.FileHandler('{}/error.log'.format(settings.LOG_DIR))
errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
errlogger.addHandler(err_fh)
err_fh.setFormatter(errformatter)

warninglogger = logging.getLogger('py.warnings')
warninglogger.setLevel(WARNING)
for hdlr in warninglogger.handlers[:]:
    warninglogger.removeHandler(hdlr)
warning_fh = logging.FileHandler('{}/warning.log'.format(settings.LOG_DIR))
warningformatter = logging.Formatter('warning:%(message)s')
warninglogger.addHandler(warning_fh)
warning_fh.setFormatter(warningformatter)


# def save_log_dirs():
#     # if os.getenv("CI"):
#     if True:
#         client = storage.Client(project="idc-dev-etl")
#         bucket = client.bucket(settings.ETL_LOGGING_RECORDS_BUCKET)
#         for log in ['success.log', 'progress.log', 'error.log']:
#             blob = bucket.blob(f'v{settings.CURRENT_VERSION}/{settings.BASE_NAME}/{log}')
#             if not blob.exists():
#                 blob.upload_from_file(open(f'{settings.LOG_DIR}/{log}'))
#             else:
#                 comp_blob = bucket.blob(f'v{settings.CURRENT_VERSION}/{settings.BASE_NAME}/compose.log')
#                 comp_blob.upload_from_file(open(f'{settings.LOG_DIR}/{log}'))
#                 blob.compose([blob, comp_blob])
#         comp_blob.delete()

