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
from logging import INFO
import settings
print(f'Logging to {settings.LOG_DIR}')

if not os.path.exists(settings.LOGGING_BASE):
    os.mkdir(settings.LOGGING_BASE)
if not os.path.exists(settings.LOG_DIR):
    os.mkdir(settings.LOG_DIR)

successlogger = logging.getLogger('root.success')
successlogger.setLevel(INFO)
for hdlr in successlogger.handlers[:]:
    successlogger.removeHandler(hdlr)
success_fh = logging.FileHandler('{}/success.log'.format(settings.LOG_DIR))
successlogger.addHandler(success_fh)
successformatter = logging.Formatter('%(message)s')
success_fh.setFormatter(successformatter)

progresslogger = logging.getLogger('root.progress')
progresslogger.setLevel(INFO)
for hdlr in progresslogger.handlers[:]:
    progresslogger.removeHandler(hdlr)
success_fh = logging.FileHandler('{}/progress.log'.format(settings.LOG_DIR))
progresslogger.addHandler(success_fh)
successformatter = logging.Formatter('%(message)s')
success_fh.setFormatter(successformatter)

errlogger = logging.getLogger('root.err')
for hdlr in errlogger.handlers[:]:
    errlogger.removeHandler(hdlr)
err_fh = logging.FileHandler('{}/error.log'.format(settings.LOG_DIR))
errformatter = logging.Formatter('%(levelname)s:err:%(message)s')
errlogger.addHandler(err_fh)
err_fh.setFormatter(errformatter)