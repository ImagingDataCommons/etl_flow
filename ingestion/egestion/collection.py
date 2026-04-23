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
# Remove a version from the DB
from egest import egest_version
import argparse
from idc.models import Version, Base
from utilities.logging_config import successlogger, progresslogger, errlogger
from python_settings import settings
from utilities.sqlalchemy_helpers import sa_session


from utilities.logging_config import successlogger, errlogger, progresslogger

if __name__ == '__main__':

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--collection_ids', default='BoneMarrowWSI-PediatricLeukemia', help='Collection to be egested')
    args = parser.parse_args()

    with sa_session(False) as sess:
        version = sess.query(Version).filter(Version.version == settings.CURRENT_VERSION).first()
        if version:
            egest_version(sess, version, args.collection_ids)
            # sess.commit()

    pass

