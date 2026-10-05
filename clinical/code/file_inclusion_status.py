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



# Document whether each file in downloads/download_<version> is included in clinical data. Basically, is a
# file found in clinical_notes.json

import os
import json
import settings

if __name__ == "__main__":
    with open("clinical_notes.json") as f:
        clinical_notes = f.read()

    not_found = []
    directory_path = f'./downloads/downloads_{settings.CURRENT_VERSION}'
    for root, dirs, files in os.walk(directory_path):
        for filename in files:
            if clinical_notes.find(filename) == -1:
                full_path = os.path.join(root, filename)
                not_found.append(full_path)
    not_found.sort()
    i = 1
    for path in not_found:
        print(i,path)
        i+=1
    pass

