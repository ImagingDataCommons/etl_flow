"""

Copyright 2019-2020, Institute for Systems Biology

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

import settings
import argparse
import json
from publish_bq_derived_tables import main


if __name__ == '__main__':
    # (sys.argv)
    parser = argparse.ArgumentParser()
    parser.add_argument('--yaml_template', default="ConfigFiles/BQViewInstall_template.yaml", \
                        help='Template specifying tables/views to be generated')
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version number')
    parser.add_argument('--project', default="idc-pdp-staging", help='Project in which tables live')
    parser.add_argument('--dataset', default=f"idc_v{settings.CURRENT_VERSION}", help="BQ dataset")
    parser.add_argument('--view_name', default='dicom_all', help='Build this table/view if specified, or all tables')
    args = parser.parse_args()

    print(f'args: {json.dumps(args.__dict__, indent=2)}')

    main(args)