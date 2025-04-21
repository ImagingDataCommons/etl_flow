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

"""
Validate that a bucket holds the correct set of instance blobs
"""
import settings
import builtins
import json
# Noramlly the progresslogger file is trunacated. The following causes it to be appended.
# builtins.APPEND_PROGRESSLOGGER = True
from utilities.logging_config import successlogger, progresslogger, errlogger
from google.cloud import storage, bigquery
from multiprocessing import Process, Queue


# Get all the blobs that are expected to be in the public bucket
# Limited to series having a rev_idc_version LTE max_version

def get_expected_series_in_bucket(args, max_version):
    client = bigquery.Client()
    query = f"""
        SELECT DISTINCT se_uuid 
        FROM `idc-dev-etl.idc_v{args.version}_dev.all_joined` aj
        WHERE se_rev_idc_version <= {max_version}
        AND dev_bucket = '{args.bucket.replace('arch', 'dev')}'
        AND i_redacted=FALSE
        ORDER BY se_uuid
    """

    query_job = client.query(query)
    series = set(query_job.result().to_dataframe()['se_uuid'].to_list())
    return series



def get_found_series_in_bucket(args):
    client = storage.Client()
    bucket = client.bucket(args.bucket)

    try:
        # Assume we've already got the list of expected series
        with open(f"{settings.LOG_DIR}/found_series.json") as f:
            found_series = json.load(f)
    except:
        # Get found series
        iterator = client.list_blobs(bucket, page_size=args.batch)
        found_series = []
        for page in iterator.pages:
            # if page.num_items:
            seriess = [blob.name.split('.zip')[0] for blob in page]
            found_series.extend(seriess)
        with open(f"{settings.LOG_DIR}/found_series.json", "w") as f:
            json.dump(found_series, f)

    return set(found_series)


def check_all_zips(args, premerge=False, max_version=settings.CURRENT_VERSION):

    found_series = get_found_series_in_bucket(args)
    expected_series = get_expected_series_in_bucket(args, max_version)

    if found_series == expected_series:
        progresslogger.info(f"Bucket {args.bucket} has the correct set of series")
    else:
        errlogger.error(f"Bucket {args.bucket} does not have the correct set of series")
        unexpected_series = list(found_series - expected_series)
        unfound_series = list(expected_series - found_series)
        # Release memory
        del found_series
        del expected_series
        if unexpected_series:
            unexpected_series.sort()
            errlogger.error(f"Unexpected series in bucket: {len(unexpected_series)}")
            for series in unexpected_series:
                errlogger.error(series)
            with open(f"{settings.LOG_DIR}/unexpected_series.json", "w") as f:
                json.dump(unexpected_series, f)
        if unfound_series:
            unfound_series.sort()
            errlogger.error(f"Expected series not found in bucket: {len(unfound_series)}")
            for series in unfound_series:
                errlogger.error(series)
            with open(f"{settings.LOG_DIR}/unfound_series.json") as f:
                json.dump(unfound_series, f)


    return


