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
from utilities.logging_config import successlogger, progresslogger, errlogger
from google.cloud import storage, bigquery

def get_expected_blobs_in_bucket(args, premerge=False):
    client = bigquery.Client()
    # query = f"""
    #   SELECT distinct concat(s.uuid,'/', i.uuid, '.dcm') as blob_name
    #   FROM `idc-dev-etl.idc_v{args.version}_dev.version` v
    #       JOIN `idc-dev-etl.idc_v{args.version}_dev.version_collection` vc ON v.version = vc.version
    #       JOIN `idc-dev-etl.idc_v{args.version}_dev.collection` c ON vc.collection_uuid = c.uuid
    #       JOIN `idc-dev-etl.idc_v{args.version}_dev.collection_patient` cp ON c.uuid = cp.collection_uuid
    #       JOIN `idc-dev-etl.idc_v{args.version}_dev.patient` p ON cp.patient_uuid = p.uuid
    #       JOIN `idc-dev-etl.idc_v{args.version}_dev.patient_study` ps ON p.uuid = ps.patient_uuid
    #       JOIN `idc-dev-etl.idc_v{args.version}_dev.study` st ON ps.study_uuid = st.uuid
    #       JOIN `idc-dev-etl.idc_v{args.version}_dev.study_series` ss ON st.uuid = ss.study_uuid
    #       JOIN `idc-dev-etl.idc_v{args.version}_dev.series` se ON ss.series_uuid = se.uuid
    #       JOIN `idc-dev-etl.idc_v{args.version}_dev.series_instance` si ON se.uuid = si.series_uuid
    #       JOIN `idc-dev-etl.idc_v{args.version}_dev.instance` i ON si.instance_uuid = i.uuid
    #       JOIN `idc-dev-etl.idc_v{args.version}_dev.all_collections` aic
    #       ON c.collection_id = aic.tcia_api_collection_id
    #       WHERE ((i.source='tcia' and aic.{args.dev_or_pub}_tcia_url="{args.bucket}")
    #       OR (i.source='idc' and aic.{args.dev_or_pub}_idc_url="{args.bucket}"))
    #       AND i.excluded = False
    #       AND if({premerge}, i.rev_idc_version < {args.version}, i.rev_idc_version <= {args.version})
    #   """

    query = f"""
      SELECT distinct concat(se_uuid,'/', i_uuid, '.dcm') as blob_name
      FROM `idc-dev-etl.idc_v{args.version}_dev.all_joined` aj
      JOIN `idc-dev-etl.idc_v{args.version}_dev.all_collections` aic
      ON aj.idc_collection_id = aic.idc_collection_id
      WHERE ((i_source='tcia' and aic.{args.dev_or_pub}_gcs_tcia_url="{args.bucket}")
      OR (i_source='idc' and aic.{args.dev_or_pub}_gcs_idc_url="{args.bucket}"))
      AND i_excluded = False
      AND if({premerge}, i_rev_idc_version < {args.version}, i_rev_idc_version <= {args.version})
  """

    query_job = client.query(query)  # Make an API request.
    query_job.result()  # Wait for the query to complete.

    # Get the destination table for the query results.
    #
    # All queries write to a destination table. If a destination table is not
    # specified, the BigQuery populates it with a reference to a temporary
    # anonymous table after the query completes.
    destination = query_job.destination

    # Get the schema (and other properties) for the destination table.
    #
    # A schema is useful for converting from BigQuery types to Python types.
    destination = client.get_table(destination)
    with open(args.expected_blobs, 'w') as f:
        for page in client.list_rows(destination, page_size=args.batch).pages:
            rows = [f'{row["blob_name"]}\n' for row in page]
            f.write(''.join(rows))

# def get_found_blobs_in_bucket(args):
#     client = storage.Client()
#     bucket = client.bucket(args.bucket)
#     page_token = ""
#     # iterator = client.list_blobs(bucket, page_token=page_token, max_results=args.batch)
#     iterator = client.list_blobs(bucket, versions=False, page_token=page_token, page_size=args.batch)
#     with open(args.found_blobs, 'w') as f:
#         for page in iterator.pages:
#             blobs = [f'{blob.name}\n' for blob in page]
#             f.write(''.join(blobs))

def get_found_blobs_in_bucket(args):
    client = storage.Client()
    bucket = client.bucket(args.bucket)
    page_token = ""
    # iterator = client.list_blobs(bucket, page_token=page_token, max_results=args.batch)
    with open(args.found_blobs, 'w') as f:
        series_iterator = client.list_blobs(bucket, versions=False, page_token=page_token, page_size=args.batch, \
                                            prefix='', delimiter='/')
        for page in series_iterator.pages:
            for prefix in page.prefixes:
                instance_iterator = client.list_blobs(bucket, versions=False, page_token=page_token, page_size=args.batch, \
                                         prefix=prefix)
                for page in instance_iterator.pages:
                    blobs = [f'{blob.name}\n' for blob in page]
                    f.write(''.join(blobs))

def check_all_instances(args, premerge=False):
    try:
        expected_blobs = set(open(args.expected_blobs).read().splitlines())
        progresslogger.info(f'Already have expected blobs')
    except:
        progresslogger.info(f'Getting expected blobs')
        get_expected_blobs_in_bucket(args, premerge)
        expected_blobs = set(open(args.expected_blobs).read().splitlines())
        # json.dump(psql_blobs, open(args.blob_names), 'w')

    try:
        found_blobs = set(open(args.found_blobs).read().splitlines())
        progresslogger.info(f'Already have found blobs')
    except:
        progresslogger.info(f'Getting found blobs')
        get_found_blobs_in_bucket(args)
        found_blobs = set(open(args.found_blobs).read().splitlines())
        # json.dump(psql_blobs, open(args.blob_names), 'w')
    if found_blobs == expected_blobs:
        successlogger.info(f"Bucket {args.bucket} has the correct set of blobs")
    else:
        errlogger.error(f"Bucket {args.bucket} does not have the correct set of blobs")
        errlogger.error(f"Unexpected blobs in bucket: {len(found_blobs - expected_blobs)}")
        for blob in found_blobs - expected_blobs:
            errlogger.error(blob)
        errlogger.error(f"Expected blobs not found in bucket: {len(expected_blobs - found_blobs)}")
        for blob in expected_blobs - found_blobs:
            errlogger.error(blob)

    return


