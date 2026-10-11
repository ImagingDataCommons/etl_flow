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

# This script generates the BQ auxiliary_metadata table. It is parameterizable
# to build either with 'pre-merge or 'post-merge' GCS URLS of new instances.
# It is also paramaterizable to build in the idc-dev-etl or idc-pdp-staging
# projects
from google.cloud import bigquery

from python_settings import settings
from utilities.bq_helpers import load_BQ_from_json, query_BQ, create_BQ_table, delete_BQ_Table
from utilities.logging_config import successlogger,progresslogger
from bq.generate_tables_and_views.auxiliary_metadata_table.schema import auxiliary_metadata_schema


def build_table(args):
    query = f"""
WITH newest AS
      # Each row is an IDC DOI and the most recent IDC version of the source having that DOI
      # Apparently not using sd, which is good
      (SELECT DISTINCT source_doi, CAST(SPLIT(source_doi, '.')[2] AS INT64) sd, MAX(i_rev_idc_version) newest 
      FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_public_and_current` 
      GROUP BY source_doi, i_source
      HAVING i_source='idc'
      ORDER BY sd),
newest_versioned_source_dois AS (
      # Each row is a source_doi, versioned_source_doi pair
      # For IDC, the versioned_source_doi is that of the newest i_rev_idc_version. That is, for auxiliary_metatdata,
      # all instances get the newest versioned_source_doi of their source.
      # For TCIA, the versioned_source_doi is ""
      (SELECT DISTINCT aj.source_doi, aj.versioned_source_doi
      FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_public_and_current` aj
      JOIN newest
      ON aj.source_doi = newest.source_doi AND aj.i_rev_idc_version=newest.newest
      ORDER by aj.source_doi)
      UNION ALL
      (SELECT DISTINCT aj.source_doi, "" versioned_source_doi 
      FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_public_and_current` aj
      WHERE i_source='tcia')
),
 revised_instances AS (
    SELECT se_uuid, i_uuid
    FROM `{settings.DEV_PROJECT}.idc_v{args.version}_dev.all_joined_public_and_current`
    WHERE se_rev_idc_version = {args.version} AND i_rev_idc_version <> {args.version}
    ORDER BY collection_id
  ),
  previous_se_uuid AS (
    SELECT DISTINCT ajp.se_uuid previous_se_uuid, ajp.i_uuid
    FROM `{settings.DEV_PROJECT}.idc_v{args.version}_dev.all_joined_public` ajp
    JOIN revised_instances
      ON ajp.i_uuid = revised_instances.i_uuid
    WHERE se_final_idc_version = {args.version - 1}
  )

SELECT 
      collection_id as collection_name,
      REPLACE(REPLACE(LOWER(collection_id),'-','_'), ' ','_') AS collection_id,
      c_min_timestamp as collection_timestamp,
      c_hashes.all_hash AS collection_hash,
      c_init_idc_version AS collection_init_idc_version,
      c_rev_idc_version AS collection_revised_idc_version,
    --
      submitter_case_id AS PatientID,
      idc_case_id AS idc_case_id,
      p_hashes.all_hash AS patient_hash,
      p_init_idc_version AS patient_init_idc_version,
      p_rev_idc_version AS patient_revised_idc_version,
    --
      study_instance_uid AS StudyInstanceUID,
      st_uuid AS study_uuid,
      study_instances,
      st_hashes.all_hash AS study_hash,
      st_init_idc_version AS study_init_idc_version,
      st_rev_idc_version AS study_revised_idc_version,
      st_final_idc_version AS study_final_idc_version,
    --
      series_instance_uid AS SeriesInstanceUID,
      # se_uuid AS series_uuid,
      IF(se_rev_idc_version = {settings.CURRENT_VERSION} and i_rev_idc_version <> {settings.CURRENT_VERSION} and not False,
                previous_se_uuid,
            #else
                se_uuid) AS series_uuid,

      # series_gcs_url
      # In the premerge case, in the event that some, but not all, instances in
      # a series are added|revised|deleted, there are will be new blobs corresponding to the unchanged instances. These
      # will have the new uuid of the revised series. However, there are not, at this stage, actual blobs with the new blob
      # name: <new_series_uuid>/<current_instance_uuid>.dcm. For each such unrevised instance we use the public bucket and
      # se_uuid of the blob which the new blob replaces.
      # Note, for these unrevised blobs, trying to use the just gs://<se_uuid/\* to get all instances
      # in the series will not get the revised instances.
      CONCAT('gs://',
        # If we are generating series_gcs_url for the public auxiliary_metadata table 
        if('{args.target}' = 'pub', 
            pub_gcs_bucket,
        #else 
            # If this series is new in this version and we 
            # have not merged new instances into dev buckets
            if(se_rev_idc_version = {settings.CURRENT_VERSION} and i_rev_idc_version = {args.version} and not {args.merged},
                # We use the premerge url prefix
                CONCAT('idc_v', {settings.CURRENT_VERSION}, 
                    '_',
                    i_source,
                    '_',
                    REPLACE(REPLACE(LOWER(collection_id),'-','_'), ' ','_')
                    ),
    
            #else
                 # This series is not new so use the public bucket prefix. The dev bucket is archived.
                pub_gcs_bucket
                )
            ), 
        '/', 
        # If the instance is unchanged but its series has changed  we use the se_uuid of the previous version
        IF(se_rev_idc_version = {args.version} and i_rev_idc_version <> {args.version} and not {args.merged},
                previous_se_uuid,
            #else
                se_uuid),
         '/') AS series_gcs_url,
      
      # There are no dev S3 buckets, so populate the aws_series_url 
      # the same for both dev and pub versions of auxiliary_metadata
      CONCAT('s3://',
        pub_aws_bucket,
            '/', se_uuid, '/') as series_aws_url,           
      IF(collection_id='APOLLO', '', aj.source_doi) AS Source_DOI,
      source_url AS Source_URL,
      nvsd.versioned_source_doi,
      series_instances,
      se_hashes.all_hash AS series_hash,
      se_init_idc_version AS series_init_idc_version,
      se_rev_idc_version AS series_revised_idc_version,
      se_final_idc_version AS series_final_idc_version,
    --
      sop_instance_uid AS SOPInstanceUID,
      aj.i_uuid AS instance_uuid,
      
    
#       CONCAT('gs://',
#         # If we are generating gcs_url for the public auxiliary_metadata table 
#         if('{args.target}' = 'pub', 
#             pub_gcs_bucket,
#         #else 
#             # We are generating the dev auxiliary_metadata
#             # If this instance is new in this version and we 
#             # have not merged new instances into dev buckets
#             # the blob is new if the containing series is new,
#             # but the instance may not be new.
#             if(se_rev_idc_version = {settings.CURRENT_VERSION} and not {args.merged},
#                 # We use the premerge url prefix
#                 CONCAT('idc_v', {settings.CURRENT_VERSION}, 
#                     '_',
#                     i_source,
#                     '_',
#                     REPLACE(REPLACE(LOWER(collection_id),'-','_'), ' ','_')
#                     ),  
#             #else
#                  # This instance is not new so use the pub bucket prefix; the dev bucket is archived
#                 pub_gcs_bucket
#                 )
#             ), 
#         '/', se_uuid, '/', aj.i_uuid, '.dcm') as gcs_url,

      # gcs_url
      CONCAT('gs://',
        # If we are generating series_gcs_url for the public auxiliary_metadata table 
        if('{args.target}' = 'pub', 
            pub_gcs_bucket,
        #else 
            # If this series is new in this version and we 
            # have not merged new instances into dev buckets
            if(se_rev_idc_version = {settings.CURRENT_VERSION} and i_rev_idc_version = {args.version} and not {args.merged},
                # We use the premerge url prefix
                CONCAT('idc_v', {settings.CURRENT_VERSION}, 
                    '_',
                    i_source,
                    '_',
                    REPLACE(REPLACE(LOWER(collection_id),'-','_'), ' ','_')
                    ),
    
            #else
                 # This series is not new so use the public bucket prefix. The dev bucket is archived.
                pub_gcs_bucket
                )
            ), 
            '/', 
        # If the instance is unchanged but its series has changed  we use the se_uuid of the previous version
        IF(se_rev_idc_version = {args.version} and i_rev_idc_version <> {args.version} and not {args.merged},
                previous_se_uuid,
            #else
                se_uuid),
         '/', aj.i_uuid, '.dcm') AS gcs_url,
      
    # gcs_bucket
    # # If we are generating gcs_bucket for the public auxiliary_metadata table 
    # if('{args.target}' = 'pub', 
    #     pub_gcs_bucket, 
    # #else 
    #     # We are generating the dev auxiliary_metadata
    #     # If this series is new in this version and we 
    #     # have not merged new instances into dev buckets
    #     if(se_rev_idc_version = {settings.CURRENT_VERSION} and not {args.merged},
    #         # We use the premerge url prefix
    #         CONCAT('idc_v', {settings.CURRENT_VERSION}, 
    #             '_',
    #             i_source,
    #             '_',
    #             REPLACE(REPLACE(LOWER(collection_id),'-','_'), ' ','_')
    #             ),
    #     #else
    #          # This instance is not new so use the public bucket prefix; the dev bucket is archived
    #         pub_gcs_bucket
    #         )
    #     ) as gcs_bucket,
      
      
        # gcs_bucket
        # If we are generating series_gcs_url for the public auxiliary_metadata table 
        if('{args.target}' = 'pub', 
            pub_gcs_bucket,
        #else 
            # If this series is new in this version and we 
            # have not merged new instances into dev buckets
            if(se_rev_idc_version = {settings.CURRENT_VERSION} and i_rev_idc_version = {args.version} and not {args.merged},
                # We use the premerge url prefix
                CONCAT('idc_v', {settings.CURRENT_VERSION}, 
                    '_',
                    i_source,
                    '_',
                    REPLACE(REPLACE(LOWER(collection_id),'-','_'), ' ','_')
                    ),
    
            #else
                 # This series is not new so use the public bucket prefix.
                 pub_gcs_bucket
                 )
            ) as gcs_bucket,

      
      
      # There are no dev S3 buckets, so populate the aws_url 
      # the same for both dev and pub versions of auxiliary_metadata
      CONCAT('s3://',
        pub_aws_bucket,
            '/', se_uuid, '/', aj.i_uuid, '.dcm') as aws_url,
      # There are no dev S3 buckets, so populate the aws_bucket 
      # the same for both dev and pub versions of auxiliary_metadata
      pub_aws_bucket aws_bucket,
      i_size AS instance_size,
      i_hash AS instance_hash,
      i_init_idc_version AS instance_init_idc_version,
      i_rev_idc_version AS instance_revised_idc_version,
      i_final_idc_version AS instance_final_idc_version,
      license_url,
      license_long_name,
      license_short_name,
      submitter_case_id AS submitter_case_id,
      "Public" Access
      FROM `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.all_joined_public_and_current` aj
      JOIN `{settings.DEV_PROJECT}.{settings.BQ_DEV_INT_DATASET}.licenses` licenses
      ON aj.source_doi = licenses.source_doi
      JOIN newest_versioned_source_dois nvsd
      ON aj.source_doi = nvsd.source_doi
      LEFT JOIN previous_se_uuid psu
      ON aj.i_uuid = psu.i_uuid
      ORDER BY
        collection_name, submitter_case_id
"""
    client = bigquery.Client(project=args.dst_project)
    result = delete_BQ_Table(client, args.dst_project, args.trg_bqdataset_name, args.bqtable_name)
    # Create a table to get the schema defined
    created_table = create_BQ_table(client, args.dst_project, args.trg_bqdataset_name, args.bqtable_name, auxiliary_metadata_schema, exists_ok=True)
    # Perform the query and save results in specified table
    results = query_BQ(client, args.trg_bqdataset_name, args.bqtable_name, query, write_disposition='WRITE_TRUNCATE')
    populated_table = client.get_table(f"{args.dst_project}.{args.trg_bqdataset_name}.{args.bqtable_name}")
    populated_table.schema = auxiliary_metadata_schema
    populated_table.description = "IDC version-related metadata"
    client.update_table(populated_table, fields=["schema", "description"])
    successlogger.info('Created auxiliary_metadata table')

def gen_aux_table(args):
    build_table(args)