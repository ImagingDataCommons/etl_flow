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

# Hierarchically removes instances having SOPInstanceUIDs in a list, then hierarchically removes higher level elements
# Does not update the hashes
# The instances to be removed are supplied to perform_partial_deletion in a list_of_instances parameter, or
# in a manifest in GDC. In the latter case, the manifest URL is included in the args param.

from utilities.logging_config import successlogger, errlogger, progresslogger
import pandas as pd


def validate_deletions(sess, deletions):
    ids = deletions['SOPInstanceUID'].unique()
    ids = ",".join(f"\'{id}\'" for id in ids)

    query = f"""
    SELECT sop_instance_uid 
    FROM idc_instance
    WHERE sop_instance_uid in ({ids})
        """

    sop_instance_uids = sess.execute(query).fetchall()
    if len(sop_instance_uids) > 0:
        errlogger.error(f'Failed to delete {len(sop_instance_uids)}')
        return False
    else:
        return True


def remove_instances(sess, instances):
    instance_list = ','.join(f"'{w}'" for w in instances)
    query = f"""
DELETE FROM idc_instance 
WHERE sop_instance_uid IN ({instance_list})
RETURNING *
    """
    result = sess.execute(query).fetchall()
    for row in result:
        successlogger.info(f'{row.sop_instance_uid}')
    return


def remove_series(sess, instances):
    remove_instances(sess, instances)

    query = f"""
DELETE FROM idc_series
WHERE NOT EXISTS (
    SELECT FROM idc_instance
    WHERE idc_series.series_instance_uid = idc_instance.series_instance_uid
    )
RETURNING *
    """
    result = sess.execute(query).fetchall()
    for row in result:
        progresslogger.info(f'Deleted series: {row.series_instance_uid}')
    return

def remove_studies(sess, instances):
    remove_series(sess, instances)

    query = f"""
DELETE FROM idc_study
WHERE NOT EXISTS (
    SELECT FROM idc_series
    WHERE idc_study.study_instance_uid = idc_series.study_instance_uid
    )
RETURNING *
    """
    result = sess.execute(query).fetchall()
    for row in result:
        progresslogger.info(f'Deleted study: {row.study_instance_uid}')
    return


def remove_patients(sess, instances):
    remove_studies(sess, instances)

    query = f"""
DELETE FROM idc_patient
WHERE NOT EXISTS (
    SELECT FROM idc_study
    WHERE idc_patient.submitter_case_id = idc_study.submitter_case_id
    )
RETURNING *
    """
    result = sess.execute(query).fetchall()
    for row in result:
        progresslogger.info(f'Deleted patient: {row.submitter_case_id}')
    return


def remove_collections(sess, instances):
    remove_patients(sess, instances)

    query = f"""
DELETE FROM idc_collection
WHERE NOT EXISTS (
    SELECT FROM idc_patient
    WHERE idc_collection.collection_id = idc_patient.collection_id
    )
RETURNING *
    """
    result = sess.execute(query).fetchall()
    for row in result:
        progresslogger.info(f'Deleted collection: {row.collection_id}')
    return

def perform_deletions(sess, deletions):
    instances = deletions["current_SOPInstanceUID"].unique()
    instances.sort()
    remove_collections(sess, instances)
    return