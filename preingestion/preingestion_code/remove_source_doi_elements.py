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

# Hierarchically removes all data associated with a source_doi from the idc_xxx hierarchy
# Does not update the hashes

from utilities.logging_config import successlogger, progresslogger, errlogger

def remove_instances(args, sess):
    query = f"""
DELETE FROM idc_instance 
USING idc_series
WHERE idc_instance.series_instance_uid = idc_series.series_instance_uid
AND idc_series.source_doi = '{args.source_doi}'
RETURNING *
"""
    result = sess.execute(query).fetchall()
    successlogger.info(f'{len(result)} instances')
    for row in result:
        progresslogger.info(row)
    return


def remove_series(args, sess):
    remove_instances(args, sess)

    query = f"""
DELETE FROM idc_series
WHERE NOT EXISTS (
    SELECT FROM idc_instance
    WHERE idc_series.series_instance_uid = idc_instance.series_instance_uid
    )
RETURNING *
"""
    result = sess.execute(query).fetchall()
    successlogger.info(f'{len(result)} series')
    for row in result:
        progresslogger.info(row)
    return

def remove_studies(args, sess):
    remove_series(args, sess)

    query = f"""
DELETE FROM idc_study
WHERE NOT EXISTS (
    SELECT FROM idc_series
    WHERE idc_study.study_instance_uid = idc_series.study_instance_uid
    )
RETURNING *
    """
    result = sess.execute(query).fetchall()
    successlogger.info(f'{len(result)} studies')
    for row in result:
        progresslogger.info(row)
    return


def remove_patients(args, sess):
    remove_studies(args,sess)

    query = f"""
DELETE FROM idc_patient
WHERE NOT EXISTS (
    SELECT FROM idc_study
    WHERE idc_patient.submitter_case_id = idc_study.submitter_case_id
    )
RETURNING *
    """
    result = sess.execute(query).fetchall()
    successlogger.info(f'{len(result)} patients')
    for row in result:
        progresslogger.info(row)
    return


def remove_collections(args, sess):
    remove_patients(args, sess)

    query = f"""
DELETE FROM idc_collection
WHERE NOT EXISTS (
    SELECT FROM idc_patient
    WHERE idc_collection.collection_id = idc_patient.collection_id
    )
RETURNING *
    """
    result = sess.execute(query).fetchall()
    successlogger.info(f'{len(result)} collections')
    for row in result:
        progresslogger.info(row)
    return

    
