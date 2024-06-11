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

# Mark instances, series, etc. as redacted in BQ tables that
# document IDC sourced data. Hashes are revised as needed.
# These tables have three forms:
# v3-v6: A single wsi_metadata table
# v7-v12: A hierarchy of wsi_collection/_patient/_study/_series/_instance one-to-many tables, include hierarchical
# hashes.
# v13-.. : As above but name changed to idc_XXX

import argparse
import settings
import json
from utilities.sqlalchemy_helpers import sa_session
from utilities.logging_config import successlogger, progresslogger, errlogger
import settings

def deprecate_instance(sess, idc_version, mitigation_id):

    # First mark redacted instances
    query = f"""
UPDATE idc_instance AS i
SET
    redacted = True,
    mitigation = '{mitigation_id}'
FROM {mitigation_id} AS d
WHERE i.sop_instance_uid=d.sop_instance_uid 
AND d.i_rev_idc_version <= {idc_version}  
AND ({idc_version} <= d.i_final_idc_version OR d.i_final_idc_version = 0 )
"""
    try:
        result = sess.execute(query)
    except Exception as exc:
        breakpoint()

    return

def deprecate_level(sess, idc_version, mitigation_id, parent, child, parent_alias, child_alias,
                    parent_id, child_id):
    # Now mark the each parent object as redacted if all its children are redacted
    query = f"""
UPDATE idc_{parent} AS {parent_alias}
SET
    redacted = redactions.redacted,
    hash = redactions.hash
FROM (
    -- For each parent in the mitigation metadata table,
    -- determine whether all its children have been redacted
    -- and its hash
    SELECT 
        {parent_alias}.{parent_id} parent_id,
        --TO_HEX(MD5(STRING_AGG(IF({child_alias}.redacted,NULL,{child_alias}.hash) ,'' ORDER BY {child_alias}.hash))) hash,
        encode(decode(MD5(STRING_AGG(IF({child_alias}.redacted,NULL,{child_alias}.hash) ,'' ORDER BY {child_alias}.hash)), 'base64'), 'hex') hash,
        bool_and({child_alias}.redacted) as redacted
    FROM idc_{parent} {parent_alias}
    JOIN idc_{child} {child_alias}
    ON {parent_alias}.{parent_id} = {child_alias}.{parent_id}
    JOIN ( 
        SELECT DISTINCT {parent_id}
        FROM {mitigation_id}
        WHERE {child_alias}_rev_idc_version <= {idc_version}  
        AND ({idc_version} <= {child_alias}_final_idc_version OR {child_alias}_final_idc_version = 0 )
        ) m
    ON {parent_alias}.{parent_id} = m.{parent_id}
    GROUP BY parent_id
    ) redactions
-- Given the table of instance level redactions, set the redaction state of the parent that are in this {idc_version}
WHERE {parent_alias}.{parent_id}=redactions.parent_id 
"""
    try:
        result = sess.execute(query)
    except Exception as exc:
        breakpoint()

    return



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version number')
    parser.add_argument('--mitigation_id', default='m1', help='ID of this mitigation event')
    args = parser.parse_args()

    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    with sa_session(echo=True) as sess:
    
        idc_version = 19
        progresslogger.info(f'Revising version {idc_version}')

        deprecate_instance(sess, idc_version, mitigation_id=args.mitigation_id)
        progresslogger.info(f'\nRevised idc_instance')
        deprecate_level(sess, idc_version,mitigation_id=args.mitigation_id, parent="series", child="instance",
                        parent_alias="se", child_alias="i", \
                        parent_id="series_instance_uid", child_id="sop_instance_uid")
        progresslogger.info(f'\nRevised idc_series')
        deprecate_level(sess, idc_version,mitigation_id=args.mitigation_id,  parent="study", child="series",
                        parent_alias="st", child_alias="se", \
                        parent_id="study_instance_uid", child_id="series_instance_uid")
        progresslogger.info(f'\nRevised idc_study')
        deprecate_level(sess, idc_version,mitigation_id=args.mitigation_id,  parent="patient", child="study", \
                        parent_alias="p", child_alias="st", \
                        parent_id="submitter_case_id", child_id="study_instance_uid")
        progresslogger.info(f'\nRevised idc_patient')
        deprecate_level(sess, idc_version,mitigation_id=args.mitigation_id,  parent="collection", child="patient", \
                        parent_alias="c", child_alias="p", \
                        parent_id="collection_id", child_id="submitter_case_id")
        progresslogger.info(f'\nRevised idc_collection')

        sess.commit()


