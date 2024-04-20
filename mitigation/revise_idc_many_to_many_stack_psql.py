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

# Mark instances as redacted in BQ 'instance' table. These tables have two  forms:
# v1-v2: instance_uuid column
# v3+: uuid column
#

from utilities.sqlalchemy_helpers import sa_session
from utilities.logging_config import successlogger, progresslogger, errlogger

def deprecate_instance(sess, idc_version, mitigation_id):

    # First mark redacted instances
    query = f"""
UPDATE instance AS i
SET
    redacted = True,
    mitigation = '{mitigation_id}'
FROM (
    SELECT
        DISTINCT i.uuid uuid
    FROM instance AS i
    JOIN {mitigation_id} AS d
    ON i.uuid=d.i_uuid 
    WHERE (d.i_final_idc_version <= {idc_version} 
    OR (d.i_rev_idc_version <= {idc_version} 
    AND ({idc_version} <= d.i_final_idc_version OR d.i_final_idc_version = 0)))
    ) redactions
WHERE i.uuid = redactions.uuid

"""
    try:
        result = sess.execute(query)
    except Exception as exc:
        breakpoint()

    return


def deprecate_series(sess, idc_version, mitigation_id, parent, child, parent_alias, child_alias,
                    parent_id, child_id):
    # Now mark the each parent object as redacted if all its children are redacted
    query = f"""
UPDATE {parent} AS {parent_alias}
SET
    redacted = redactions.redacted,
    hashes = redactions.hs
FROM (
    -- For each parent in the mitigation metadata table,
    -- determine whether all its children have been redacted
    -- and its hash
    SELECT
        {parent_alias}_{child_alias}.{parent}_uuid parent_id,
        bool_and({child_alias}.redacted) as redacted,
        if(bool_and({child_alias}.redacted),
            -- If all children are redacted, then the parent is redacted and its hashes are all null
            CAST(ROW(
                '', 
                '', 
                '') AS hashes),
            -- Otherwise we separately aggregate the 'tcia', 'idc' and 'all' hash of each child. If a child is redacted,
            -- its hashes are aggregated as NULLs 
            if(STRING_AGG(if({child_alias}.redacted,NULL,{child_alias}.hash) ,'') IS NULL,
                CAST(ROW(
                    '', 
                    '', 
                    '') AS hashes),
                if({child_alias}.source='tcia',
                     CAST(ROW(
                        encode(decode(MD5(STRING_AGG(if({child_alias}.redacted,NULL,{child_alias}.hash) ,'')), 'base64'), 'hex'),
                        '',
                        encode(decode(MD5(STRING_AGG(if({child_alias}.redacted,NULL,{child_alias}.hash) ,'')), 'base64'), 'hex') 
                        ) AS hashes),
                    CAST(ROW(
                        '',
                        encode(decode(MD5(STRING_AGG(if({child_alias}.redacted,NULL,{child_alias}.hash) ,'')), 'base64'), 'hex'),
                        encode(decode(MD5(STRING_AGG(if({child_alias}.redacted,NULL,{child_alias}.hash) ,'')), 'base64'), 'hex')
                        ) AS hashes)
                )
            )
        ) hs
    FROM {parent}_{child} {parent_alias}_{child_alias}
    JOIN {child} {child_alias}
    ON {parent_alias}_{child_alias}.{child}_uuid = {child_alias}.uuid
    JOIN ( 
        SELECT DISTINCT {parent_alias}_uuid
        FROM {mitigation_id}
        WHERE {child_alias}_final_idc_version <= {idc_version}  
        OR ({child_alias}_rev_idc_version <= {idc_version}  
        AND ({idc_version} <= {child_alias}_final_idc_version OR {child_alias}_final_idc_version = 0 ))
        ) m
    ON {parent_alias}_{child_alias}.{parent}_uuid = m.{parent_alias}_uuid
    GROUP BY parent_id, {child_alias}.source
    ) redactions
-- Given the table of instance level redactions, set the redaction state of the parent that are in this {idc_version}
WHERE {parent_alias}.uuid=redactions.parent_id 
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
UPDATE {parent} AS {parent_alias}
SET
    -- We do not expect to redact an entire version
    {"redacted = redactions.redacted," if parent != 'version' else ''}
    hashes = redactions.hs
FROM (
    -- For each parent in the mitigation metadata table,
    -- determine whether all its children have been redacted
    -- and its hash
    SELECT
        {parent_alias}_{child_alias}.{parent}{'_uuid' if parent != 'version' else ''} parent_id,
        bool_and({child_alias}.redacted) as redacted,
        if(bool_and({child_alias}.redacted),
            -- If all children are redacted, then the parent is redacted and its hashes are all null
            CAST(ROW(
                '', 
                '', 
                '') AS hashes),
            -- Otherwise we separately aggregate the 'tcia', 'idc' and 'all' hash of each child. If a child is redacted,
            -- its hashes are aggregated as NULLs 
            CAST(ROW( 
                IF(STRING_AGG(({child_alias}).hashes.tcia, '')='', '', 
                    IF(STRING_AGG(({child_alias}).hashes.tcia, '') IS NULL, '', 
                        encode(decode(MD5(STRING_AGG(if({child_alias}.redacted,NULL,({child_alias}).hashes.tcia) ,'' ORDER BY ({child_alias}).hashes.tcia)), 'base64'), 'hex'))),
                IF(STRING_AGG(({child_alias}).hashes.idc, '')='', '', 
                    IF(STRING_AGG(({child_alias}).hashes.idc, '') IS NULL, '', 
                       encode(decode(MD5(STRING_AGG(if({child_alias}.redacted,NULL,({child_alias}).hashes.idc) ,'' ORDER BY ({child_alias}).hashes.idc)), 'base64'), 'hex'))),
                if(STRING_AGG(({child_alias}).hashes.all_sources, '')='', '', 
                    if(STRING_AGG(({child_alias}).hashes.all_sources, '') IS NULL, '', 
                       encode(decode(MD5(STRING_AGG(if({child_alias}.redacted,NULL,({child_alias}).hashes.all_sources) ,'' ORDER BY ({child_alias}).hashes.all_sources)), 'base64'), 'hex')))
                ) AS hashes)
        ) hs
    FROM {parent}_{child} {parent_alias}_{child_alias}
    JOIN {child} {child_alias}
    ON {parent_alias}_{child_alias}.{child}_uuid = {child_alias}.uuid
    JOIN ( 
        SELECT DISTINCT {parent_alias}_uuid
        FROM {mitigation_id}
        WHERE {child_alias}_final_idc_version <= {idc_version}  
        OR ({child_alias}_rev_idc_version <= {idc_version}  
        AND ({idc_version} <= {child_alias}_final_idc_version OR {child_alias}_final_idc_version = 0 ))
        ) m
    ON {parent_alias}_{child_alias}.{parent}{'_uuid' if parent != 'version' else ''} = m.{parent_alias}_uuid
    GROUP BY parent_id
    ) redactions
-- Given the table of instance level redactions, set the redaction state of the parent that are in this {idc_version}
WHERE {parent_alias}.{'uuid' if parent != 'version' else 'version'}=redactions.parent_id 
"""
    try:
        result = sess.execute(query)
    except Exception as exc:
        breakpoint()

    return


if __name__ == "__main__":
    with sa_session(echo=True) as sess:

        idc_version = 19
        progresslogger.info(f'Revising version {idc_version}')
        deprecate_instance(sess, idc_version, mitigation_id="m1")
        progresslogger.info(f'\nRevised instance')
        deprecate_series(sess, idc_version, mitigation_id="m1", parent="series", child="instance",
                        parent_alias="se", child_alias="i", \
                        parent_id="series_instance_uid", child_id="sop_instance_uid")
        progresslogger.info(f'\nRevised series')
        deprecate_level(sess, idc_version, mitigation_id="m1",  parent="study", child="series",
                        parent_alias="st", child_alias="se", \
                        parent_id="study_instance_uid", child_id="series_instance_uid")
        progresslogger.info(f'\nRevised study')
        deprecate_level(sess, idc_version, mitigation_id="m1",  parent="patient", child="study", \
                        parent_alias="p", child_alias="st", \
                        parent_id="submitter_case_id", child_id="study_instance_uid")
        progresslogger.info(f'\nRevised patient')
        deprecate_level(sess, idc_version, mitigation_id="m1",  parent="collection", child="patient", \
                        parent_alias="c", child_alias="p", \
                        parent_id="collection_id", child_id="submitter_case_id")
        progresslogger.info(f'\nRevised collection')
        deprecate_level(sess, idc_version, mitigation_id="m1",  parent="version", child="collection", \
                        parent_alias="v", child_alias="c", \
                        parent_id="version", child_id="collection_id")
        progresslogger.info(f'\nRevised collection')

        sess.commit()


