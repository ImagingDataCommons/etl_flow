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

from google.cloud import bigquery
from utilities.logging_config import successlogger, progresslogger, errlogger
import settings
from mitigation_utilities import get_deleted_instance_uuids

IDC_VERSION = 3

def deprecate_instance(client, idc_version, table_prefix, mitigation_id):
    client = bigquery.Client()

    # First mark redacted instances
    query = f"""
UPDATE `{settings.DEV_MITIGATION_PROJECT}.idc_v{idc_version}_dev.{table_prefix}instance` AS i
SET
    redacted = True,
    mitigation = '{mitigation_id}'
FROM (
    SELECT
        DISTINCT i.uuid uuid,
    FROM `{settings.DEV_MITIGATION_PROJECT}.idc_v{idc_version}_dev.{table_prefix}instance` AS i
    JOIN `{settings.DEV_MITIGATION_PROJECT}.mitigations.{mitigation_id}` AS d
    ON i.uuid=d.i_uuid 
    WHERE (d.i_final_idc_version <= {idc_version} 
    OR (d.i_rev_idc_version <= {idc_version} 
    AND ({idc_version} <= d.i_final_idc_version OR d.i_final_idc_version = 0)))
    ) redactions
WHERE i.uuid = redactions.uuid

"""
    result = client.query(query)
    while result.state != 'DONE':
        result = client.get_job(result.job_id)
    if result.error_result != None:
        breakpoint()
    return


def deprecate_series(client, idc_version, table_prefix, mitigation_id, parent, child, parent_alias, child_alias,
                    parent_id, child_id):
    # Now mark the each parent object as redacted if all its children are redacted
    query = f"""
UPDATE `{settings.DEV_MITIGATION_PROJECT}.idc_v{idc_version}_dev.{table_prefix}{parent}` AS {parent_alias}
SET
    redacted = redactions.redacted,
    `hashes` = STRUCT(
        if(`redactions`.`hashes`[0] IS NULL, "", `redactions`.`hashes`[0]) AS tcia_hash,
        if(`redactions`.`hashes`[1] IS NULL, "", `redactions`.`hashes`[1]) AS idc__hash,
        if(`redactions`.`hashes`[2] IS NULL, "", `redactions`.`hashes`[2]) AS all_hash
    )
FROM (
    # For each parent in the mitigation metadata table,
    # determine whether all its children have been redacted
    # and its hash
    SELECT
        {parent_alias}_{child_alias}.{parent}_uuid parent_id,
        LOGICAL_AND({child_alias}.redacted) as redacted,
        if(LOGICAL_AND({child_alias}.redacted),
            # If all children are redacted, then the parent is redacted and its hashes are all null
            STRUCT(
                "" AS tcia_hash, 
                "" AS idc_hash, 
                "" AS all_hash),
            # Otherwise we separately aggregate the 'tcia', 'idc' and 'all' hash of each child. If a child is redacted,
            # its hashes are aggregated as NULLs 
            if({child_alias}.source='tcia',
 
                STRUCT(
                    TO_HEX(MD5(STRING_AGG(if({child_alias}.redacted,NULL,{child_alias}.hash) ,"" ORDER BY {child_alias}.hash))) AS tcia_hash,
                    "" AS tcia_hash,
                    TO_HEX(MD5(STRING_AGG(if({child_alias}.redacted,NULL,{child_alias}.hash) ,"" ORDER BY {child_alias}.hash))) AS all_hash 
                    ),
                STRUCT(
                    "" AS tcia_hash,
                    TO_HEX(MD5(STRING_AGG(if({child_alias}.redacted,NULL,{child_alias}.hash) ,"" ORDER BY {child_alias}.hash))) AS tcia_hash,
                    TO_HEX(MD5(STRING_AGG(if({child_alias}.redacted,NULL,{child_alias}.hash) ,"" ORDER BY {child_alias}.hash))) AS all_hash 
                )
            )
        ) hashes
    FROM `{settings.DEV_MITIGATION_PROJECT}.idc_v{idc_version}_dev.{parent}_{child}` {parent_alias}_{child_alias}
    JOIN `{settings.DEV_MITIGATION_PROJECT}.idc_v{idc_version}_dev.{table_prefix}{child}` {child_alias}
    ON {parent_alias}_{child_alias}.{child}_uuid = {child_alias}.uuid
    JOIN ( 
        SELECT DISTINCT {parent_alias}_uuid
        FROM `idc-dev-mitigation.mitigations.{mitigation_id}`
        WHERE {child_alias}_final_idc_version <= {idc_version}  
        OR ({child_alias}_rev_idc_version <= {idc_version}  
        AND ({idc_version} <= {child_alias}_final_idc_version OR {child_alias}_final_idc_version = 0 ))
        ) m
    ON {parent_alias}_{child_alias}.{parent}_uuid = m.{parent_alias}_uuid
    GROUP BY parent_id, {child_alias}.source
    ) redactions
# Given the table of instance level redactions, set the redaction state of the parent that are in this {idc_version}
WHERE {parent_alias}.uuid=redactions.parent_id 
"""
    result = client.query(query)
    while result.state != 'DONE':
        result = client.get_job(result.job_id)
    if result.error_result != None:
        breakpoint()

    return


def deprecate_level(client, idc_version, table_prefix, mitigation_id, parent, child, parent_alias, child_alias,
                    parent_id, child_id):
    # Now mark the each parent object as redacted if all its children are redacted
    query = f"""
UPDATE `{settings.DEV_MITIGATION_PROJECT}.idc_v{idc_version}_dev.{table_prefix}{parent}` AS {parent_alias}
SET
    # We do not expect to redact an entire version
    {"redacted = redactions.redacted," if parent != 'version' else ""}
    `hashes` = STRUCT(
        if(`redactions`.`hashes`[0] IS NULL, "", `redactions`.`hashes`[0]) AS tcia_hash,
        if(`redactions`.`hashes`[1] IS NULL, "", `redactions`.`hashes`[1]) AS idc_hash,
        if(`redactions`.`hashes`[2] IS NULL, "", `redactions`.`hashes`[2]) AS all_hash
    )
FROM (
    # For each parent in the mitigation metadata table,
    # determine whether all its children have been redacted
    # and its hash
    SELECT
        {parent_alias}_{child_alias}.{parent}{'_uuid' if parent != 'version' else ''} parent_id,
        LOGICAL_AND({child_alias}.redacted) as redacted,
        if(LOGICAL_AND({child_alias}.redacted),
            # If all children are redacted, then the parent is redacted and its hashes are all null
            STRUCT(
                "" AS tcia_hash, 
                "" AS idc_hash, 
                "" AS all_hash),
            # Otherwise we separately aggregate the 'tcia', 'idc' and 'all' hash of each child. If a child is redacted,
            # its hashes are aggregated as NULLs 
            STRUCT( 
                IF(STRING_AGG({child_alias}.hashes.tcia_hash, "")="", "", 
                    TO_HEX(MD5(STRING_AGG(if({child_alias}.redacted,NULL,{child_alias}.hashes[0]) ,"" ORDER BY {child_alias}.hashes[0])))) AS tcia_hash,
                IF(STRING_AGG({child_alias}.hashes.idc_hash, "")="", "", 
                   TO_HEX(MD5(STRING_AGG(if({child_alias}.redacted,NULL,{child_alias}.hashes[1]) ,"" ORDER BY {child_alias}.hashes[1])))) AS idc_hash,
                if(STRING_AGG({child_alias}.hashes.all_hash, "")="", "", 
                   TO_HEX(MD5(STRING_AGG(if({child_alias}.redacted,NULL,{child_alias}.hashes[2]) ,"" ORDER BY {child_alias}.hashes[2])))) AS all_hash
                )
        ) hashes
    FROM `{settings.DEV_MITIGATION_PROJECT}.idc_v{idc_version}_dev.{parent}_{child}` {parent_alias}_{child_alias}
    JOIN `{settings.DEV_MITIGATION_PROJECT}.idc_v{idc_version}_dev.{table_prefix}{child}` {child_alias}
    ON {parent_alias}_{child_alias}.{child}_uuid = {child_alias}.uuid
    JOIN ( 
        SELECT DISTINCT {parent_alias}_uuid
        FROM `idc-dev-mitigation.mitigations.{mitigation_id}`
        WHERE {child_alias}_final_idc_version <= {idc_version}  
        OR ({child_alias}_rev_idc_version <= {idc_version}  
        AND ({idc_version} <= {child_alias}_final_idc_version OR {child_alias}_final_idc_version = 0 ))
        ) m
    ON {parent_alias}_{child_alias}.{parent}{'_uuid' if parent != 'version' else ''} = m.{parent_alias}_uuid
    GROUP BY parent_id
    ) redactions
# Given the table of instance level redactions, set the redaction state of the parent that are in this {idc_version}
WHERE {parent_alias}.{'uuid' if parent != 'version' else 'version'}=redactions.parent_id 
"""
    result = client.query(query)
    while result.state != 'DONE':
        result = client.get_job(result.job_id)
    if result.error_result != None:
        breakpoint()
    return


if __name__ == "__main__":
    client = bigquery.Client()

    for idc_version in range(18,19):
        progresslogger.info(f'Revising version {idc_version}')
        prefix = ''
        deprecate_instance(client, idc_version, prefix, mitigation_id="m1")
        progresslogger.info(f'\nRevised {prefix}instance')
        deprecate_series(client, idc_version, prefix, mitigation_id="m1", parent="series", child="instance",
                        parent_alias="se", child_alias="i", \
                        parent_id="series_instance_uid", child_id="sop_instance_uid")
        progresslogger.info(f'\nRevised {prefix}series')
        deprecate_level(client, idc_version, prefix, mitigation_id="m1",  parent="study", child="series",
                        parent_alias="st", child_alias="se", \
                        parent_id="study_instance_uid", child_id="series_instance_uid")
        progresslogger.info(f'\nRevised {prefix}study')
        deprecate_level(client, idc_version, prefix, mitigation_id="m1",  parent="patient", child="study", \
                        parent_alias="p", child_alias="st", \
                        parent_id="submitter_case_id", child_id="study_instance_uid")
        progresslogger.info(f'\nRevised {prefix}patient')
        deprecate_level(client, idc_version, prefix, mitigation_id="m1",  parent="collection", child="patient", \
                        parent_alias="c", child_alias="p", \
                        parent_id="collection_id", child_id="submitter_case_id")
        progresslogger.info(f'\nRevised {prefix}collection')
        deprecate_level(client, idc_version, prefix, mitigation_id="m1",  parent="version", child="collection", \
                        parent_alias="v", child_alias="c", \
                        parent_id="version", child_id="collection_id")
        progresslogger.info(f'\nRevised {prefix}collection')

