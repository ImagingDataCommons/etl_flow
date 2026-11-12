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

# Mark instances, series, etc. as redacted in PSQL tables that
# document all ingested IDC.
# These are internal tables, so we do not delete data, we just mark it as deprecated

import argparse
import settings
import json
from utilities.sqlalchemy_helpers import sa_session
from utilities.logging_config import successlogger, progresslogger, errlogger
from sqlalchemy import text

def revise_series_hashes(sess, idc_version, mitigation_id, parent, child, parent_alias):
    query = text(
        f"""

WITH seriess AS (
    SELECT *
    FROM series
    WHERE series.uuid IN (
        SELECT DISTINCT se_uuid from {mitigation_id}
    )   
)UPDATE series
SET
    hashes = hash_data.hs
FROM (
    -- For each parent compute its hashes
    SELECT
        seriess.uuid parent_id,
        
        if (seriess.redacted, 
--          -- If the series is redacted, set its hashes to NULLs
            CAST(ROW('','','') AS hashes), 
            -- else
            if(instance.source='tcia',
            -- then
                 CAST(ROW(
                    encode(decode(MD5(STRING_AGG(if(instance.redacted,NULL,instance.hash) ,'')), 'base64'), 'hex'),
                    '',
                    encode(decode(MD5(STRING_AGG(if(instance.redacted,NULL,instance.hash) ,'')), 'base64'), 'hex')
                    ) AS hashes),
            -- else
                CAST(ROW(
                    '',
                    encode(decode(MD5(STRING_AGG(if(instance.redacted,NULL,instance.hash) ,'')), 'base64'), 'hex'),
                    encode(decode(MD5(STRING_AGG(if(instance.redacted,NULL,instance.hash) ,'')), 'base64'), 'hex')
                    ) AS hashes) 
            )
        ) hs
    FROM seriess
    JOIN series_instance
    ON seriess.uuid = series_instance.series_uuid
    JOIN instance
    ON series_instance.instance_uuid = instance.uuid
    GROUP BY parent_id, instance.source, seriess.redacted
    ) hash_data
-- Given the table of instance level redactions, set the redaction state of the parent that are in this {idc_version}
WHERE series.uuid=hash_data.parent_id
RETURNING hash_data.parent_id, hash_data.hs, series.mitigation, series.redacted
""")

    try:
        result = sess.execute(query)
        updated_rows = result.all()
        for row in updated_rows:
            print(row)
    except Exception as exc:
        breakpoint()

    progresslogger.info(f'Redacted rows: {len([row for row in updated_rows if row[-2] == mitigation_id])} of {len(updated_rows)} rows')

    return


def revise_hashes(sess, idc_version, mitigation_id, parent, child, parent_alias):
    query = text(
        f"""
WITH parents AS (
    SELECT *
    FROM {parent}
    WHERE {parent}.uuid IN (
        SELECT DISTINCT {parent_alias}_uuid from {mitigation_id}
    )   
)
UPDATE {parent}
SET
    hashes = hash_data.hs
FROM (
    -- For each parent compute its hashes
    SELECT
        parents.uuid parent_id,
        CAST(ROW(
            IF(STRING_AGG(({child}).hashes.tcia, '')='', '',
                IF(STRING_AGG(({child}).hashes.tcia, '') IS NULL, '',
                    encode(decode(MD5(STRING_AGG(({child}).hashes.tcia ,'' ORDER BY ({child}).hashes.tcia)), 'base64'), 'hex'))),
            IF(STRING_AGG(({child}).hashes.idc, '')='', '',
                IF(STRING_AGG(({child}).hashes.idc, '') IS NULL, '',
                   encode(decode(MD5(STRING_AGG(({child}).hashes.idc ,'' ORDER BY ({child}).hashes.idc)), 'base64'), 'hex'))),
            if(STRING_AGG(({child}).hashes.all_sources, '')='', '',
                if(STRING_AGG(({child}).hashes.all_sources, '') IS NULL, '',
                   encode(decode(MD5(STRING_AGG(({child}).hashes.all_sources,'' ORDER BY ({child}).hashes.all_sources)), 'base64'), 'hex')))
            ) AS hashes
        ) hs
    FROM parents
    JOIN {parent}_{child}
    ON parents.uuid = {parent}_{child}.{parent}_uuid
    JOIN {child}
    ON {parent}_{child}.{child}_uuid = {child}.uuid
--     JOIN {mitigation_id}
--     ON parents.uuid = {mitigation_id}.{parent_alias}_uuid
    GROUP BY parent_id
    ) hash_data
-- Given the table of instance level redactions, set the redaction state of the parent that are in this {idc_version}
WHERE {parent}.uuid=hash_data.parent_id
RETURNING hash_data.parent_id, hash_data.hs, {parent}.mitigation, {parent}.redacted
""")

    try:
        result = sess.execute(query)
        updated_rows = result.all()
        for row in updated_rows:
            print(row)
    except Exception as exc:
        breakpoint()

    progresslogger.info(f'Redacted rows: {len([row for row in updated_rows if row[-2] == mitigation_id])} of {len(updated_rows)} rows')

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=settings.CURRENT_VERSION, help='IDC version number')
    parser.add_argument('--mitigation_id', default=f'm{settings.MITIGATION_VERSION}', help='ID of this mitigation event')
    args = parser.parse_args()

    progresslogger.info(f'args: {json.dumps(args.__dict__, indent=2)}')

    with sa_session(echo=False) as sess:

        idc_version = args.version
        progresslogger.info(f'Revising version {idc_version}')
        revise_series_hashes(sess, idc_version, mitigation_id=args.mitigation_id, parent="series", child="instance",
                        parent_alias="se")
        progresslogger.info(f'\nRevised series')
        revise_hashes(sess, idc_version, mitigation_id=args.mitigation_id,  parent="study", child="series",
                        parent_alias="st")
        progresslogger.info(f'\nRevised study')
        revise_hashes(sess, idc_version, mitigation_id=args.mitigation_id,  parent="patient", child="study", \
                        parent_alias="p")
        progresslogger.info(f'\nRevised patient')
        revise_hashes(sess, idc_version, mitigation_id=args.mitigation_id,  parent="collection", child="patient", \
                        parent_alias="c")
        progresslogger.info(f'\nRevised collection')

        sess.commit()


