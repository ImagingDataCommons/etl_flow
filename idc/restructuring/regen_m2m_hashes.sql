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

# This SQL regenerates hierarchical study, patient, collection and version hashes in the m2m DB.
# It assumes that series hashes are correct.
# The hierarchy generated only includes series for which Access == 'Public'.

BEGIN;
WITH seriess AS (
  SELECT distinct uuid, hash
  FROM series se
  JOIN all_sources a_s
  ON se.source_doi = a_s.source_doi
  WHERE a_s.Access = 'Public'
),
study_hashes AS (
SELECT DISTINCT st.uuid, MD5(STRING_AGG(ses.hash, '' ORDER BY ses.hash)) hash
FROM study st
JOIN study_series st_se
ON st.uuid = st_se.study_uuid
JOIN seriess ses 
ON st_se.series_uuid = ses.uuid
GROUP BY st.uuid
)
UPDATE study st
SET hash = st_h.hash
FROM study_hashes st_h
where st.uuid = st_h.uuid
;

WITH studiess AS (
  SELECT distinct st.uuid, st.hash
  FROM study st
  JOIN study_series st_se
  ON st.uuid = st_se.study_uuid
  JOIN series se
  ON st_se.series_uuid = se.uuid
  JOIN all_sources a_s
  ON se.source_doi = a_s.source_doi
  WHERE a_s.Access = 'Public'
),
patient_hashes AS (
SELECT DISTINCT p.uuid, MD5(STRING_AGG(sts.hash, '' ORDER BY sts.hash)) hash
FROM patient p
JOIN patient_study p_st
ON p.uuid = p_st.patient_uuid
JOIN studiess sts
ON p_st.study_uuid = sts.uuid
GROUP BY p.uuid
)
UPDATE patient p
SET hash = p_h.hash
FROM patient_hashes p_h
WHERE p.uuid = p_h.uuid
;

WITH patients AS (
  SELECT distinct p.uuid, p.hash
  FROM patient p
  JOIN patient_study p_s
  ON p.uuid = p_s.patient_uuid
  JOIN study st
  ON p_s.study_uuid = st.uuid
  JOIN study_series st_se
  ON st.uuid = st_se.study_uuid
  JOIN series se
  ON st_se.series_uuid = se.uuid
  JOIN all_sources a_s
  ON se.source_doi = a_s.source_doi
  WHERE a_s.Access = 'Public'
),
collection_hashes AS (
SELECT c.uuid, MD5(STRING_AGG(ps.hash, '' ORDER BY ps.hash)) hash
FROM collection c
JOIN collection_patient c_p
ON c.uuid = c_p.collection_uuid
JOIN patients ps
ON c_p.patient_uuid = ps.uuid
GROUP BY c.uuid
)
UPDATE collection c
SET hash = c_h.hash
FROM collection_hashes c_h
WHERE c.uuid = c_h.uuid
;

WITH collections AS (
  SELECT distinct c.uuid, c.hash
  FROM collection c
  JOIN collection_patient c_p
  ON c.uuid = c_p.collection_uuid
  JOIN patient p
  ON c_p.patient_uuid = p.uuid
  JOIN patient_study p_s
  ON p.uuid = p_s.patient_uuid
  JOIN study st
  ON p_s.study_uuid = st.uuid
  JOIN study_series st_se
  ON st.uuid = st_se.study_uuid
  JOIN series se
  ON st_se.series_uuid = se.uuid
  JOIN all_sources a_s
  ON se.source_doi = a_s.source_doi
  WHERE a_s.Access = 'Public'
),
version_hashes AS(
SELECT v.version, MD5(STRING_AGG(cs.hash, '' ORDER BY cs.hash)) hash
FROM version v
JOIN version_collection v_c
ON v.version = v_c.version
JOIN collections cs
ON v_c.collection_uuid = cs.uuid
GROUP by v.version
)
UPDATE version v
SET hash = v_h.hash
FROM version_hashes v_h
WHERE v.version = v_h.version
;

END;
