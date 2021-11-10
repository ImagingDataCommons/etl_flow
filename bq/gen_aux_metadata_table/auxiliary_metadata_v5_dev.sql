WITH
  coll_stat AS (
  SELECT o.tcia_api_collection_id, o.{target}_url as url, o.access
  FROM
    `idc-dev-etl.idc_v{version}.open_collections` as o
  UNION ALL
  SELECT cr.tcia_api_collection_id, cr.{target}_url as url, cr.access
  FROM
    `idc-dev-etl.idc_v{version}.cr_collections` as cr
  UNION ALL
  SELECT r.tcia_api_collection_id, r.{target}_url as url, r.access
  FROM
    `idc-dev-etl.idc_v{version}.redacted_collections` as r
  UNION ALL
  SELECT d.tcia_api_collection_id, d.{target}_url as url, d.access
  FROM
    `idc-dev-etl.idc_v{version}.defaced_collections` as d),
  series_hashes AS(
  SELECT
    se.series_instance_uid,
    TO_HEX(md5(STRING_AGG(i.HASH, ''
        ORDER BY
          i.HASH ASC))) AS hash_all,
    se.study_instance_uid
  FROM
    `idc-dev-etl.idc_v{version}.instance` AS i
  JOIN
    `idc-dev-etl.idc_v{version}.series` AS se
  ON
    se.series_instance_uid = i.series_instance_uid
  GROUP BY
    se.series_instance_uid,
    se.study_instance_uid ),
  series_hash_all AS (
  SELECT
    se.* except (source_doi),
    IF(STARTS_WITH(source_doi,'doi:'), SUBSTR(source_doi,5),IF(STARTS_WITH(source_doi,' '), SUBSTR(source_doi,2), IF(source_doi='https://wiki.cancerimagingarchive.net/x/N4NyAQ', '', source_doi))) AS source_doi,
    seh.hash_all
  FROM
    `idc-dev-etl.idc_v{version}.series` AS se
  JOIN
    series_hashes AS seh
  ON
    se.series_instance_uid = seh.series_instance_uid ),
  study_hashes AS(
  SELECT
    st.study_instance_uid,
    TO_HEX(md5(STRING_AGG(se.hash_all, ''
        ORDER BY
          se.hash_all ASC))) AS hash_all,
    st.submitter_case_id
  FROM
    series_hashes AS se
  JOIN
    `idc-dev-etl.idc_v{version}.study` AS st
  ON
    st.study_instance_uid = se.study_instance_uid
  GROUP BY
    st.study_instance_uid,
    st.submitter_case_id ),
  study_hash_all AS (
  SELECT
    st.*,
    sth.hash_all
  FROM
    `idc-dev-etl.idc_v{version}.study` AS st
  JOIN
    study_hashes AS sth
  ON
    st.study_instance_uid = sth.study_instance_uid ),
  patient_hashes AS(
  SELECT
    p.submitter_case_id,
    TO_HEX(md5(STRING_AGG(st.hash_all, ''
        ORDER BY
          st.hash_all ASC))) AS hash_all,
    p.collection_id
  FROM
    study_hashes AS st
  JOIN
    `idc-dev-etl.idc_v{version}.patient` AS p
  ON
    p.submitter_case_id = st.submitter_case_id
  GROUP BY
    p.submitter_case_id,
    p.collection_id ),
  patient_hash_all AS (
  SELECT
    p.*,
    ph.hash_all
  FROM
    `idc-dev-etl.idc_v{version}.patient` AS p
  JOIN
    patient_hashes AS ph
  ON
    p.submitter_case_id = ph.submitter_case_id ),
  collection_hashes AS(
  SELECT
    c.collection_id,
    TO_HEX(md5(STRING_AGG(p.hash_all, ''
        ORDER BY
          p.hash_all ASC))) AS hash_all
  FROM
    patient_hashes AS p
  JOIN
    `idc-dev-etl.idc_v{version}.collection` AS c
  ON
    c.collection_id = p.collection_id
  GROUP BY
    c.collection_id ),
  collection_hash_all AS (
  SELECT
    c.*,
    o.Access as Access,
    ch.hash_all AS hash_all
  FROM
    `idc-dev-etl.idc_v{version}.collection` AS c
  JOIN
    collection_hashes AS ch
  ON
    c.collection_id = ch.collection_id
  JOIN `idc-dev-etl.idc_v{version}.original_collections_metadata` as o
  ON
    c.collection_id = o.tcia_api_collection_id),
  version_hashes AS(
  SELECT
    TO_HEX(md5(STRING_AGG(c.hash_all, ''
        ORDER BY
          c.hash_all ASC))) AS hash_all
  FROM
    collection_hashes AS c ),
  version_hash_all AS (
  SELECT
    v.*,
    vh.hash_all
  FROM
    `idc-dev-etl.idc_v{version}.version` AS v
  JOIN
    version_hashes AS vh
  ON
    1=1 ),
  license_info AS (
  SELECT
    DOI,
    license_url,
    license_long_name,
    license_short_name
  FROM
    `idc-dev-etl.idc_v{version}.original_collections_metadata`
  UNION ALL
  SELECT
    DOI,
    license_url,
    license_long_name,
    license_short_name
  FROM
    `idc-dev-etl.idc_v{version}.analysis_results_metadata` )
SELECT
  --      3 AS idc_version,
  --      v.max_timestamp AS version_timestamp,
  --     v.hash_all AS version_hash,
  c.collection_id AS tcia_api_collection_id,
  REPLACE(REPLACE(LOWER(c.collection_id),'-','_'), ' ','_') AS idc_webapp_collection_id,
  c.min_timestamp as collection_timestamp,
  c.hash_all AS collection_hash,
  c.init_idc_version AS collection_init_idc_version,
  c.rev_idc_version AS collection_revised_idc_version,
--   c.Access As access,
  coll_stat.access AS access,
  p.submitter_case_id AS submitter_case_id,
  p.idc_case_id AS idc_case_id,
  p.hash_all AS patient_hash,
  p.init_idc_version AS patient_init_idc_version,
  p.rev_idc_version AS patient_revised_idc_version,
  st.study_instance_uid AS StudyInstanceUID,
  st.uuid AS study_uuid,
  st.study_instances AS study_instances,
  st.hash_all AS study_hash,
  st.init_idc_version AS study_init_idc_version,
  st.rev_idc_version AS study_revised_idc_version,
  se.series_instance_uid AS SeriesInstanceUID,
  se.uuid AS series_uuid,
  IF(c.collection_id='APOLLO', '', se.source_doi) AS source_doi,
  se.series_instances AS series_instances,
  se.hash_all AS series_hash,
  se.init_idc_version AS series_init_idc_version,
  se.rev_idc_version AS series_revised_idc_version,
  i.sop_instance_uid AS SOPInstanceUID,
  i.uuid AS instance_uuid,
--   CONCAT('gs://{gcs_bucket}/', i.uuid, '.dcm') as gcs_url,
  CONCAT('gs://', coll_stat.url, '/', i.uuid, '.dcm') as gcs_url,
  i.size AS instance_size,
  i.hash AS instance_hash,
  i.init_idc_version AS instance_init_idc_version,
  i.rev_idc_version AS instance_revised_idc_version,
  li.license_url AS license_url,
  li.license_long_name AS license_long_name,
  li.license_short_name AS license_short_name
FROM
  version_hash_all AS v
JOIN
  collection_hash_all AS c
ON
  1=1
JOIN
  coll_stat
ON
  c.collection_id = coll_stat.tcia_api_collection_id
JOIN
  patient_hash_all AS p
ON
  c.collection_id = p.collection_id
JOIN
  study_hash_all AS st
ON
  p.submitter_case_id = st.submitter_case_id
JOIN
  series_hash_all AS se
ON
  st.study_instance_uid = se.study_instance_uid
JOIN
  `idc-dev-etl.idc_v{version}.instance` AS i
ON
  se.series_instance_uid = i.series_instance_uid
LEFT JOIN
  `idc-dev-etl.idc_v{version}.excluded_collections` AS ex
ON
  LOWER(c.collection_id) = LOWER(ex.tcia_api_collection_id)
JOIN
  license_info AS li
ON
  se.source_doi = li.DOI
WHERE
  ex.tcia_api_collection_id IS NULL