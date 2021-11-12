SELECT blob_name
FROM
(SELECT
  CONCAT(i.uuid,'.dcm') AS blob_name
FROM
  `idc-dev-etl.idc_v5.collection` AS c
JOIN
  `idc-dev-etl.idc_v5.open_collections` AS o
ON
  c.collection_id = o.tcia_api_collection_id
JOIN
  `idc-dev-etl.idc_v5.patient` AS p
ON
  o.tcia_api_collection_id = p.collection_id
JOIN
  `idc-dev-etl.idc_v5.study` AS st
ON
  p.submitter_case_id =st.submitter_case_id
JOIN
  `idc-dev-etl.idc_v5.series` AS se
ON
  st.study_instance_uid = se.study_instance_uid
JOIN
  `idc-dev-etl.idc_v5.instance` AS i
ON
  se.series_instance_uid = i.series_instance_uid
UNION ALL
SELECT
  CONCAT(r.instance_uuid,'.dcm') AS blob_name
FROM
  `idc-dev-etl.idc_v5.retired` AS r
JOIN
  `idc-dev-etl.idc_v5.open_collections` AS o
ON
  r.collection_id = o.tcia_api_collection_id)
ORDER BY blob_name