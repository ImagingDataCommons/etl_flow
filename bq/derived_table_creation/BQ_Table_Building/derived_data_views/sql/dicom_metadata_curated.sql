SELECT
  SOPInstanceUID,
  SAFE_CAST(SliceThickness AS FLOAT64) AS SliceThickness
FROM
  `{project}.{dataset}.dicom_metadata` AS dcm
