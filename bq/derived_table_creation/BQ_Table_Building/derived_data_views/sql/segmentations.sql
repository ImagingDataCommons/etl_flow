WITH
  segmentations AS (
  WITH
    segs AS (
    SELECT
      PatientID,
      StudyInstanceUID,
      SeriesInstanceUID,
      SOPInstanceUID,
      FrameOfReferenceUID,
      SegmentSequence
    FROM
      `{0}`
    WHERE
      # more reliable than Modality = "SEG"
      SOPClassUID = "1.2.840.10008.5.1.4.1.1.66.4" )
  SELECT
    PatientID,
    StudyInstanceUID,
    SeriesInstanceUID,
    SOPInstanceUID,
    FrameOfReferenceUID,
    CASE ARRAY_LENGTH(unnested.AnatomicRegionSequence)
      WHEN 0 THEN NULL
    ELSE
    STRUCT( unnested.AnatomicRegionSequence [
    OFFSET
      (0)].CodeValue AS CodeValue,
      unnested.AnatomicRegionSequence [
    OFFSET
      (0)].CodingSchemeDesignator AS CodingSchemeDesignator,
      unnested.AnatomicRegionSequence [
    OFFSET
      (0)].CodeMeaning AS CodeMeaning )
  END
    AS AnatomicRegion,
    CASE ( ARRAY_LENGTH(unnested.AnatomicRegionSequence) > 0
      AND ARRAY_LENGTH( unnested.AnatomicRegionSequence [
      OFFSET
        (0)].AnatomicRegionModifierSequence ) > 0 )
      WHEN TRUE THEN unnested.AnatomicRegionSequence [ OFFSET (0)].AnatomicRegionModifierSequence [ OFFSET (0)] #unnested.AnatomicRegionSequence[OFFSET(0)].AnatomicRegionModifierSequence,
    ELSE
    NULL
  END
    AS AnatomicRegionModifier,
    CASE ARRAY_LENGTH(unnested.SegmentedPropertyCategoryCodeSequence)
      WHEN 0 THEN NULL
    ELSE
    unnested.SegmentedPropertyCategoryCodeSequence [
  OFFSET
    (0)]
  END
    AS SegmentedPropertyCategory,
    CASE ARRAY_LENGTH(unnested.SegmentedPropertyTypeCodeSequence)
      WHEN 0 THEN NULL
    ELSE
    unnested.SegmentedPropertyTypeCodeSequence [
  OFFSET
    (0)]
  END
    AS SegmentedPropertyType,
    #unnested.SegmentedPropertyTypeCodeSequence,
    #unnested.SegmentedPropertyTypeModifierCodeSequence,
    unnested.SegmentAlgorithmType,
	unnested.SegmentAlgorithmName,
    unnested.SegmentNumber,
    unnested.TrackingUID,
    unnested.TrackingID
  FROM
    segs
  CROSS JOIN
    UNNEST(SegmentSequence) AS unnested),
  sampled_sops AS (
  SELECT
    SOPInstanceUID AS seg_SOPInstanceUID,
    ReferencedSeriesSequence[SAFE_OFFSET(0)].ReferencedInstanceSequence[SAFE_OFFSET(0)].ReferencedSOPInstanceUID AS rss_one,
    ReferencedImageSequence[SAFE_OFFSET(0)].ReferencedSOPInstanceUID AS ris_one,
    SourceImageSequence[SAFE_OFFSET(0)].ReferencedSOPInstanceUID AS sis_one
  FROM
    `{1}`
  WHERE
    Modality="SEG"
    AND SOPClassUID = "1.2.840.10008.5.1.4.1.1.66.4" ),
  coalesced_ref AS (
  SELECT
    *,
    COALESCE(rss_one, ris_one, sis_one) AS referenced_sop
  FROM
    sampled_sops)
SELECT
  segmentations.*,
  dicom_all.SeriesInstanceUID AS segmented_SeriesInstanceUID,
  CONCAT("https://viewer.imaging.datacommons.cancer.gov/viewer/", segmentations.StudyInstanceUID,"?seriesInstanceUID=",segmentations.SeriesInstanceUID,",",dicom_all.SeriesInstanceUID) AS viewer_url,
FROM
  coalesced_ref
JOIN
  `{1}` AS dicom_all
ON
  coalesced_ref.referenced_sop = dicom_all.SOPInstanceUID
RIGHT JOIN
  segmentations
ON
  segmentations.SOPInstanceUID = coalesced_ref.seg_SOPInstanceUID