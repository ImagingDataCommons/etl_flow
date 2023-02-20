--creating a temporary table that is flatenned on most columns using left joins and 
--unnesting upto three levels starting with zeroth level
--the temporary table could be very helpful to visualize.
--Once the table is flattened,
--sub tables are created, one for each attribute of interest.
--then they are joined on measurement group numbers and sop instance uid.


-- Start by creating a temporary table called 'temp'
with temp as (
-- In the SELECT statement, we're choosing which columns to include in the temporary table
SELECT
SOPInstanceUID, 
measurementGroup_number, -- Number assigned to a measurement group
cs_l2.UID, -- Unique identifier for the content sequence at level 2
cs_l2.TextValue, -- Text value associated with the content sequence at level 2
PatientID, 
SeriesDescription, 
SOPClassUID,
cts_l0.MappingResource, -- Resource used to map the content template sequence at level 0
cts_l0.TemplateIdentifier, -- Unique identifier for the content template sequence at level 0
cs_l0.ValueType, -- Type of value associated with the content sequence at level 0
cs_l1_cncs.CodeMeaning, -- Code meaning associated with the concept name coding sequence at level 1
cs_l2.ValueType, -- Type of value associated with the content sequence at level 2
cs_l2_cncs.CodeValue, -- Code value associated with the concept name coding sequence at level 2
cs_l2_cncs.CodingSchemeDesignator, -- Coding scheme designator associated with the concept name coding sequence at level 2
cs_l2_cncs.CodeMeaning as cm2, -- Code meaning associated with the concept name coding sequence at level 2, with an alias of 'cm2'
cs_l2_rss.ReferencedSOPClassUID, -- Unique identifier for the referenced SOP class
cs_l2_rss.ReferencedSOPInstanceUID, -- Unique identifier for the referenced SOP instance
cs_l2_rss.ReferencedSegmentNumber, -- Number assigned to a referenced segment
cs_l2_css, -- concept Code sequence associated with the content sequence at level 2
cs_l1 -- Content sequence at level 1
FROM
idc-dev-etl.idc_v13_pub.dicom_metadata bid -- Data source
-- Left join zeroth level of ContentTemplateSequence
LEFT JOIN
UNNEST(bid.ContentTemplateSequence) cts_l0
-- Left join zeroth level of ContentSequence
LEFT JOIN
UNNEST(bid.ContentSequence) cs_l0
-- Unnest content sequence at level 1, with an offset assigned to measurementGroup_number
LEFT JOIN
unnest(cs_l0.ContentSequence) cs_l1
WITH
OFFSET
AS measurementGroup_number
-- Left join ConceptNameCodeSequence at level 1
LEFT JOIN
unnest(cs_l1.ConceptNameCodeSequence) cs_l1_cncs
-- Unnest content sequence at level 2
LEFT JOIN
unnest(cs_l1.ContentSequence) cs_l2
-- Left join ConceptNameCodeSequence at level 2
LEFT JOIN
unnest(cs_l2.ConceptNameCodeSequence) cs_l2_cncs
-- Left join ReferencedSOPSequence at level 2
LEFT JOIN
unnest(cs_l2.ReferencedSOPSequence) cs_l2_rss
-- Left join ConceptCodeSequence at level 2
LEFT JOIN
unnest(cs_l2.ConceptCodeSequence) cs_l2_css
WHERE
--SeriesDescription in ("BPR landmark annotations") and
-- We only want to include records where the TemplateIdentifier is 1500 and MappingResource is DCMR
TemplateIdentifier IN ('1500')
AND MappingResource IN ('DCMR')

-- We only want to include CONTAINER value types in the first level of content sequence
AND cs_l0.ValueType IN ('CONTAINER')

-- We only want to include Measurement Group Code Meanings in the second level of content sequence
AND cs_l1_cncs.CodeMeaning IN ("Measurement Group")

-- We want to include certain value types and code values in the third level of content sequence
AND (
  -- TEXT value type with specific Code Value and Coding Scheme Designator
  (cs_l2.ValueType IN ('TEXT') AND cs_l2_cncs.CodeValue IN ('112039')AND cs_l2_cncs.CodingSchemeDesignator IN ('DCM')) 
  -- UIDREF value type with specific Code Value and Coding Scheme Designator
  OR (cs_l2.ValueType IN ('UIDREF') AND cs_l2_cncs.CodeValue IN ('112040')AND cs_l2_cncs.CodingSchemeDesignator IN ('DCM'))  
  -- IMAGE value type with specific Referenced SOP Class UID
  OR (cs_l2.ValueType IN ('IMAGE') AND  cs_l2_rss.ReferencedSOPClassUID IN ("1.2.840.10008.5.1.4.1.1.66.4"))
  -- UIDREF value type with specific Code Value and Coding Scheme Designator
  OR (cs_l2.ValueType IN ('UIDREF') AND cs_l2_cncs.CodeValue IN ('121232')AND cs_l2_cncs.CodingSchemeDesignator IN ('DCM')) 
  -- CODE value type with specific Code Value and Coding Scheme Designator
  OR (cs_l2.ValueType IN ('CODE') AND cs_l2_cncs.CodeValue IN ('121071')AND cs_l2_cncs.CodingSchemeDesignator IN ('DCM')) 
  -- CODE value type with specific Code Value and Coding Scheme Designator
  OR (cs_l2.ValueType IN ('CODE') AND cs_l2_cncs.CodeValue IN ('G-C0E3')AND cs_l2_cncs.CodingSchemeDesignator IN ('SRT'))
  -- CODE value type with specific Code Value and Coding Scheme Designator
  OR (cs_l2.ValueType IN ('CODE') AND cs_l2_cncs.CodeValue IN ('363698007')AND cs_l2_cncs.CodingSchemeDesignator IN ('SCT'))  
)
-- We only want to include certain SOP Class UIDs
AND SOPClassUID IN ("1.2.840.10008.5.1.4.1.1.88.11", "1.2.840.10008.5.1.4.1.1.88.22", "1.2.840.10008.5.1.4.1.1.88.33","1.2.840.10008.5.1.4.1.1.88.34","1.2.840.10008.5.1.4.1.1.88.35" )

-- We could activate the below line for testing
--AND SOPInstanceUID in ('1.2.276.0.7230010.3.1.4.0.11647.1553294587.292373')

),
finding as (SELECT * from temp where cm2 ='Finding'),
findingsite as (SELECT * from temp where cm2 ='Finding Site'),
ReferencedSegment as (SELECT * from temp where cm2 ='Referenced Segment'),
SourceSeriesforsegmentation as (SELECT * from temp where cm2 ='Source series for segmentation'),
TrackingIdentifier as (SELECT * from temp where cm2 ='Tracking Identifier'),
TrackingUniqueIdentifier as  (SELECT * from temp where cm2 ='Tracking Unique Identifier')

Select
TrackingIdentifier.SOPInstanceUID, 
TrackingIdentifier.measurementGroup_number,
TrackingUniqueIdentifier.UID as trackingUniqueidentifier,
TrackingIdentifier.TextValue as trackingidentifier,
TrackingIdentifier.PatientID,
TrackingIdentifier.SeriesDescription, 

SourceSeriesforsegmentation.UID as sourceSegmentedSeriesUID,
ReferencedSegment.ReferencedSOPInstanceUID as segmentationInstanceUID,
ReferencedSegment.ReferencedSegmentNumber as segmentationSegmentNumber,

ContentSequence1.ConceptNameCodeSequence as Quantity, --different from measurements query
ContentSequence1.ConceptCodeSequence as derivationModifier,--different from measurements query and qualitative measurements
(SAFE_CAST((MeasuredValueSequence.NumericValue[SAFE_OFFSET(0)]) AS NUMERIC )) AS Value,--different from measurements query and qualitative measurements
MeasurementUnitsCodeSequence AS Units,--different from measurements query and qualitative measurements

finding.cs_l2_css as finding,
findingsite.cs_l2_css as findingSite,

from TrackingIdentifier
join TrackingUniqueIdentifier using (SOPInstanceUID, measurementGroup_number)
left join finding using (SOPInstanceUID, measurementGroup_number)
left join findingsite using (SOPInstanceUID, measurementGroup_number)
left join ReferencedSegment using (SOPInstanceUID, measurementGroup_number)
left join SourceSeriesforsegmentation using (SOPInstanceUID, measurementGroup_number)
--the next two lines are different from measurements query and the next two lines are different from qualitative measurements query
left join unnest(TrackingIdentifier.cs_l1.ContentSequence) as ContentSequence1
left join unnest(ContentSequence1.ConceptNameCodeSequence) as ConceptNameCodeSequence2
left join unnest(ContentSequence1.MeasuredValueSequence) as MeasuredValueSequence
left join unnest(MeasuredValueSequence.MeasurementUnitsCodeSequence) as MeasurementUnitsCodeSequence
--the filter applied is changed from CODE in qualitative measurements query to NUM in the quantitative query
where ContentSequence1.ValueType in ('NUM') --and ConceptNameCodeSequence2.CodeValue not in ('121071','G-C0E3') --and finding.PatientID in ("LIDC-IDRI-0001")