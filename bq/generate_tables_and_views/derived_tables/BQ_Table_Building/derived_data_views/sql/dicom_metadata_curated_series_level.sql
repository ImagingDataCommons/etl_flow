WITH
  temp_table AS (
  SELECT
    dicom_all.SeriesInstanceUID,
    ANY_VALUE(Modality) AS Modality,
    STRING_AGG(DISTINCT(collection_id),",") AS collection_id,
    ANY_VALUE(OpticalPathSequence[SAFE_OFFSET(0)].ObjectiveLensPower) AS ObjectiveLensPower,
    MAX(DISTINCT(TotalPixelMatrixColumns)) AS max_TotalPixelMatrixColumns,
    MAX(DISTINCT(TotalPixelMatrixRows)) AS max_TotalPixelMatrixRows,
    MAX(DISTINCT(`Columns`)) AS max_Columns,
    MAX(DISTINCT(`Rows`)) AS max_Rows,
    MIN(DISTINCT(SAFE_CAST(PixelSpacing[SAFE_OFFSET(0)] AS FLOAT64))) AS min_spacing_0,
    MIN(SAFE_CAST(SharedFunctionalGroupsSequence[SAFE_OFFSET(0)].PixelMeasuresSequence[SAFE_OFFSET(0)]. PixelSpacing[SAFE_OFFSET(0)] AS FLOAT64)) AS fg_min_spacing_0,
    ARRAY_AGG(DISTINCT(CONCAT(SpecimenDescriptionSequence[SAFE_OFFSET(0)].PrimaryAnatomicStructureSequence[SAFE_OFFSET(0)].CodingSchemeDesignator,":", SpecimenDescriptionSequence[SAFE_OFFSET(0)].PrimaryAnatomicStructureSequence[SAFE_OFFSET(0)].CodeValue, ":", SpecimenDescriptionSequence[SAFE_OFFSET(0)].PrimaryAnatomicStructureSequence[SAFE_OFFSET(0)].CodeMeaning)) IGNORE NULLS)[SAFE_OFFSET(0)] AS primaryAnatomicStructure_code_str,
    ARRAY_AGG(DISTINCT(CONCAT(SpecimenDescriptionSequence[SAFE_OFFSET(0)].PrimaryAnatomicStructureSequence[SAFE_OFFSET(0)].PrimaryAnatomicStructureModifierSequence[SAFE_OFFSET(0)].CodingSchemeDesignator,":", SpecimenDescriptionSequence[SAFE_OFFSET(0)].PrimaryAnatomicStructureSequence[SAFE_OFFSET(0)].PrimaryAnatomicStructureModifierSequence[SAFE_OFFSET(0)].CodeValue, ":", SpecimenDescriptionSequence[SAFE_OFFSET(0)].PrimaryAnatomicStructureSequence[SAFE_OFFSET(0)].PrimaryAnatomicStructureModifierSequence[SAFE_OFFSET(0)].CodeMeaning)) IGNORE NULLS)[SAFE_OFFSET(0)] AS primaryAnatomicStructureModifier_code_str,

    ARRAY_AGG(DISTINCT(CONCAT(OpticalPathSequence[SAFE_OFFSET(0)].IlluminationTypeCodeSequence[SAFE_OFFSET(0)].CodingSchemeDesignator,":", OpticalPathSequence[SAFE_OFFSET(0)].IlluminationTypeCodeSequence[SAFE_OFFSET(0)].CodeValue, ":", OpticalPathSequence[SAFE_OFFSET(0)].IlluminationTypeCodeSequence[SAFE_OFFSET(0)].CodeMeaning)) IGNORE NULLS)[SAFE_OFFSET(0)] AS illuminationType_code_str,
  FROM
    `{project}.{dataset}.dicom_all` AS dicom_all
  GROUP BY
    SeriesInstanceUID
  ),

SpecimenPreparationSequence_unnested AS (
      SELECT
        SeriesInstanceUID,
        concept_name_code_sequence.CodeMeaning AS cnc_cm,
        concept_name_code_sequence.CodingSchemeDesignator AS cnc_csd,
        concept_name_code_sequence.CodeValue AS cnc_val,
        concept_code_sequence.CodeMeaning AS ccs_cm,
        concept_code_sequence.CodingSchemeDesignator AS ccs_csd,
        concept_code_sequence.CodeValue AS ccs_val,
      FROM `bigquery-public-data.idc_v18.dicom_all`,
      UNNEST(SpecimenDescriptionSequence[SAFE_OFFSET(0)].SpecimenPreparationSequence) as preparation_unnest_step1,
      UNNEST(preparation_unnest_step1.SpecimenPreparationStepContentItemSequence) as preparation_unnest_step2,
      UNNEST(preparation_unnest_step2.ConceptNameCodeSequence) as concept_name_code_sequence,
      UNNEST(preparation_unnest_step2.ConceptCodeSequence) as concept_code_sequence
    ),

    slide_embedding AS (
    SELECT
      SeriesInstanceUID,
      ARRAY_AGG(DISTINCT(CONCAT(ccs_cm,":",ccs_csd,":",ccs_val))) as embeddingMedium_code_str
    FROM SpecimenPreparationSequence_unnested
    WHERE (cnc_csd = 'SCT' and cnc_val = '430863003') -- CodeMeaning is 'Embedding medium'
    GROUP BY SeriesInstanceUID
    ),

    slide_fixative AS (
    SELECT
      SeriesInstanceUID,
      ARRAY_AGG(DISTINCT(CONCAT(ccs_cm, ":", ccs_csd,":",ccs_val))) as tissueFixative_code_str
    FROM SpecimenPreparationSequence_unnested
    WHERE (cnc_csd = 'SCT' and cnc_val = '430864009') -- CodeMeaning is 'Tissue Fixative'
    GROUP BY SeriesInstanceUID
    ),

    slide_staining AS (
    SELECT
      SeriesInstanceUID,
      ARRAY_AGG(DISTINCT(CONCAT(ccs_cm, ":", ccs_csd,":",ccs_val))) as staining_usingSubstance_code_str,
    FROM SpecimenPreparationSequence_unnested
    WHERE (cnc_csd = 'SCT' and cnc_val = '424361007') -- CodeMeaning is 'Using substance'
    GROUP BY SeriesInstanceUID
    )

SELECT
  temp_table.SeriesInstanceUID,
  temp_table.Modality,
  -- Embedding Medium
  ARRAY(
    SELECT IF(code IS NULL, NULL, SPLIT(code, ':')[SAFE_OFFSET(0)])
    FROM UNNEST(embeddingMedium_code_str) AS code
  ) AS embeddingMedium_CodeMeaning,
  ARRAY(
    SELECT IF(code IS NULL, NULL,
              IF(STRPOS(code, ':') = 0, NULL,
                 SUBSTR(code, STRPOS(code, ':') + 1)))
    FROM UNNEST(embeddingMedium_code_str) AS code
  ) AS embeddingMedium_code_designator_value_str,
  -- Tissue Fixative
  ARRAY(
    SELECT IF(code IS NULL, NULL, SPLIT(code, ':')[SAFE_OFFSET(0)])
    FROM UNNEST(tissueFixative_code_str) AS code
  ) AS tissueFixative_CodeMeaning,
  ARRAY(
    SELECT IF(code IS NULL, NULL,
              IF(STRPOS(code, ':') = 0, NULL,
                 SUBSTR(code, STRPOS(code, ':') + 1)))
    FROM UNNEST(tissueFixative_code_str) AS code
  ) AS tissueFixative_code_designator_value_str,
  -- Staining using substance
  ARRAY(
    SELECT IF(code IS NULL, NULL, SPLIT(code, ':')[SAFE_OFFSET(0)])
    FROM UNNEST(staining_usingSubstance_code_str) AS code
  ) AS staining_usingSubstance_CodeMeaning,
  ARRAY(
    SELECT IF(code IS NULL, NULL,
              IF(STRPOS(code, ':') = 0, NULL,
                 SUBSTR(code, STRPOS(code, ':') + 1)))
    FROM UNNEST(staining_usingSubstance_code_str) AS code
  ) AS staining_usingSubstance_code_designator_value_str,

  if(COALESCE(min_spacing_0, fg_min_spacing_0) = 0, 0,
    round(COALESCE(min_spacing_0, fg_min_spacing_0) ,CAST(2 -1-floor(log10(abs(COALESCE(min_spacing_0, fg_min_spacing_0) ))) AS INT64))) AS min_PixelSpacing_2sf,
  COALESCE(max_TotalPixelMatrixColumns, max_Columns) AS max_TotalPixelMatrixColumns,
  COALESCE(max_TotalPixelMatrixRows, max_Rows) AS max_TotalPixelMatrixRows,
  SAFE_CAST(ObjectiveLensPower as INT) as ObjectiveLensPower,
  CONCAT(SPLIT(primaryAnatomicStructure_code_str,":")[SAFE_OFFSET(0)],":",SPLIT(primaryAnatomicStructure_code_str,":")[SAFE_OFFSET(1)]) as primaryAnatomicStructure_code_designator_value_str,
  SPLIT(primaryAnatomicStructure_code_str,":")[SAFE_OFFSET(2)] as primaryAnatomicStructure_CodeMeaning,
  CONCAT(SPLIT(primaryAnatomicStructureModifier_code_str,":")[SAFE_OFFSET(0)],":",SPLIT(primaryAnatomicStructureModifier_code_str,":")[SAFE_OFFSET(1)]) as primaryAnatomicStructureModifier_code_designator_value_str,
  SPLIT(primaryAnatomicStructureModifier_code_str,":")[SAFE_OFFSET(2)] as primaryAnatomicStructureModifier_CodeMeaning,

  CONCAT(SPLIT(illuminationType_code_str,":")[SAFE_OFFSET(0)],":",SPLIT(illuminationType_code_str,":")[SAFE_OFFSET(1)]) as illuminationType_code_designator_value_str,
  SPLIT(illuminationType_code_str,":")[SAFE_OFFSET(2)] as illuminationType_CodeMeaning,
FROM
  temp_table
LEFT JOIN slide_embedding on temp_table.SeriesInstanceUID = slide_embedding.SeriesInstanceUID
LEFT JOIN slide_fixative on temp_table.SeriesInstanceUID = slide_fixative.SeriesInstanceUID
LEFT JOIN slide_staining on temp_table.SeriesInstanceUID = slide_staining.SeriesInstanceUID