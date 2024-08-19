WITH
  aux AS (
  SELECT am.*, arm.ID as analysis_result_id
  FROM `{project}.{dataset}.auxiliary_metadata` am
  LEFT JOIN `{project}.{dataset}.analysis_results_metadata` arm
  ON (am.source_DOI = arm.source_doi)
  ),
  pre_dicom_all AS (
  SELECT
    aux.collection_name AS collection_name,
    aux.collection_id AS collection_id,
    aux.collection_timestamp AS collection_timestamp,
    aux.collection_hash as collection_hash,
    aux.collection_init_idc_version AS collection_init_idc_version,
    aux.collection_revised_idc_version AS collection_revised_idc_version,
    data_collections.TumorLocations AS collection_tumorLocation,
    data_collections.Species AS collection_species,
    data_collections.CancerTypes AS collection_cancerType,
    aux.access AS access,
    aux.idc_case_id as idc_case_id,
    aux.patient_hash as patient_hash,
    aux.patient_init_idc_version AS patient_init_idc_version,
    aux.patient_revised_idc_version AS patient_revised_idc_version,
    aux.study_uuid as crdc_study_uuid,
    aux.study_hash as study_hash,
    aux.study_init_idc_version AS study_init_idc_version,
    aux.study_revised_idc_version AS study_revised_idc_version,
    aux.series_uuid as crdc_series_uuid,
    aux.series_gcs_url as series_gcs_url,
    aux.series_aws_url as series_aws_url,
    aux.series_hash as series_hash,
    aux.series_init_idc_version AS series_init_idc_version,
    aux.series_revised_idc_version AS series_revised_idc_version,
    aux.SOPInstanceUID AS SOPInstanceUID,
    aux.instance_uuid as crdc_instance_uuid,
    aux.gcs_url as gcs_url,
    aux.gcs_bucket as gcs_bucket,
    aux.aws_url as aws_url,
    aux.aws_bucket as aws_bucket,
    aux.instance_size as instance_size,
    aux.instance_hash as instance_hash,
    aux.instance_init_idc_version AS instance_init_idc_version,
    aux.instance_revised_idc_version AS instance_revised_idc_version,
    aux.Source_DOI as Source_DOI,
    aux.Source_URL as Source_URL,
    aux.analysis_result_id as analysis_result_id,
    aux.license_url as license_url,
    aux.license_long_name as license_long_name,
    aux.license_short_name as license_short_name,
    aux.collection_name AS tcia_api_collection_id,
    aux.collection_id AS idc_webapp_collection_id,
    data_collections.Location as tcia_tumorLocation,
    data_collections.Species as tcia_species,
    data_collections.CancerType as tcia_cancerType
   FROM
    aux
  INNER JOIN
    `{project}.{dataset}.original_collections_metadata` AS data_collections
  ON
    aux.collection_id = data_collections.collection_id)

  SELECT
    pda.collection_name AS collection_name,
    pda.collection_id AS collection_id,
    pda.collection_timestamp AS collection_timestamp,
    pda.collection_hash as collection_hash,
    pda.collection_init_idc_version AS collection_init_idc_version,
    pda.collection_revised_idc_version AS collection_revised_idc_version,
    pda.collection_tumorLocation AS collection_tumorLocation,
    pda.collection_species AS collection_species,
    pda.collection_cancerType AS collection_cancerType,
    pda.access AS access,
    dcm.PatientID as PatientID,
    pda.idc_case_id as idc_case_id,
    pda.patient_hash as patient_hash,
    pda.patient_init_idc_version AS patient_init_idc_version,
    pda.patient_revised_idc_version AS patient_revised_idc_version,
    dcm.StudyInstanceUID AS StudyInstanceUID,
    pda.crdc_study_uuid as crdc_study_uuid,
    pda.study_hash as study_hash,
    pda.study_init_idc_version AS study_init_idc_version,
    pda.study_revised_idc_version AS study_revised_idc_version,
    dcm.SeriesInstanceUID AS SeriesInstanceUID,
    pda.crdc_series_uuid as crdc_series_uuid,
    pda.series_gcs_url as series_gcs_url,
    pda.series_aws_url as series_aws_url,
    pda.series_hash as series_hash,
    pda.series_init_idc_version AS series_init_idc_version,
    pda.series_revised_idc_version AS series_revised_idc_version,
    dcm.SOPInstanceUID AS SOPInstanceUID,
    pda.crdc_instance_uuid as crdc_instance_uuid,
    pda.gcs_url as gcs_url,
    pda.gcs_bucket as gcs_bucket,
    pda.aws_url as aws_url,
    pda.aws_bucket as aws_bucket,
    pda.instance_size as instance_size,
    pda.instance_hash as instance_hash,
    pda.instance_init_idc_version AS instance_init_idc_version,
    pda.instance_revised_idc_version AS instance_revised_idc_version,
    pda.Source_DOI as Source_DOI,
    pda.Source_URL as Source_URL,
    pda.analysis_result_id as analysis_result_id,
    pda.license_url as license_url,
    pda.license_long_name as license_long_name,
    pda.license_short_name as license_short_name,
    pda.tcia_api_collection_id AS tcia_api_collection_id,
    pda.idc_webapp_collection_id AS idc_webapp_collection_id,
    pda.tcia_tumorLocation as tcia_tumorLocation,
    pda.tcia_species as tcia_species,
    pda.tcia_cancerType as tcia_cancerType,

    dcm.* except(SOPInstanceUID, PatientID, StudyInstanceUID, SeriesInstanceUID)
  FROM pre_dicom_all pda
  INNER JOIN
    `{project}.{dataset}.dicom_metadata` AS dcm
  ON
    pda.SOPInstanceUID = dcm.SOPInstanceUID


-- WITH
--   aux AS (
--   SELECT am.*, arm.ID as analysis_result_id
--   FROM `{project}.{dataset}.auxiliary_metadata` am
--   LEFT JOIN `{project}.{dataset}.analysis_results_metadata` arm
--   ON (am.source_DOI = arm.source_doi)
--   ),
--   pre_dicom_all AS (
--   SELECT
--     aux.collection_name AS collection_name,
--     aux.collection_id AS collection_id,
--     aux.collection_timestamp AS collection_timestamp,
--     aux.collection_hash as collection_hash,
--     aux.collection_init_idc_version AS collection_init_idc_version,
--     aux.collection_revised_idc_version AS collection_revised_idc_version,
--     data_collections.Location AS collection_tumorLocation,
--     data_collections.Species AS collection_species,
--     data_collections.CancerType AS collection_cancerType,
--     aux.access AS access,
--     aux.submitter_case_id as PatientID,
--     aux.idc_case_id as idc_case_id,
--     aux.patient_hash as patient_hash,
--     aux.patient_init_idc_version AS patient_init_idc_version,
--     aux.patient_revised_idc_version AS patient_revised_idc_version,
--     aux.StudyInstanceUID AS StudyInstanceUID,
--     aux.study_uuid as crdc_study_uuid,
--     aux.study_hash as study_hash,
--     aux.study_init_idc_version AS study_init_idc_version,
--     aux.study_revised_idc_version AS study_revised_idc_version,
--     aux.SeriesInstanceUID AS SeriesInstanceUID,
--     aux.series_uuid as crdc_series_uuid,
--     aux.series_gcs_url as series_gcs_url,
--     aux.series_aws_url as series_aws_url,
--     aux.series_hash as series_hash,
--     aux.series_init_idc_version AS series_init_idc_version,
--     aux.series_revised_idc_version AS series_revised_idc_version,
--     aux.SOPInstanceUID AS SOPInstanceUID,
--     aux.instance_uuid as crdc_instance_uuid,
--     aux.gcs_url as gcs_url,
--     aux.gcs_bucket as gcs_bucket,
--     aux.aws_url as aws_url,
--     aux.aws_bucket as aws_bucket,
--     aux.instance_size as instance_size,
--     aux.instance_hash as instance_hash,
--     aux.instance_init_idc_version AS instance_init_idc_version,
--     aux.instance_revised_idc_version AS instance_revised_idc_version,
--     aux.source_doi as Source_DOI,
--     aux.source_url as Source_URL,
--     aux.analysis_result_id as analysis_result_id,
--     aux.license_url as license_url,
--     aux.license_long_name as license_long_name,
--     aux.license_short_name as license_short_name,
--     aux.tcia_api_collection_id AS tcia_api_collection_id,
--     aux.idc_webapp_collection_id AS idc_webapp_collection_id,
--     data_collections.Location as tcia_tumorLocation,
--     data_collections.Species as tcia_species,
--     data_collections.CancerType as tcia_cancerType
--    FROM
--     aux
--   INNER JOIN
--     `{project}.{dataset}.original_collections_metadata` AS data_collections
--   ON
--     aux.collection_id = data_collections.collection_id)
--   SELECT
--     pre_dicom_all.*,
--     dcm.* except(SOPInstanceUID, PatientID, StudyInstanceUID, SeriesInstanceUID)
--   FROM pre_dicom_all
--   INNER JOIN
--     `{project}.{dataset}.dicom_metadata` AS dcm
--   ON
--     pre_dicom_all.SOPInstanceUID = dcm.SOPInstanceUID
