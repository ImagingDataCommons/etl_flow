SELECT DISTINCT aj.idc_collection_id, aj.se_uuid, aj.i_uuid as uuid,
if(aj.i_source='tcia', ac.dev_tcia_url, ac.dev_idc_url) as dev_gcs_bucket,
if(aj.i_source='tcia', ac.pub_aws_tcia_url, ac.pub_aws_idc_url) as dev_aws_bucket,
if(aj.i_source='tcia', ac.pub_gcs_tcia_url, ac.pub_gcs_idc_url) as pub_gcs_bucket,
if(aj.i_source='tcia', ac.pub_aws_tcia_url, ac.pub_aws_idc_url) as pub_aws_bucket,
CONCAT(
  'gs://',
  if(aj.i_source='tcia', ac.dev_tcia_url, ac.dev_idc_url),
  '/', aj.se_uuid, '/', aj.i_uuid, '.dcm')
as dev_gcs_url,
CONCAT(
  's3://',
  if(aj.i_source='tcia', ac.pub_aws_tcia_url, ac.pub_aws_idc_url),
  '/', aj.se_uuid, '/', aj.i_uuid, '.dcm')
as dev_aws_url,
CONCAT(
  'gs://',
  if(aj.i_source='tcia', ac.pub_gcs_tcia_url, ac.pub_gcs_idc_url),
  '/', aj.se_uuid, '/', aj.i_uuid, '.dcm')
 as pub_gcs_url,
CONCAT(
  's3://',
  if(aj.i_source='tcia', ac.pub_aws_tcia_url, ac.pub_aws_idc_url),
  '/', aj.se_uuid, '/', aj.i_uuid, '.dcm')
as pub_aws_url,
if(aj.i_source='tcia', ac.tcia_access , ac.idc_access) as access, i_source source
FROM `idc-dev-etl.idc_v14_dev.all_joined` aj
JOIN `idc-dev-etl.idc_v14_dev.all_collections` ac
on aj.collection_id = ac.tcia_api_collection_id
order by collection_id
