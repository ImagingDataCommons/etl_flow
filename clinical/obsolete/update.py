from google.cloud import bigquery

table_id='idc-dev-etl.idc_v11_clinical.table_metadata'


if __name__=="__main__":
  client = bigquery.Client()
  query ="update "+table_id+" set idc_version_table_added='idc_v11', idc_version_table_prior='idc_v11', idc_version_table_updated='idc_v11' where True"
  query_job=client.query(query)
  query_job.result()

