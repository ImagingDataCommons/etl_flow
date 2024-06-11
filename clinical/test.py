from google.cloud import bigquery

if __name__=="__main__":
  client=bigquery.Client()
  query="select count(*) from `canceridc-data.idc_current.original_collections_metadata` limit 3"
  job=client.query(query)
  for row in job.result():
    print(row)
