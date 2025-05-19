from google.cloud import bigquery
from google.oauth2 import service_account

DEFAULT_PROJECT='idc-dev-etl'

if __name__=="__main__":

  #credentials=service_account.Credentials.from_service_account_file('../../secure_files/webapp-dev-runtime.key.json')
  dataset_id='idc-dev-etl.idc_v15_pub'
  table_id='dicom_all_view'
  client = bigquery.Client(project=DEFAULT_PROJECT)
  ll=client.list_tables(dataset_id)
  f=1
  query = "select distinct PatientID FROM `idc-dev-etl.idc_v15_pub.dicom_all_view` where collection_id like 'htan_%' "
  job = client.query(query)
  ids={}
  for row in job.result():
    cid=row['PatientID']
    ids[cid]=1

  idl= list(ids.keys())
  idl = ["'"+id+"'" for id in idl]
  idst= ", ".join(idl)
  htan_dataset='isb-cgc-bq.HTAN'
  tbls=client.list_tables(htan_dataset)
  for tbl in tbls:
    jj=1
    tname=tbl.table_id
    if 'clinical_tier1' in tname:
      query = "select count(distinct HTAN_Participant_ID) as num, HTAN_Center from isb-cgc-bq.HTAN."+tname+" where HTAN_Participant_ID in ("+idst+") group by HTAN_Center"
      job = client.query(query)
      for row in job.result():
        print(tname+" "+str(row['HTAN_Center'])+" "+str(row['num']))

      ll=1


