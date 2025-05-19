from google.cloud import bigquery
import json
from clinical.utils import read_clin_file

client = bigquery.Client()
query = "select tcia_api_collection_id, tcia_wiki_collection_id, idc_webapp_collection_id from `idc-dev-etl.idc_v17_pub.original_collections_metadata` order by `tcia_wiki_collection_id`"
job = client.query(query)

mp={}
clinJson ={}
for row in job.result():
  wiki = row['tcia_wiki_collection_id']
  id = row['idc_webapp_collection_id']
  mp[wiki] =id

clinJsonO = read_clin_file('./clinical_notes_old.json')
for collec in clinJsonO:
  clin = clinJsonO[collec]
  if collec in mp:
    id = mp[collec]
    clinJson[id] = clin
  else:
    clinJson[collec] = clin

f = open('./clinical_notes.json','w')
json.dump(clinJson,f)
f.close()