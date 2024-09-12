from google.cloud import bigquery
import json
import sys
from clinical.utils import read_clin_file
from python_settings import settings
from os import path, listdir,mkdir
import shutil
import requests
import hashlib

import settings as etl_settings
settings.configure(etl_settings)
assert settings.configured

if __name__=="__main__":
  download_dir='./clinical/downloads/downloads_'+str(settings.CURRENT_VERSION)
  shutil.rmtree(download_dir, ignore_errors=True)
  mkdir(download_dir)

  client = bigquery.Client(project='idc-dev-etl')
  client = bigquery.Client()

  ref_tbl= settings.DEV_PROJECT+'.'+settings.BQ_DEV_INT_DATASET+'.collection_id_map'
  query = "select distinct idc_webapp_collection_id  from `" + ref_tbl + "` order by `idc_webapp_collection_id`"
  job = client.query(query)
  ref = {}

  for row in job.result():
    ref[row['idc_webapp_collection_id']] = 1


  tbl=settings.DEV_PROJECT+'.'+settings.BQ_DEV_INT_DATASET+'.tcia_clinical_and_related_metadata'

  query = "select idc_collection_id, download_url, download_type  from `" + tbl + "` order by `idc_collection_id`, `download_url`"
  job = client.query(query)

  ndata=[]
  for row in job.result():
    id = row['idc_collection_id']
    if id in ref:
      download_url = row['download_url']
      download_type = row['download_type']
      coldir = download_dir+'/'+id
      filenmA = download_url.split('/')
      filenm = filenmA[len(filenmA)-1]
      if '?' in filenm:
        filenmA = filenm.split('?')
        filenm = filenmA[0]
      if not path.isdir(coldir):
        mkdir(coldir)
      resp = requests.get(download_url)
      md5 = hashlib.md5(resp.content).hexdigest()
      with open(coldir + '/' + filenm, 'wb') as f:
        f.write(resp.content)
      ndata.append(id+'\t'+filenm+'\t'+download_type+'\n')

  fl =open(download_dir+"/summary.txt","w+")
  fl.writelines(ndata)
  fl.close()









