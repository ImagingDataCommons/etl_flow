from google.cloud import bigquery
import json
import sys
from clinical.utils import read_clin_file

#[collec, colid, files_used, files_not_mtch, urls_not_mtch]

download_schema = [
    bigquery.SchemaField("wiki_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("idc_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("files", "STRING", mode="REPEATED"),
    bigquery.SchemaField("files_not_found", "STRING", mode="REPEATED"),
    bigquery.SchemaField("urls_not_used", "STRING", mode="REPEATED")

]


if __name__=="__main__":
  client = bigquery.Client(project='idc-dev-etl')
  dataset = client.dataset('gw_temp')
  client = bigquery.Client()
  query = "select collection_name, collection_id  from `idc-dev-etl.idc_v18_pub.original_collections_metadata` order by `collection_name`"
  job = client.query(query)

  colnmMp = {}
  colidMp = {}
  fullset ={}

  rows = []


  for row in job.result():
    collec_nm = row['collection_name']
    collec_id = row['collection_id']
    colnmMp[collec_nm] = collec_id
    colidMp[collec_id] = collec_nm

  clinJson = read_clin_file('./clinical_notes.json')
  cloads = {}
  for colnm in clinJson:
    colecData = clinJson[colnm]

    if (('tcia' in colecData) and (colecData['tcia']=='yes')):
      if (not('spec' in colecData) and ('srcs' in colecData)):
        colid = colecData["idc_id"]
        fullset[colid] = 1
        cloads[colid] = []
        for i in range(0,len(colecData['srcs'])):
          for j in range(0,len(colecData['srcs'][i])):
            src = colecData['srcs'][i][j]
            if 'archive' in src:
              for j in range(0,len(src['archive'])):
                azips = src['archive'][j]
                if not (azips in cloads[colid]):
                  cloads[colid].append(azips)
            elif 'filenm' in src:
              if not (src['filenm'] in cloads[colid]):
                cloads[colid].append(src['filenm'])
      elif (('spec' in colecData) and (colecData['spec'] == 'acrin') and ('uzip' in colecData)):
        colid = colecData["idc_id"]
        #colid = colnmMp[colnm]
        fullset[colid] = 1
        cloads[colid] = []
        srcs =colecData['uzip']
        for i in range(0,len(srcs)):
          src = srcs[i]
          if not (src in cloads[colid]):
            cloads[colid].append(src)

  tloads = {}

  client = bigquery.Client()
  query = "select idc_collection_id, download_url, collection_wiki_id  from `idc-dev-etl.gw_temp.tcia_clinical_and_related_metadata2` order by `idc_collection_id`"
  job = client.query(query)

  for row in job.result():
    collec_wiki_id = row['collection_wiki_id']
    idc_collection_id = row['idc_collection_id']

    url = row['download_url']
    urlA = url.split('/')
    url = urlA[len(urlA)-1]
    if (idc_collection_id in colidMp):
      #colnm=colidMp[collec_id]
      if not (idc_collection_id in tloads):
        fullset[idc_collection_id] = 1
        print (idc_collection_id+" "+collec_wiki_id)
        tloads[idc_collection_id]=[]
      tloads[idc_collection_id].append(url)

  fsetkeys=list(fullset.keys())
  fsetkeys.sort()
  rows=[]
  for i in range(len(fsetkeys)):
  #for i in range(2):
    row=[]
    files_used =[]
    urls_not_mtch=[]
    files_not_mtch=[]
    collec=fsetkeys[i]
    colid=''
    if collec in cloads:
      files = cloads[collec]
      files_used = [file for file in files]
      filesU = [fl.replace(' ','-') for fl in files]

      if (collec in tloads):
        urls =  tloads[collec]
        for url in urls:
          if not url in filesU:
            urls_not_mtch.append(url)
        for i in range(len(filesU)):
          if not filesU[i] in urls:
            files_not_mtch.append(files[i])
      else:
        files_not_mtch = [ file for file in files]
    else:
      urls_not_mtch = [url for url in tloads[collec]]

    #row = [collec, colid, files_used, files_not_mtch, urls_not_mtch]
    row = {"wiki_id":collec, "idc_id":colid, "files":files_used, "files_not_found":files_not_mtch, "urls_not_used":urls_not_mtch}
    rows.append(row)
  #jrow = json.dumps(rows)

  try:
    client = bigquery.Client(project='idc-dev-etl')
    dataset = client.dataset('gw_temp')
    # table = dataset.table('tst')
    table_id = 'idc-dev-etl.gw_temp.tst'
    client.delete_table(table_id, not_found_ok=True)
    table = bigquery.Table(table_id, schema=download_schema)
    client.create_table(table)
    #rows= [{"wiki_id":"fd", "idc_id":"rt", "files":[], "files_not_found":[], "urls_not_used":[]}]
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                                        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                                        schema=download_schema)

    job = client.load_table_from_json(rows, table, job_config=job_config)
    print(job.result())

  except Exception as exc:
    print('Table creation failed: {exc}')








