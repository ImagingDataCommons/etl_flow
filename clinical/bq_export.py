from google.cloud import bigquery
import json
from os import listdir
from os.path import isfile,join,splitext
import sys
from clinical.addcptac import addTables, CPTAC_SRC,TCGA_SRC,HTAN_SRCS,HTAN_TABLES
from python_settings import settings

from utilities.logging_config import successlogger, progresslogger, errlogger, warninglogger


DEFAULT_SUFFIX='clinical'
DEFAULT_DESCRIPTION='clinical data'
DEFAULT_PROJECT ='idc-dev-etl'
#DEFAULT_PROJECT ='idc-dev'
DICOM_META='idc-dev-etl.idc_v'+str(settings.CURRENT_VERSION)+'_pub.dicom_all'


CURRENT_VERSION = 'idc_v'+str(settings.CURRENT_VERSION)
LAST_VERSION = 'idc_v'+str(settings.PREVIOUS_VERSION)
FINAL_PROJECT='bigquery-public-data'

DATASET=CURRENT_VERSION+'_clinical'
LAST_DATASET=LAST_VERSION+'_clinical'


META_SUM_SCHEMA= [
          bigquery.SchemaField("collection_id","STRING"),
          bigquery.SchemaField("table_name","STRING"),
          bigquery.SchemaField("table_description", "STRING"),
          bigquery.SchemaField("idc_version_table_added", "STRING"),
          bigquery.SchemaField("table_added_datetime", "STRING"),
          bigquery.SchemaField("post_process_src","STRING"),
          bigquery.SchemaField("post_process_src_added_md5","STRING"),
          
          bigquery.SchemaField("idc_version_table_prior", "STRING"),
          bigquery.SchemaField("post_process_src_prior_md5", "STRING"),
          bigquery.SchemaField("idc_version_table_updated","STRING"),
          bigquery.SchemaField("table_updated_datetime","STRING"),
          bigquery.SchemaField("post_process_src_updated_md5","STRING"),
          
          bigquery.SchemaField("number_batches","INTEGER"),
          bigquery.SchemaField("source_info","RECORD",mode="REPEATED",
              fields=[bigquery.SchemaField("srcs","STRING",mode="REPEATED"),
              bigquery.SchemaField("md5","STRING"),
              bigquery.SchemaField("table_last_modified", "STRING"),
              bigquery.SchemaField("table_size", "INTEGER"),
            ]  
          ),

           ] 

def create_meta_summary(project, dataset):
  client = bigquery.Client(project=project)
  dataset_id= project+"."+dataset
  table_id = dataset_id+".table_metadata"
  #filenm=CURRENT_VERSION+"_table_metadata.json"
  schema = [
          bigquery.SchemaField("collection_id","STRING"),
          bigquery.SchemaField("table_name","STRING"),
          bigquery.SchemaField("table_description", "STRING"),
          bigquery.SchemaField("idc_version_table_added", "STRING"),
          bigquery.SchemaField("table_added_datetime", "STRING"),
          bigquery.SchemaField("post_process_src","STRING"),
          bigquery.SchemaField("post_process_src_added_md5","STRING"),
          
          bigquery.SchemaField("idc_version_table_prior", "STRING"),
          bigquery.SchemaField("post_process_src_prior_md5", "STRING"),
          bigquery.SchemaField("idc_version_table_updated","STRING"),
          bigquery.SchemaField("table_updated_datetime","STRING"),
          bigquery.SchemaField("post_process_src_updated_md5","STRING"),
          
          bigquery.SchemaField("number_batches","INTEGER"),
          bigquery.SchemaField("source_info","RECORD",mode="REPEATED",
              fields=[bigquery.SchemaField("srcs","STRING",mode="REPEATED"),
              bigquery.SchemaField("md5","STRING"),
              bigquery.SchemaField("table_last_modified", "STRING"),
              bigquery.SchemaField("table_size", "INTEGER"),
            ]  
          ),

           ] 


  client.delete_table(table_id,not_found_ok=True)
  table = bigquery.Table(table_id, schema=schema)
  client.create_table(table)

def load_meta_summary(project, dataset, cptacColRows,filenm):
  client = bigquery.Client(project=project)
  dataset_id = project + "." + dataset
  table_id = dataset_id + ".table_metadata"
  table = bigquery.Table(table_id)
  job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, write_disposition=bigquery.WriteDisposition.WRITE_APPEND, schema=META_SUM_SCHEMA)

  try:
      f = open(filenm, "r")
  except Exception as exc:
      pass
  metaD = json.load(f)
  f.close()
  metaD.extend(cptacColRows)
  job = client.load_table_from_json(metaD, table, job_config=job_config)
  progresslogger.info(job.result())


def create_meta_table(project, dataset):

  client = bigquery.Client(project=project)
  dataset_id= project+"."+dataset
  table_id = dataset_id+".column_metadata"

  schema = [
            bigquery.SchemaField("collection_id","STRING"),
            bigquery.SchemaField("case_col","BOOLEAN"),
            bigquery.SchemaField("table_name","STRING"),
            bigquery.SchemaField("column","STRING"),
            bigquery.SchemaField("column_label","STRING"),
            bigquery.SchemaField("data_type","STRING"),
            bigquery.SchemaField("original_column_headers","STRING", mode="REPEATED",
                ),
            bigquery.SchemaField("values", "RECORD", mode="REPEATED",
                fields=[
                  bigquery.SchemaField("option_code","STRING"),
                  bigquery.SchemaField("option_description","STRING"),
             ],
            ),
           bigquery.SchemaField("values_source","STRING"),
           bigquery.SchemaField("files", "STRING", mode="REPEATED"),
           bigquery.SchemaField("sheet_names","STRING",mode="REPEATED"),
           bigquery.SchemaField("batch", "INTEGER",mode="REPEATED"),
           bigquery.SchemaField("column_numbers", "INTEGER", mode="REPEATED")
           ] 
  
  dataset=bigquery.Dataset(dataset_id)
  dataset.location='US'
  client.delete_table(table_id,not_found_ok=True)
  table = bigquery.Table(table_id, schema=schema)
  client.create_table(table)

def load_meta(project, dataset, filenm,cptacRows):
  client = bigquery.Client(project=project)
  dataset_id = project+"."+dataset
  table_id = dataset_id+".column_metadata"
  table = bigquery.Table(table_id)

  job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, write_disposition=bigquery.WriteDisposition.WRITE_APPEND)

  with open(filenm,"rb") as source_file:
    f=open(filenm,'r')
    metaD=json.load(f)
    f.close()
    metaD.extend(cptacRows)
    job=client.load_table_from_json(metaD, table, job_config=job_config)
    progresslogger.info(job.result())

def checkData():  
  dataset_id=DEFAULT_PROJECT+'.'+DATASET
  client = bigquery.Client(project=DEFAULT_PROJECT)
  query = "select distinct collection_id, PatientID from "+DICOM_META+" order by idc_webapp_collection_id"
  job = client.query(query)
  ids={}
  for row in job.result():
    colec=row['idc_webapp_collection_id']
    cid=row['PatientID']
    if not (colec in ids):
        progresslogger.info("Collecs "+colec)
        ids[colec]={}
    ids[colec][cid]=1


  tables= client.list_tables(dataset_id)
  tableNms=[tb.table_id for tb in tables]
  if ("table_metadata" in tableNms):
    tableNms.remove("table_metadata")
  else:
    errlogger.error("table_metadata is missing!")
  if ("column_metadata" in tableNms):
    tableNms.remove("column_metadata")
  else:
    errlogger.error("column_metadata is missing!")

  tableNms.sort()

  query = "select distinct table_name from "+dataset_id+".table_metadata "
  job = client.query(query)
  tableL = [row.table_name for row in job.result()]
  tableL = [x.split('.')[len(x.split('.'))-1] for x in tableL]
  tableL.sort()
  if not (tableNms == tableL):
    errlogger.error("table_metadata list is incorrect")

  query = "select distinct table_name from " + dataset_id + ".column_metadata "
  job = client.query(query)
  tableL = [row.table_name for row in job.result()]
  tableL = [x.split('.')[len(x.split('.'))-1] for x in tableL]
  tableL.sort()
  if not (tableNms == tableL):
    errlogger.error("column_metadata table list is incorrect")

  for tableNm in tableNms:
    table_id=dataset_id+'.'+tableNm
    colec=tableNm.rsplit('_',1)[0]
    progresslogger.info("colec "+colec)
    table=client.get_table(table_id)
    colNames=[col.name for col in table.schema]
    colNames.sort()
    final_id=FINAL_PROJECT+"."+CURRENT_VERSION+"_clinical."+tableNm 
    query = "select table_name,column from " + dataset_id + ".column_metadata where table_name= '"+final_id+"'"
    job = client.query(query)
    progresslogger.info(query)
    colL = [row.column for row in job.result()]
    colL.sort()
    if not (colNames == colL):
      errlogger.error ("mismatch in columns for table "+tableNm+"!")
    i=1
    numExt=0
    curDic=ids[colec]
    query = "select distinct dicom_patient_id from " + table_id
    job = client.query(query)
    for row in job.result():
      cid=row['dicom_patient_id']
      if not (cid in curDic):
        numExt=numExt+1
    if (numExt>0):
      errlogger.error("for table "+tableNm+ " "+str(numExt)+" ids not in dicom ")


def load_clin_files(project, dataset, cpath, srcfiles):
  error_sets=[]  
  client = bigquery.Client(project=project)
  ofiles=[]
  if srcfiles is None:
    ofiles = [f for f in listdir(cpath) if isfile(join(cpath,f))]
  else:
    ofiles=srcfiles
  dataset_created={}
  for ofile in ofiles:
    cfile= join(cpath,ofile)
    collec = splitext(ofile)[0]
    file_ext = splitext(ofile)[1]
    progresslogger.info(collec+" "+file_ext)
    if file_ext=='.csv':
        table_id=project+"."+dataset+"."+collec
        job_config=bigquery.LoadJobConfig(autodetect=True,source_format=bigquery.SourceFormat.CSV)
        progresslogger.info(cfile)
        with open(cfile,'rb') as nfile:
          job=client.load_table_from_file(nfile,table_id, job_config=job_config)
          progresslogger.info(job.result())
        nfile.close()   
    if (file_ext=='.json') and not collec.startswith(CURRENT_VERSION):
      table_id =project+"."+dataset+"."+collec
      job_config= bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)
      schema=[]
      f=open(cfile,'r')
      metaD=json.load(f)
      f.close()
      schemaD=metaD['schema']
      cdata=metaD['data']
      for nset in schemaD:
        col=nset[0]
        dtype=nset[1]
        colType="STRING"
        if dtype == "int":
          colType="INTEGER"
        elif dtype == "float":
          colType="FLOAT"
        schema.append(bigquery.SchemaField(col,colType))
      client.delete_table(table_id,not_found_ok=True)
      table=bigquery.Table(table_id)
      job_config =bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, schema=schema, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
      try:
        job= client.load_table_from_json(cdata, table, job_config=job_config)    
        progresslogger.info(job.result())
      except:
        error_sets.append(collec)        
  errlogger.error('error sets')
  errlogger.error(str(error_sets))


def load_all(project,dataset,version,last_dataset, last_version):
  client = bigquery.Client(project=project)
  dataset_id=project+"."+dataset
  ds = bigquery.Dataset(dataset_id)
  ds.location = 'US'
  client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
  ds = client.create_dataset(dataset_id)

  bqSrcMetaTbl = []
  bqSrcMetaCol = []

  # We get HTAN and CPTAC tables from ISB-CGC and TCGA IDC BQ
  htan = addTables(project,dataset,version, "HTAN", None, HTAN_TABLES, HTAN_SRCS, "HTAN_Participant_ID",False, last_dataset, last_version)
  cptac = addTables(project, dataset, version, "CPTAC", None, ["clinical"], [CPTAC_SRC], "submitter_id", False, last_dataset, last_version)
  tcga = addTables(project, dataset, version, "TCGA", None, ["clinical"], [TCGA_SRC], "case_barcode", False, last_dataset, last_version)

  bqSrcMetaTbl = htan[0]+cptac[0]+tcga[0]
  bqSrcMetaCol = htan[1]+cptac[1]+tcga[1]

  create_meta_summary(project, dataset)
  create_meta_table(project, dataset)
  filenm = "./json/clin_" + CURRENT_VERSION + "/" + CURRENT_VERSION + "_table_metadata.json"
  load_meta_summary(project, dataset, bqSrcMetaTbl,filenm)

  # filenm = "./clinical/json/clin_" + CURRENT_VERSION + "/" + CURRENT_VERSION + "_column_metadata.json"
  filenm = "./json/clin_" + CURRENT_VERSION + "/" + CURRENT_VERSION + "_column_metadata.json"
  load_meta(project,dataset,filenm,bqSrcMetaCol)

  # dirnm="./clinical/json/clin_"+CURRENT_VERSION
  dirnm="./json/clin_"+CURRENT_VERSION
  load_clin_files(project,dataset,dirnm,None)


if __name__=="__main__":
  load_all(DEFAULT_PROJECT, DATASET,CURRENT_VERSION, LAST_DATASET, LAST_VERSION)
  #checkData()

