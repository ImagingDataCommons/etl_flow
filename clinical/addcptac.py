from google.cloud import bigquery
import json
from datetime import datetime,date
from utils import getHist, read_clin_file
import pytz

from python_settings import settings
import settings as etl_settings
settings.configure(etl_settings)
assert settings.configured
from utilities.logging_config import successlogger,progresslogger, warninglogger, errlogger
import logging
progresslogger.setLevel(logging.INFO)

DEFAULT_PROJECT ='idc-dev-etl'
DEFAULT_SUFFIX="clinical"
DEFAULT_DESCRIPTION="clinical data"

HTAN_SRCS=['isb-cgc-bq.HTAN.clinical_tier1_demographics_current','isb-cgc-bq.HTAN.clinical_tier1_diagnosis_current',
           'isb-cgc-bq.HTAN.clinical_tier1_exposure_current','isb-cgc-bq.HTAN.clinical_tier1_familyhistory_current',
           'isb-cgc-bq.HTAN.clinical_tier1_followup_current','isb-cgc-bq.HTAN.clinical_tier1_moleculartest_current',
           'isb-cgc-bq.HTAN.clinical_tier1_therapy_current']
HTAN_TABLES=['demographics','diagnosis','exposure','familyhistory','followup','moleculartest','therapy']
CPTAC_SRC='isb-cgc-bq.CPTAC_versioned.clinical_gdc_r31'
NLST='idc-dev-etl.idc_current'
NLST_SRCA=['nlst_canc','nlst_ctab','nlst_ctabc','nlst_prsn','nlst_screen']


IDC_VERSION = 'idc_v'+str(settings.CURRENT_VERSION)
IDC_VERSION_LAST = 'idc_v'+str(settings.PREVIOUS_VERSION)

TCGA_SRC='idc-dev-etl.'+IDC_VERSION+'_pub.tcga_clinical_rel9'

IDC_COLLECTION_ID_SRC='`idc-dev-etl.'+IDC_VERSION+'_pub.original_collections_metadata`'
IDC_PATIENT_ID_SRC='`idc-dev-etl.'+IDC_VERSION+'_pub.dicom_all`'


SOURCE_BATCH_COL='source_batch'
SOURCE_BATCH_LABEL='idc_provenance_source_batch'
DICOM_COL= 'dicom_patient_id'
DICOM_LABEL='idc_provenance_dicom_patient_id'
FINAL_PROJ='bigquery-public-data.'
TCGA_REC_SRC='bigquery-public-data.'+IDC_VERSION+'.tcga_clinical_rel9'



def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))


def create_table_meta_row(collec,table_name,dataset_id,version,src_table_id, table_src_rec, dataset_id_lst,version_lst,tbltype):
  full_table_name = FINAL_PROJ + IDC_VERSION + '_clinical.' + table_name
  client = bigquery.Client(project=DEFAULT_PROJECT)
  src_table = client.get_table(src_table_id)
  table_last_modified = str(src_table.modified)
  table_size = str(src_table.num_bytes)

  hist={}
  #table_id = dataset_id + '.table_metadata'
  table_id_lst= dataset_id_lst + '.table_metadata'
  if not version==version_lst:
    getHist(hist, table_id_lst)

  sumArr=[]
  #for coll in cptac:
  sumDic = {}
  suffix = DEFAULT_SUFFIX
  table_description = tbltype
  #collection_id = str(cptac)
  #table_name = 'cptac_clinical'
  sumDic['collection_id'] = collec
  sumDic['table_name'] = full_table_name
  sumDic['table_description'] = table_description
  sumDic['source_info']=[]
  sumDic['source_info'].append({})
  sumDic['source_info'][0]['table_last_modified']=table_last_modified
  sumDic['source_info'][0]['table_size'] = table_size
  sumDic['source_info'][0]['srcs']=[table_src_rec]

  if table_name in hist:
    for nkey in hist[table_name]:
      if (nkey not in sumDic) and not (nkey == 'source_info'):
        sumDic[nkey] = hist[table_name][nkey]
      old_table_modified=sumDic['source_info'][0]['table_last_modified']
      old_table_size=sumDic['source_info'][0]['table_size']
      if not (old_table_modified == table_last_modified):
        sumDic['idc_version_table_prior']=sumDic['idc_version_table_updated']
        sumDic['idc_version_table_updated'] = version
        sumDic['table_updated_datetime'] = str(datetime.now(pytz.utc))
  else:
    sumDic['idc_version_table_added'] = version
    sumDic['table_added_datetime'] = str(datetime.now(pytz.utc))
    sumDic['idc_version_table_prior'] = version
    sumDic['idc_version_table_updated'] = version
    sumDic['table_updated_datetime'] = str(datetime.now(pytz.utc))
    sumDic['number_batches'] = 1
  sumArr.append(sumDic)
  return sumArr


def create_column_meta_rows(collec, table_name,dataset_id):
  full_table_name=FINAL_PROJ+IDC_VERSION+'_clinical.'+table_name
  src_table_id = dataset_id + '.' + table_name
  client = bigquery.Client(project=DEFAULT_PROJECT)
  src_table = client.get_table(src_table_id)
  newArr=[]
  valSet={}
  fieldSet={}
  for field in src_table.schema:
    curRec={}
    nm = field.name
    valSet[nm]=[]
    fieldSet[nm]=True
    type = field.field_type
    curRec['collection_id']=collec
    if nm == "submitter_id":
      curRec['case_col']=True
    else:
      curRec['case_col']=False
    curRec['table_name']=full_table_name

    curRec['column'] = nm
    
    if nm == SOURCE_BATCH_COL:
      curRec['column_label']=SOURCE_BATCH_LABEL
    elif nm == DICOM_COL:
      curRec['column_label']=DICOM_LABEL
    else:
      curRec['column_label'] = nm
    curRec['data_type']=type
    curRec['batch']=[0]
    newArr.append(curRec)

  query = "select * from `" + src_table_id + "`"
  ii=1
  job = client.query(query)
  for row in job.result():
    for nm in fieldSet:
      val=row[nm]
      if fieldSet[nm] and (not val in valSet[nm]):
        valSet[nm].append(val)
        if len(valSet[nm])>20:
          fieldSet[nm]=False
          valSet.pop(nm)

  for rec in newArr:
    if rec['column'] in valSet and len(valSet[rec['column']])>0:
      valSet[rec['column']] = [str(x) for x in valSet[rec['column']]]
      valSet[rec['column']].sort()
      rec['values'] = [{"option_code":x} for x in valSet[rec['column']]]
      rec['values_source']='derived from inspection of values'
  return newArr

def copy_table(dataset_id, table_name, lst, src_table_id, id_col, intIds):
  if table_name is None:
    table_name="cptac_clinical"
  #src_table_id = CPTAC_SRC
  progresslogger.info(f'Copying {table_name}')
  client = bigquery.Client(project=DEFAULT_PROJECT)
  src_table = client.get_table(src_table_id)
  nschema=[bigquery.SchemaField("dicom_patient_id","STRING"),
          bigquery.SchemaField("source_batch","INTEGER")]
  nschema.extend(src_table.schema)

  dest_table_id = dataset_id + '.'+table_name
  client.delete_table(dest_table_id, not_found_ok=True)

  if lst is None:
    query = "select " + id_col + " as dicom_patient_id, 0 as source_batch, * from `" + src_table_id + "`"
  else:
    if intIds:
      qslst = [ str(x)  for x in lst]
      inp=",".join(qslst)
    else:
      qslst=["\"" + str(x) + "\"" for x in lst]
      inp =",".join(qslst)
    query = "select " + id_col + " as dicom_patient_id, 0 as source_batch, * from `" + src_table_id + "` where " + id_col + " in (" + inp + ")"
  job_config=bigquery.QueryJobConfig(destination=dest_table_id)
  progresslogger.debug(query)
  query_job=client.query(query, job_config=job_config)
  progresslogger.debug(query_job.result())
  dest_table=client.get_table(dest_table_id)
  nrows=dest_table.num_rows
  if nrows==0:
    client.delete_table(dest_table_id, not_found_ok=True)
  return(nrows)
  kk=1

# Return a dictionary indexed by collection_ids of collections in program
# with a list of patient IDs for each collection
def get_ids(program,collection):
  client = bigquery.Client(project=DEFAULT_PROJECT)
  query = "select distinct t1.collection_id, PatientID from "+IDC_COLLECTION_ID_SRC+" t1,"+IDC_PATIENT_ID_SRC+" t2 where "
  if (program is not None):
    query = query + "Program = '"+ program +"' and "
  if (collection is not None):
    query = query + "t1.collection_id = '" + collection + "' and "
  query = query + "t1.collection_id = t2.collection_id "\
          "order by t1.collection_id, PatientID"
  progresslogger.info(query)
  job = client.query(query)
  program_Dic={}
  for row in job.result():
    idc_webapp=row['collection_id']
    patientID = row['PatientID']

    if not idc_webapp in program_Dic:
      program_Dic[idc_webapp]=[]
    program_Dic[idc_webapp].append(patientID)
  for collec in program_Dic:
    program_Dic[collec].sort()

  return program_Dic

def addTables(proj_id, dataset_id, version,program,collection,types,table_srcs, id_col,intIds,dataset_id_lst,version_lst):
  nrows=[]
  colrows=[]
  # Get HTAN patient IDs
  collec_id_mp = get_ids(program, collection)
  dataset_id = proj_id + "." + dataset_id
  dataset_id_lst = proj_id + "." + dataset_id_lst
  for collec in collec_id_mp:
    progresslogger.info(f'collec: {collec}')
    for i in range(len(table_srcs)):
      table_src=table_srcs[i]
      tbltype=types[i]
      table_name = collec + "_" + tbltype
      numr = copy_table(dataset_id, table_name, collec_id_mp[collec],table_src, id_col, intIds)
      if numr > 0:
        table_src_rec=table_src
        nrows.extend(create_table_meta_row(collec, table_name, dataset_id, version,table_src, table_src_rec,dataset_id_lst,version_lst,tbltype))
        colrows.extend(create_column_meta_rows(collec, table_name, dataset_id))
  return([nrows,colrows])

if __name__=="__main__":
  #ret=addTables("idc-dev","idc_v11_clinical","idc_v11","CPTAC",None,["clinical"],[CPTAC_SRC],"submitter_id", False)
  #get_ids('HTAN',None)
  #ret=addTables("idc-dev","idc_v11_clinical","idc_v11","TCGA",None,["clinical"],[TCGA_SRC],"case_barcode", False)
  ret = addTables("idc-dev", "idc_v16_clinical", "idc_v16", "HTAN", None, HTAN_TABLES, HTAN_SRCS, "HTAN_Participant_ID",False,'idc-dev-etl.v15_current','v15')
  '''for colec in NLST_SRCA:
    sufx=colec.split('_')[1].lower()
    src=NLST+'.'+colec
    nret = addTables("idc-dev", "idc_v11_clinical", "idc_v11", "NCI Trials", "nlst", sufx, src, "pid",True)'''
  rr=1


