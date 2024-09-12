from google.cloud import bigquery
import json
import traceback
import re
import copy
#from parse_clinical_files import formatForBQ
DEFAULT_PROJECT='idc-dev-etl'

def formatForBQ(attrs, lc=False):
  patt=re.compile(r"[a-zA-Z_0-9]")
  justNum=re.compile(r"[0-9]")
  headcols=[]
  for i in range(len(attrs)):
    headSet=[attrs[i][j] for j in range(len(attrs[i])) if len(attrs[i][j])>0]
    header='_'.join(str(k) for k in headSet)
    header=header.replace('/','_')
    header=header.replace('-', '_')
    header=header.replace(' ', '_')
    normHeader = ''
    for i in range(len(header)):
      if bool(patt.search(header[i])):
        normHeader = normHeader + header[i]
    if (len(normHeader) > 0) and bool(justNum.search(normHeader[0])):
      normHeader='c_'+normHeader
    if lc:
      normHeader = normHeader.lower()


    headcols.append(normHeader)
  return headcols


def excelLableToIndex(lbl):
  num=0
  for j in range(len(lbl)):
    num=num+j*26+ord(lbl[j])-65
  return num

def parseAMBLDic(df):
  lbls={"negative", "postive", "low", "intermediate","high"}
  cols=[]
  data_dict={}
  grptype=-1
  dataCols=[]
  excelCol=''
  ordinal=["first","second","third"]
  pos=["pos1","pos2","pos3"]
  benign=["benign1", "benign2","benign3"]

  for index, row in df.iterrows():
    if ('column' in str(row[0])) and not ('columns' in str(row[0])):
      excelCol=row[0].strip('column').strip()
      dataCol = row[1]
      colBq = formatForBQ([[dataCol]], True)[0]
      data_dict[colBq]={}

      if ((excelCol>='H') and colBq.endswith('1')):
        dataCols = [colBq[:-1]+ str(x) for x in range(1,4)]
      else:
        dataCols= [colBq]
      for col in dataCols:
        data_dict[col]= {}
        if ((excelCol>='H') and colBq.endswith('1')):
          data_dict[col]['opts']=[{"option_code":"-1", "option_description":"indicates that data is missing or not applicable"}]

    elif (len(str(row[0]))==0) and (len(str(row[1]))>0) and not ('see definition above' in row[1]) and (len(dataCols)>0):
      curind=0
      for col in dataCols:
        lbl=row[1]
        lbl=lbl.replace(ordinal[0], ordinal[curind])
        lbl = lbl.replace(pos[0], pos[curind])
        lbl = lbl.replace(benign[0], benign[curind])
        if not ('label' in data_dict[col]):
          data_dict[col]['label']=lbl
        else:
          data_dict[col]['label'] = data_dict[col]['label']+lbl
        curind=curind+1
    elif (str(row[0]) in lbls) or (isinstance(row[0], int)):
      optcode= str(row[0])
      desc= row[1]
      opt = {"option_code": optcode, "option_description": desc}
      for col in dataCols:
        if not ('opts' in data_dict[col]):
          data_dict[col]['opts']=[]
        data_dict[col]['opts'].append(opt)

  if ('reason_for_referral_id' in data_dict) and ('additional_reason_for_referral_id' in data_dict):
     if ('label' in data_dict['reason_for_referral_id']):
       data_dict['additional_reason_for_referral_id']['label'] = data_dict['reason_for_referral_id']['label']
     if ('opts' in data_dict['reason_for_referral_id']):
       data_dict['additional_reason_for_referral_id']['opts'] = copy.deepcopy(data_dict['reason_for_referral_id']['opts'])


  return data_dict




def parseIspyDic(df):
  data_dict={}
  prefix=''
  hasPrefix = False
  colA=[]
  colD=''
  optA=[]
  for index, row in df.iterrows():
    if ((len(row['Variable Name']) >0) and (len(row['Variable Description']) >0) and not hasPrefix) or ((len(row['Variable Name']) ==0) and (len(row['Variable Description']) ==0)):
      for col in colA:
        data_dict[col]={}
        data_dict[col]['label']=colD
        data_dict[col]['opts']=optA
      hasPrefix = False
      prefix = ''
      optA=[]
      colD=''
      colA=[]

    if (len(row['Variable Name']) >0) and (len(row['Variable Description']) >0):
      if (':' in row['Variable Name']) and not hasPrefix:
        hasPrefix= True
        prefix = row['Variable Name'].replace(':','')
        colD = row['Variable Description']
      elif hasPrefix:
        colA.append(prefix+" "+row['Variable Name'])
        optPrA=row['Variable Description'].split("=")
        optA.append( {"option_code": optPrA[0], "option_description": optPrA[1]}  )
      else:
        colD=row['Variable Description']
        colA=[row['Variable Name']]

    elif (len(row['Variable Description']) >0):
      optPrA = row['Variable Description'].split("=")
      optA.append({"option_code": optPrA[0], "option_description": optPrA[1]})

  return data_dict


def read_clin_file(filenm):
  f =open(filenm,'r')
  clinJson=json.load(f)
  f.close()
  return clinJson


def getSumDic(CURRENT_VERSION,src_info,num_bstches,propost_process_src,post_process_src_current_md5):
  sumDic['collection_id'] = collection_id
  sumDic['table_name'] = table_name
  sumDic['post_process_src'] = post_process_src

  if table_name in hist:
    for nkey in hist[table_name]:
      if (nkey not in sumDic) and not (nkey == 'source_info'):
        sumDic[nkey] = hist[table_name][nkey]
    if (hist[table_name]['post_process_src'] != post_process_src) or (
            post_process_src_current_md5 != hist[table_name]['post_process_src_updated_md5']):
      sumDic['idc_version_table_prior'] = sumDic['idc_version_table_updated']
      sumDic['idc_version_table_prior_md5'] = sumDic['idc_version_table_updated_md5']
      sumDic['idc_version_table_updated'] = CURRENT_VERSION
      sumDic['idc_version_table_updated_md5'] = post_process_src_current_md5
      for i in range(len(src_info)):
        if (i < len(hist[table_name]['source_info'])) and (
                src_info[i]['srcs'][0] == hist[table_name]['source_info']['srcs'][0]):
          src_info[i]['added_md5'] = hist[table_name]['source_info'][i]['added_md5']
          if src_info[i]['update_md5'] == hist[table_name]['source_info'][i]['update_md5']:
            src_info[i]['prior_md5'] = hist[table_name]['source_info'][i]['prior_md5']
          else:
            src_info[i]['prior_md5'] = hist[table_name]['source_info'][i]['update_md5']
        else:
          src_info[i]['added_md5'] = src_info[i]['update_md5']
          src_info[i]['prior_md5'] = src_info[i]['prior_md5']
  else:
    sumDic['idc_version_table_added'] = CURRENT_VERSION
    sumDic['table_added_datetime'] = str(datetime.now(pytz.utc))
    # sumDic['post_process_src']=post_process_src
    sumDic['post_process_src_added_md5'] = post_process_src_current_md5
    sumDic['idc_version_table_prior'] = CURRENT_VERSION
    sumDic['post_process_src_prior_md5'] = post_process_src_current_md5
    sumDic['idc_version_table_updated'] = CURRENT_VERSION
    sumDic['table_updated_datetime'] = str(datetime.now(pytz.utc))
    sumDic['post_process_src_updated_md5'] = post_process_src_current_md5
    sumDic['number_batches'] = num_batches
    for i in range(len(src_info)):
      src_info[i]['added_md5'] = src_info[i]['update_md5']
      src_info[i]['prior_md5'] = src_info[i]['update_md5']

  sumDic['source_info'] = src_info
  sumDic['project'] = project
  sumDic['dataset'] = dataset

def getHist(hist,table_id):
  client = bigquery.Client(project=DEFAULT_PROJECT)
  query = "select * from `" + table_id + "`"
  try:
    job = client.query(query)
    for row in job.result():
      nmInd = row['table_name'].split('.')
      tbl = nmInd[len(nmInd)-1]
      cdic={}
      cdic['idc_version_table_added'] = row['idc_version_table_added']
      cdic['table_added_datetime'] = row['table_added_datetime']
      cdic['post_process_src'] = row['post_process_src']
      cdic['post_process_src_added_md5'] = row['post_process_src_added_md5']
      cdic['idc_version_table_prior'] = row['idc_version_table_prior']
      cdic['post_process_src_prior_md5'] = row['post_process_src_prior_md5']
      cdic['idc_version_table_updated'] = row['idc_version_table_updated']
      cdic['table_updated_datetime'] = row['table_updated_datetime']
      cdic['post_process_src_updated_md5'] = row['post_process_src_updated_md5']
      cdic['number_batches'] = row['number_batches']
      cdic['source_info']=row['source_info']
      hist[tbl]=cdic
  except:
    traceback.print_exc()
    k=1

