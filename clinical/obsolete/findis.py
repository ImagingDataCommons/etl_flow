from google.cloud import bigquery
import random
import json
import re
import pandas as pd
import numpy as np
import re

def discover_ids(tble,collection_id,filenm):
  client = bigquery.Client()
  query = "select PatientID, collection_id from `idc-dev-etl.idc_current.dicom_all` where collection_id='"+collection_id+"'"
  job = client.query(query)

  origIds=[]
  for row in job.result():
    origIds.append(row['PatientID'])
    nm=random.sample(len(origIds)-5,4)

    cur_align_set= origIds[nm[0]]
    for i in range(1,len(nm)):
      new_align_set=""
      nword= origIds[nm[i]]
      for j in range(nword):
        if nword[j] == cur_align_set[j]:
          new_align_set = new_align_set+nword[j]




if __name__=="main":
  def_discover_ids(None,"Duke-Breasr-Cancer-MRI", None)
