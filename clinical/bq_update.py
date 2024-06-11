from google.cloud import bigquery
import json
from os import listdir
from os.path import isfile,join,splitext
import sys

from bq_export import DEFAULT_SUFFIX, DEFAULT_DESCRIPTION, DEFAULT_PROJECT, DICOM_META, CURRENT_VERSION, LAST_VERSION, FINAL_PROJECT, DATASET, LAST_DATASET

from bq_export import load_meta_summary, load_meta, load_clin_files

SRCFILES=["rms_mutation_prediction_demographics.json", "rms_mutation_prediction_diagnosis.json", "rms_mutation_prediction_sample.json"]
UPDATENUM="1"
COLLECS=['rms_mutation_prediction']

def delCollecs(collecs,project, dataset):
    ncollecs = ["'"+x+"'" for x in collecs]
    collecStr = ", ".join(ncollecs)

    client = bigquery.Client(project=project)
    dataset_id=project+"."+dataset
    table_id= dataset_id+".table_metadata"
    table = bigquery.Table(table_id)
    del_sql= f"""delete from """+ table_id +""" where collection_id in ("""+collecStr+""")"""
    job = client.query(del_sql)
    print(del_sql)

    table_id2=dataset_id+".column_metadata"
    del_sql2= f"""delete from """+table_id2+""" where collection_id in ("""+collecStr+""")"""
    client.query(del_sql2)


if __name__=="__main__":
  project = DEFAULT_PROJECT
  dataset = DATASET
  collecs=COLLECS
  delCollecs(collecs,project,dataset)

  filenm="./" + CURRENT_VERSION + "_" + UPDATENUM + "_table_metadata.json"
  load_meta_summary(project, dataset, [], filenm)

  filenm = "./" + CURRENT_VERSION + "_" + UPDATENUM + "_column_metadata.json"
  load_meta(project, dataset, filenm, [])

  dirnm = "./clin_" + CURRENT_VERSION
  load_clin_files(project, dataset, dirnm, SRCFILES)

