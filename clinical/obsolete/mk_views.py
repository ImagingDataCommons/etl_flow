from google.cloud import bigquery
from python_settings import settings
import settings as etl_settings
settings.configure(etl_settings)
assert settings.configured

CURRENT_VERSION = 'idc_v'+str(settings.CURRENT_VERSION)
DEFAULT_PROJECT ='idc-dev-etl'


dataset_src_id='idc-dev-etl.'+CURRENT_VERSION+'_clinical'
dataset_dest_id='idc-pdp-staging.'+CURRENT_VERSION+'_clinical'


if __name__=="__main__":
  client = bigquery.Client(project=DEFAULT_PROJECT)
  dataset_dest=bigquery.Dataset(dataset_dest_id)
  dataset_dest.location='US'
  client.delete_dataset(dataset_dest_id,delete_contents=True,not_found_ok=True)
  client.create_dataset(dataset_dest_id)


  tables=client.list_tables(dataset_src_id)

  for table in tables:
      cur_table= table.table_id
      src_table=dataset_src_id+'.'+cur_table
      dest_view=dataset_dest_id+'.'+cur_table
      view= bigquery.Table(dest_view)
      view.view_query=f"select * from `{src_table}`"
      view = client.create_table(view)
      #job=client.copy_table(src_table,dest_table)
      #job.result()

