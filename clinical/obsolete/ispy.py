import pandas as pd
filenm='/Users/george/fed/actcianable/output/clinical_files/ISPY1 (ACRIN 6657)/I-SPY 1 All Patient Clinical and Outcome Data.xlsx'
engine = 'openpyxl'
sheetNo=0
sheetnm='Outcome Data Dictionary'
if __name__=="__main__":
  df = pd.read_excel(filenm, engine=engine, sheet_name=sheetnm, keep_default_na=False, skiprows=9)
  #sheetnm = list(dfi.keys())[sheetNo]
  #df = dfi[sheetnm]
  kk=1

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
