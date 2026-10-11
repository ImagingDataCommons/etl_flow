import pandas as pd
from pandas.io import json
import numpy as np
from os import path

import logging

logger = logging.getLogger("idc-clinical-cleaner")
logger.setLevel(logging.CRITICAL)
console_handler = logging.StreamHandler()
logging.getLogger("idc-clinical-cleaner").addHandler(console_handler)

class DictionaryReader:

  def __init__(self, file_name, form_name = None):
    self._dict_file_name = file_name



    self._dict_df = pd.read_excel(file_name, sheet_name=None)
    if form_name is not None:

      extension = path.splitext(form_name)[1]
      engine = 'xlrd'
      if extension == '.xlsx':
        engine = 'openpyxl'
      elif extension == '.xlsb':
        engine = 'pyxlsb'

      form_df_dic = pd.read_excel(form_name,sheet_name=None,engine=engine)
      form_df=form_df_dic[list(form_df_dic.keys())[0]]
      self._dict_df['Form Index']=form_df

    self._cleanup_dict_df()

    self._dict = {}

    if not "Form Index" in self._dict_df.keys():
      raise ValueError

  def _cleanup_dict_df(self):
    # replace all blank cells with NaN

    for form_id in self.get_dictionary_names():
      # remove all blank rows
      frame = self._dict_df[form_id]
      with pd.option_context("future.no_silent_downcasting", True):
        frame.replace(r'^\s*$', np.nan, regex=True, inplace=True)
      #frame.replace('', np.nan)
      self._dict_df[form_id] = frame.dropna(how='all', inplace=False)

      self._variable_names = ["Form element number","Variable Name","Variable Label","Data Type"]
      self._value_names = ["Option Code","Option Description"]

  @staticmethod
  def _normalized_col_name(name):
    return name.lower().replace(" ","_")

  def _parse_entry_from_row(self, row):
    json_entry = {}
    for name in self._variable_names:
      json_entry[self._normalized_col_name(name)] = str(row[name])
    json_entry["values"] = []
    value_entry = {}
    for name in self._value_names:
      value_entry[self._normalized_col_name(name)] = str(row[name])
    json_entry["values"].append(value_entry)
    return json_entry

  def get_meta_dictionary(self):
    metadict = []
    for dict_name in self.get_dictionary_names():
      entry = {"dict_name": dict_name, "dict_description": self.get_dictionary_desc(dict_name)}
      metadict.append(entry)
    return metadict

  def get_dictionary_desc(self, name):
    index_df = self._dict_df["Form Index"]
    try:
      cols = list(index_df.columns)
      tbl_nm_col = cols[0]
      return index_df.loc[index_df[tbl_nm_col] == name].iloc[0].iloc[1]
    except KeyError:
      return None
    except ValueError:
      return None

  def get_dictionary_names(self):
    ret = list(self._dict_df.keys())
    ret.remove('Form Index')
    return ret

  def get_dictionary(self, name):
    return self._dict[name]

  def get_dataframe(self, name):
    return self._dict_df[name]

  def parse_dictionary(self, form_id):
    form_json = []
    form_df = self._dict_df[form_id]
    json_entry = {}
    for index, row in form_df.iterrows():
      #print(f"row {index}")
      # this is empty line, skip it if in the beginning, or add current entry if
      #. this is a separator
      logger.debug("----")
      logger.debug(row)
      
      if json_entry.keys():
        # this is new entry, and we have a non-empty one - add it
        if not pd.isnull(row["Variable Name"]):
          form_json.append(json_entry)
          logger.debug("Added entry: ")
          logger.debug(json_entry)
          json_entry = self._parse_entry_from_row(row)
        else:
          # this must be a List item - confirm first
          if json_entry[self._normalized_col_name("Data Type")] != "List":
            logger.critical(f"Malformed input - expected List or non-empty Variable name in row {index}")
            logger.debug(str(row))
            logger.debug(json_entry)
            raise ValueError

          value_entry = {}
          for name in self._value_names:
            value_entry[self._normalized_col_name(name)] = str(row[name])
          json_entry["values"].append(value_entry)

      # current entry is blank, parse it from the row
      else:
        json_entry = self._parse_entry_from_row(row)

        if(row["Data Type"]) != "List":
          form_json.append(json_entry)
          logger.debug("Added entry: ")
          logger.debug(json_entry)            
          json_entry = {}

    if json_entry.keys():
      form_json.append(json_entry)

    self._dict[form_id] = form_json
    return form_json

  def parse_dictionaries(self):
    for form_id in self.get_dictionary_names():

      self._dict[form_id] = self.parse_dictionary(form_id)
