#
# Copyright 2015-2021, Institute for Systems Biology
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from google.cloud import bigquery

import settings
import argparse


def create_hierarchy():
    query = f"""
    SELECT * 
    FROM `{settings.DEV_MITIGATION_PROJECT}.mitigations.deleted_instances`
    """
    df = bigquery.Client().query(query).to_dataframe()
    collections = {}
    for c_uuid in df['c_uuid'].unique():
        collection_df = df.loc[df['c_uuid'] == c_uuid]
        collections[c_uuid] = {
            'collection_id': collection_df['collection_id'].unique()[0],
            'c_init_idc_version': collection_df['c_init_idc_version'].unique()[0],
            'c_rev_idc_version': collection_df['c_rev_idc_version'].unique()[0],
            'c_final_idc_version': collection_df['c_final_idc_version'].unique()[0],
            'patients': {}
        }
        patients = collections[c_uuid]['patients']
        for p_uuid in collection_df['p_uuid'].unique():
            patient_df = df.loc[df['p_uuid'] == p_uuid]
            patients[p_uuid] = {
                'submitter_case_id': patient_df['submitter_case_id'].unique()[0],
                'p_init_idc_version': patient_df['p_init_idc_version'].unique()[0],
                'p_rev_idc_version': patient_df['p_rev_idc_version'].unique()[0],
                'p_final_idc_version': patient_df['p_final_idc_version'].unique()[0],
                'studies': {}
            }
            studies = patients[p_uuid]['studies']
            for st_uuid in patient_df['st_uuid'].unique():
                study_df = df.loc[df['st_uuid'] == st_uuid]
                studies[st_uuid] = {
                    'StudyInstanceUID': study_df['StudyInstanceUID'].unique()[0],
                    'st_init_idc_version': study_df['st_init_idc_version'].unique()[0],
                    'st_rev_idc_version': study_df['st_rev_idc_version'].unique()[0],
                    'st_final_idc_version': study_df['st_final_idc_version'].unique()[0],
                    'series': {}
                }
                series = studies[st_uuid]['series']
                for se_uuid in study_df['se_uuid'].unique():
                    series_df = df.loc[df['se_uuid'] == se_uuid]
                    series[se_uuid] = {
                        'SeriesInstanceUID': series_df['SeriesInstanceUID'].unique()[0],
                        'se_init_idc_version': series_df['se_init_idc_version'].unique()[0],
                        'se_rev_idc_version': series_df['se_rev_idc_version'].unique()[0],
                        'se_final_idc_version': series_df['se_final_idc_version'].unique()[0],
                        'instances': {}
                    }
                    instances = series[se_uuid]['instances']
                    for i_uuid in series_df['i_uuid'].unique():
                        instances_df = df.loc[df['i_uuid'] == i_uuid]
                        instances[i_uuid] = {
                            'SOPInstanceUID': instances_df['SOPInstanceUID'].unique()[0],
                            'i_init_idc_version': instances_df['se_init_idc_version'].unique()[0],
                            'i_rev_idc_version': instances_df['se_rev_idc_version'].unique()[0],
                            'i_final_idc_version': instances_df['se_final_idc_version'].unique()[0],
                            'dev_bucket': instances_df['dev_bucket'].unique()[0],
                            'pub_gcs_bucket': instances_df['pub_gcs_bucket'].unique()[0],
                            'pub_aws_bucket': instances_df['pub_aws_bucket'].unique()[0],
                            'i_source': instances_df['i_source'].unique()[0],
                        }

    return

if __name__ == '__main__':
    create_hierarchy()