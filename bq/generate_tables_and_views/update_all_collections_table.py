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

# Update the all_collections table, with new collections added during ingestion
# This presumes that all such collections have neither a CR license nor are
# considered potential candidates for defacing. Therefore all collections will
# have 'Public' access and will be in the GCS public-datasets-idc and AWS idc-open-data buckets.

from idc.models import Base, Collection, All_Collections, Collection_id_map
import settings
from sqlalchemy import create_engine, func, or_
from sqlalchemy.orm import Session
from utilities.logging_config import progresslogger


def prebuild():

    sql_uri = f'postgresql+psycopg2://{settings.CLOUD_USERNAME}:{settings.CLOUD_PASSWORD}@{settings.CLOUD_HOST}:{settings.CLOUD_PORT}/{settings.CLOUD_DATABASE}'
    # sql_engine = create_engine(sql_uri, echo=True)
    sql_engine = create_engine(sql_uri)
    Base.metadata.create_all(sql_engine)

    with Session(sql_engine) as sess:
        for collection_id, idc_collection_id in sess.query(Collection_id_map.collection_id, Collection_id_map.idc_collection_id).outerjoin(All_Collections, Collection_id_map.idc_webapp_collection_id==  \
                func.replace(func.replace(func.lower(All_Collections.tcia_api_collection_id),'-','_'),' ','_')).\
                filter(All_Collections.tcia_api_collection_id==None).all():
            sources = sess.query(Collection.sources).filter(Collection.collection_id==collection_id).one()[0]
            collection = All_Collections(
                    tcia_api_collection_id=collection_id,
                   idc_collection_id=idc_collection_id,
                   dev_tcia_url='idc-dev-open' if sources.tcia else None,
                   dev_idc_url='idc-dev-open' if sources.idc else None,
                   pub_gcs_tcia_url='public-datasets-idc' if sources.tcia else None,
                   pub_gcs_idc_url='public-datasets-idc' if sources.idc else None,
                   pub_aws_tcia_url='idc-open-data' if sources.tcia else None,
                   pub_aws_idc_url='idc-open-data' if sources.idc else None,
                   tcia_access='Public' if sources.tcia else None,
                   idc_access='Public' if sources.idc else None,
                   tcia_metadata_sunset=0,
                   idc_metadata_sunset=0)
            sess.add(collection)
            progresslogger.info(f'Added collection {collection}')
        # sess.commit()
    with Session(sql_engine) as sess:
        collections = sess.query(All_Collections).outerjoin(Collection, Collection.idc_collection_id== \
                All_Collections.idc_collection_id).\
                filter(Collection.rev_idc_version==settings.CURRENT_VERSION).filter( or_(Collection.sources==(True,True), Collection.sources==(False,True))).all() #.filter(Collection.sources.==True).all()
        for collection in collections:
            if collection.dev_idc_url== None:
                collection.dev_idc_url='idc-dev-open'
                collection.pub_gcs_idc_url='public-datasets-idc'
                collection.pub_aws_idc_url='idc-open-data'
                collection.idc_access='Public'
                collection.idc_metadata_sunset=0

                progresslogger.info(f'Updated collection {collection.tcia_api_collection_id}')
        sess.commit()

    return


if __name__ == '__main__':
    prebuild()


