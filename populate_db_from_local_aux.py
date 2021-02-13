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

from idc_sqlalchemy.sqlalchemy_orm_models import Version, Collection, Patient, Study, Series, Instance, Auxilliary_Metadata, \
    sql_engine
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

def populate_versions(Session):
    with Session() as session:
        stmt = select(
            Auxilliary_Metadata.idc_version_number,
            Auxilliary_Metadata.idc_version_timestamp
        ).distinct()
        result = session.execute(stmt)
        for row in result:
            session.add(
                Version(
                    idc_version_number=row.idc_version_number,
                    idc_version_timestamp=row.idc_version_timestamp,
                )
            )
        try:
            session.commit()
        except IntegrityError as e:
            print(e)
    pass


def populate_collections(Session):

    with Session() as session:
        stmt = select(
            Version.id,
            Version.idc_version_number,
            Version.idc_version_timestamp
        ).distinct()
        versions = session.execute(stmt)
        for version in versions:
            stmt = select(
                Auxilliary_Metadata.tcia_api_collection_id,
                Auxilliary_Metadata.idc_version_number,
                Auxilliary_Metadata.idc_version_timestamp,
            ).where(Auxilliary_Metadata.idc_version_number == version.idc_version_number).distinct()
            aux_versions = session.execute(stmt)
            for row in aux_versions:
                session.add(
                    Collection(
                        version = version.id,
                        tcia_api_collection_id = row.tcia_api_collection_id,
                        idc_version_number=row.idc_version_number,
                        idc_version_timestamp=row.idc_version_timestamp
                    )
                )
            try:
                session.commit()
            except IntegrityError as e:
                print(e)
    pass


def populate_patients(Session):
    with Session() as session:
        stmt = select(
            Collection.id,
            Collection.idc_version_number,
            Collection.idc_version_timestamp,
            Collection.tcia_api_collection_id
        ).distinct()
        collections = session.execute(stmt)
        for collection in collections:
            stmt = select(
                Auxilliary_Metadata.submitter_case_id,
                Auxilliary_Metadata.crdc_case_id,
                Auxilliary_Metadata.idc_version_number,
                Auxilliary_Metadata.idc_version_timestamp,
            ).where(
                collection.idc_version_number == Auxilliary_Metadata.idc_version_number and \
                collection.tcia_api_collection_id == Auxilliary_Metadata.tcia_api_collection_id).distinct()
            aux_patients = session.execute(stmt)
            for row in aux_patients:
                session.add(
                    Patient(
                        collection = collection.id,
                        submitter_case_id = row.submitter_case_id,
                        crdc_case_id = row.crdc_case_id,
                        idc_version_number=row.idc_version_number,
                        idc_version_timestamp=row.idc_version_timestamp,
                    )
                )
            try:
                session.commit()
            except IntegrityError as e:
                print(e)
        pass


def populate_studies(Session):
    with Session() as session:
        stmt = select(
            Patient.id,
            Patient.idc_version_number,
            Patient.idc_version_timestamp,
            Patient.submitter_case_id
        ).distinct()
        patients = session.execute(stmt)
        for patient in patients:
            stmt = select(
                    Auxilliary_Metadata.study_uuid,
                    Auxilliary_Metadata.study_instance_uid,
                    Auxilliary_Metadata.study_instances,
                    Auxilliary_Metadata.idc_version_number,
                    Auxilliary_Metadata.idc_version_timestamp,
                 ).where(
                    patient.idc_version_number == Auxilliary_Metadata.idc_version_number and \
                    patient.submitter_case_id == Auxilliary_Metadata.submitter_case_id).distinct()
            result = session.execute(stmt)
            for row in result:
                session.add(
                    Study(
                        patient = patient.id,
                        study_uuid = row.study_uuid,
                        study_instance_uid = row.study_instance_uid,
                        study_instances = row.study_instances,
                        idc_version_number=row.idc_version_number,
                        idc_version_timestamp=row.idc_version_timestamp,
                     )
                )
            try:
                session.commit()
            except IntegrityError as e:
                print(e)
    pass


def populate_series(Session):
    with Session() as session:
        stmt = select(
            Auxilliary_Metadata.series_uuid,
            Auxilliary_Metadata.series_instance_uid,
            Auxilliary_Metadata.series_instances,
            Auxilliary_Metadata.idc_version_number,
            Auxilliary_Metadata.idc_version_timestamp,
            Auxilliary_Metadata.study_uuid,
        ).distinct()
        result = session.execute(stmt)
        for row in result:
            session.add(
                Series(
                    series_uuid = row.series_uuid,
                    series_instance_uid = row.series_instance_uid,
                    series_instances = row.series_instances,
                    idc_version_number=row.idc_version_number,
                    idc_version_timestamp=row.idc_version_timestamp,
                    study_uuid = row.study_uuid,
            ))
        try:
            session.commit()
        except IntegrityError as e:
            print(e)
    pass


def populate_instances(Session):
    with Session() as session:
        stmt = select(
            Auxilliary_Metadata.instance_uuid,
            Auxilliary_Metadata.sop_instance_uid,
            Auxilliary_Metadata.idc_version_number,
            Auxilliary_Metadata.idc_version_timestamp,
            Auxilliary_Metadata.series_uuid,
            Auxilliary_Metadata.gcs_url,
            Auxilliary_Metadata.instance_hash,
            Auxilliary_Metadata.instance_size
        ).distinct()
        result = session.execute(stmt)
        rows = result.all()

    limit = 100000
    total = len(rows)
    index = 0
    while index < total:
        with Session() as session:

            while True:
                row = rows[index]
                session.add(
                    Instance(
                        series_uuid = row.series_uuid,
                        instance_uuid = row.instance_uuid,
                        idc_version_number=row.idc_version_number,
                        idc_version_timestamp=row.idc_version_timestamp,
                        sop_instance_uid = row.sop_instance_uid,
                        gcs_url = row.gcs_url,
                        instance_hash = row.instance_hash,
                        instance_size = row.instance_size
                    )
                )
                index += 1
                if index%limit == 0:
                    break
                if index == total:
                    break
            try:
                session.commit()
            except IntegrityError as e:
                print(e)
    pass


def populate_db():
    Session = sessionmaker(bind= sql_engine)

    # populate_versions(Session)
    # populate_collections(Session)
    # populate_patients(Session)
    populate_studies(Session)
    populate_series(Session)
    populate_instances(Session)
    pass






if __name__ == '__main__':
    populate_db()





