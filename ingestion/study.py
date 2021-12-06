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


import time
from datetime import datetime, timedelta
import logging
from uuid import uuid4
from idc.models import Study, Series
from ingestion.utils import accum_sources
from ingestion.series import clone_series, build_series

rootlogger = logging.getLogger('root')
errlogger = logging.getLogger('root.err')


def clone_study(study, uuid):
    new_study = Study(uuid=uuid)
    for key, value in study.__dict__.items():
        if key not in ['_sa_instance_state', 'uuid', 'patients', 'seriess']:
            setattr(new_study, key, value)
    for series in study.seriess:
        new_study.seriess.append(series)
    return new_study


def expand_study(sess, args, all_sources, study, data_collection_doi, analysis_collection_dois):
    # Get the series that the sources know about
    seriess = all_sources.series(study)

    if len(seriess) != len(set(seriess)):
        errlogger.error("\tp%s: Duplicate series in expansion of study %s", args.id,
                        study.study_instance_uid)
        raise RuntimeError("p%s: Duplicate series expansion of study %s", args.id,
                           study.study_instance_uid)

    if study.is_new:
        for series in sorted(seriess):
            rev_series = Series()
            rev_series.series_instance_uid = series
            if args.build_mtm_db:
                rev_series.uuid = seriess[series]['uuid']
                rev_series.min_timestamp = seriess[series]['min_timestamp']
                rev_series.max_timestamp = seriess[series]['max_timestamp']
                rev_series.source_doi = seriess[series]['source_doi']
                rev_series.series_instances = seriess[series]['series_instances']
                rev_series.sources = seriess[series]['sources']
                rev_series.hashes = seriess[series]['hashes']
            else:
                rev_series.uuid = str(uuid4())
                rev_series.min_timestamp = datetime.utcnow()
                rev_series.source_doi=analysis_collection_dois[series] \
                    if series in analysis_collection_dois \
                    else data_collection_doi
                rev_series.series_instances = 0
                rev_series.sources = (False, False)
                rev_series.hashes = ("", "", "")
            rev_series.max_timestamp = rev_series.min_timestamp
            rev_series.init_idc_version=args.version
            rev_series.rev_idc_version=args.version
            rev_series.final_idc_version = 0
            rev_series.revised=False
            rev_series.done=False
            rev_series.is_new=True
            rev_series.expanded=False
            study.seriess.append(rev_series)

    else:
        idc_objects = {object.series_instance_uid: object for object in study.seriess}

        new_objects = sorted([id for id in seriess if id not in idc_objects])
        retired_objects = sorted([idc_objects[id] for id in idc_objects if id not in seriess], key=lambda series: series.series_instance_uid)
        existing_objects =sorted( [idc_objects[id] for id in seriess if id in idc_objects], key=lambda series: series.series_instance_uid)

        for series in retired_objects:
            rootlogger.info('Series %s:%s retiring', series.series_instance_uid, series.uuid)
            series.final_idc_version = args.previous_version

            # retire_series(sess, args, series, source)
            # # if the series does not include instances from an source, delete it
            # if not any(series.sources):
            #     sess.delete(series)

        for series in existing_objects:
            if all_sources.series_was_updated(series):
                rootlogger.debug('**Series %s needs revision', series.series_instance_uid)
                rev_series = clone_series(series,series[series.series_instance_uid]['uuid'] if args.build_mtm_db else str(uuid4()))
                assert args.version == series[series.series_instance_uid]['rev_idc_version']
                # rev_series.uuid = str(uuid4())
                rev_series.rev_idc_version = args.version
                rev_series.revised = True
                rev_series.done = False
                rev_series.is_new = False
                rev_series.expanded = False
                if args.build_mtm_db:
                    rev_series.min_timestamp = series[series.series_instance_uid]['min_timestamp']
                    rev_series.max_timestamp = series[series.series_instance_uid]['max_timestamp']
                    rev_series.sources = series[series.series_instance_uid]['sources']
                    rev_series.hashes = series[series.series_instance_uid]['hashes']
                    rev_series.rev_idc_version = series[series.series_instance_uid]['rev_idc_version']
                else:
                    rev_series.rev_idc_version = args.version
                study.seriess.append(rev_series)

                # Mark the now previous version of this object as having been replaced
                # and drop it from the revised study
                series.final_idc_version = args.previous_version
                study.seriess.remove(series)
            else:
                # The series is unchanged. Just add it to the study.
                if not args.build_mtm_db:
                    # Stamp this series showing when it was checked
                    series.min_timestamp = datetime.utcnow()
                    series.max_timestamp = datetime.utcnow()
                    # Make sure the collection is marked as done and expanded
                    # Shouldn't be needed if the previous version is done
                    series.done = True
                    series.expanded = True
                rootlogger.debug('Series %s unchanged', series.series_instance_uid)
                # study.seriess.append(series)

        for series in sorted(new_objects):
            rev_series = Series()
            rev_series.series_instance_uid = series
            if args.build_mtm_db:
                rev_series.uuid = seriess[series]['uuid']
                rev_series.min_timestamp = seriess[series]['min_timestamp']
                rev_series.max_timestamp = seriess[series]['max_timestamp']
                rev_series.source_doi = seriess[series]['source_doi']
                rev_series.series_instances = seriess[series]['series_instances']
                rev_series.sources = seriess[series]['sources']
                rev_series.hashes = seriess[series]['hashes']
            else:
                rev_series.uuid = str(uuid4())
                rev_series.min_timestamp = datetime.utcnow()
                rev_series.source_doi = analysis_collection_dois[series] \
                    if series in analysis_collection_dois \
                    else data_collection_doi
                rev_series.series_instances = 0
                rev_series.sources = (False, False)
                rev_series.hashes = ("", "", "")
            rev_series.max_timestamp = rev_series.min_timestamp
            rev_series.init_idc_version = args.version
            rev_series.rev_idc_version = args.version
            rev_series.final_idc_version = 0
            rev_series.revised = False
            rev_series.done = False
            rev_series.is_new = True
            rev_series.expanded = False
            study.seriess.append(rev_series)

    study.expanded = True
    sess.commit()
    # rootlogger.debug("    p%s: Expanded study %s",args.id,  study.study_instance_uid)
    return

def build_study(sess, args, all_sources, study_index, version, collection, patient, study, data_collection_doi, analysis_collection_dois):
    begin = time.time()
    if not study.expanded:
        expand_study(sess, args, all_sources, study, data_collection_doi, analysis_collection_dois)
    rootlogger.info("    p%s: Study %s, %s, %s series, expand time: %s", args.id, study.study_instance_uid, study_index, len(study.seriess), time.time()-begin)
    for series in study.seriess:
        series_index = f'{study.seriess.index(series) + 1} of {len(study.seriess)}'
        if not series.done:
            build_series(sess, args, all_sources, series_index, version, collection, patient, study, series)
        else:
            rootlogger.info("      p%s: Series %s, %s, previously built", args.id, series.series_instance_uid, series_index)

    if all([series.done for series in study.seriess]):
        # study.min_timestamp = min([series.min_timestamp for series in study.seriess if series.min_timestamp != None])
        study.max_timestamp = max([series.max_timestamp for series in study.seriess if series.max_timestamp != None])
        if args.build_mtm_db:
            study.done = True
            sess.commit()
            duration = str(timedelta(seconds=(time.time() - begin)))
            rootlogger.info("    p%s: Study %s, %s,  completed in %s", args.id, study.study_instance_uid, study_index, duration)
        else:
            # Get hash of children
            hashes = all_sources.idc_study_hashes
            if all_sources.src_study_hashes(study.study_instance_uid) != hashes:
                # errlogger.error('Hash match failed for study %s', study.study_instance_uid)
                raise Exception('Hash match failed for study %s', study.study_instance_uid)
            else:
                # Test whether anything has changed
                if hash != study.hashes:
                    study.hashes = hashes
                    study.sources = accum_sources(study, study.seriess)
                    study.study_instances = sum([series.series_instances for series in study.seriess])
                    study.rev_idc_version = args.version
                else:
                    rootlogger.info("    p%s: Study %s, %s, unchanged", args.id, study.study_instance_uid,
                                    study_index)

                study.done = True
                sess.commit()
                duration = str(timedelta(seconds=(time.time() - begin)))
                rootlogger.info("    p%s: Study %s, %s,  completed in %s", args.id, study.study_instance_uid, study_index, duration)


