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
from utilities.logging_config import successlogger, progresslogger, errlogger
from uuid import uuid4
from idc.models import Study, Series, instance_source
from ingestion.utilities.utils import accum_sources, get_merkle_hash, is_skipped
from ingestion.series import clone_series, build_series, retire_series

from python_settings import settings

# successlogger = logging.getLogger('root.success')
# progresslogger = logging.getLogger('root.progressr')
# errlogger = logging.getLogger('root.err')


def clone_study(study, uuid):
    new_study = Study(uuid=uuid)
    for key, value in study.__dict__.items():
        if key not in ['_sa_instance_state', 'uuid', 'patients', 'seriess']:
            setattr(new_study, key, value)
    for series in study.seriess:
        new_study.seriess.append(series)
    return new_study


def retire_study(args, study ):
    # If this object has children from source, delete them
    progresslogger.debug('    p%s: Study %s:%s retiring', args.pid, study.study_instance_uid, study.uuid)
    for series in study.seriess:
        retire_series(args, series)
    study.final_idc_version = settings.PREVIOUS_VERSION


# def expand_study(sess, args, all_sources, version, collection, patient, study, data_collection_doi_url, analysis_collection_dois):
def expand_study(sess, args, all_sources, version, collection, patient, study, dois_urls_licenses):
    skipped = is_skipped(args.skipped_collections, collection.collection_id)
    # Get the series that the sources know about
    seriess = all_sources.series(study, skipped)

    if len(seriess) != len(set(seriess)):
        errlogger.error("\tp%s: Duplicate series in expansion of study %s", args.pid,
                        study.study_instance_uid)
        raise RuntimeError("p%s: Duplicate series expansion of study %s", args.pid,
                           study.study_instance_uid)

    if study.is_new:
        # All patients are new by definition
        new_objects = seriess
        retired_objects = []
        existing_objects = []
    else:
        # Get the IDs of the series that we have.
        idc_objects = {object.series_instance_uid: object for object in study.seriess}
        # If any (non-skipped) source has an object but IDC does not, it is new. Note that we don't get objects from
        # skipped collections
        new_objects = sorted([id for id in seriess \
                if not id in idc_objects])
        # An object in IDC will continue to exist if any non-skipped source has the object or IDC's object has a
        # skipped source. I.E. if an object has a skipped source then, we can't ask the source about it so assume
        # it exists.
        existing_objects = [obj for id, obj in idc_objects.items() \
                if id in seriess or any([a and b for a, b in zip(obj.sources,skipped)])]
        # An object in IDC is retired if it no longer exists in IDC
        retired_objects = [obj for id, obj in idc_objects.items() \
                if not obj in existing_objects]

    for series in sorted(new_objects):
        new_series = Series()
        new_series.series_instance_uid = series
        new_series.uuid = str(uuid4())
        new_series.min_timestamp = datetime.utcnow()
        try:
            new_series.source_doi = dois_urls_licenses[series]['source_doi']
            new_series.source_url = dois_urls_licenses[series]['source_url']
            new_series.versioned_source_doi = dois_urls_licenses[series]['versioned_source_doi']
            new_series.license_url = dois_urls_licenses[series]['license']['license_url']
            new_series.license_long_name = dois_urls_licenses[series]['license']['license_long_name']
            new_series.license_short_name = dois_urls_licenses[series]['license']['license_short_name']
        except Exception as exc:
            errlogger.error(f'No DOI/URL for series {series.series_instance_uid}')
            return
        new_series.series_instances = 0
        new_series.revised = seriess[series]
        new_series.sources = seriess[series]
        new_series.hashes = ("","","")
        new_series.max_timestamp = new_series.min_timestamp
        new_series.init_idc_version=settings.CURRENT_VERSION
        new_series.rev_idc_version=settings.CURRENT_VERSION
        new_series.final_idc_version = 0
        new_series.done=False
        new_series.is_new=True
        new_series.expanded=False
        study.seriess.append(new_series)
        progresslogger.debug('      p%s:Series %s new', args.pid, new_series.series_instance_uid)

    for series in existing_objects:
        idc_hashes = series.hashes

        # Get the hash from each source that is not skipped
        # The hash of a source is "" if the source is skipped, or the source that does not have
        # the object
        src_hashes = all_sources.src_series_hashes(collection.collection_id, series.series_instance_uid, skipped)
        # A source is revised the idc hashes[source] and the source hash differ and the source is not skipped
        revised = [(x != y) and not z for x, y, z in \
                   zip(idc_hashes[:-1], src_hashes, skipped)]
        # If any source is revised, then the object is revised.
        if any(revised):
            progresslogger.debug('**Series %s needs revision', series.series_instance_uid)
            rev_series = clone_series(series, str(uuid4()))
            rev_series.min_timestamp = datetime.utcnow()
            rev_series.rev_idc_version = settings.CURRENT_VERSION
            # rev_series.revised = True
            rev_series.done = False
            rev_series.is_new = False
            rev_series.expanded = False
            rev_series.revised = revised
            rev_series.hashes = ("","","")
            rev_series.sources = seriess[series.series_instance_uid]
            rev_series.rev_idc_version = settings.CURRENT_VERSION
            rev_series.versioned_source_doi = dois_urls_licenses[series.series_instance_uid]['versioned_source_doi']
            study.seriess.append(rev_series)
            progresslogger.debug('      p%s:Series %s revised',  args.pid, rev_series.series_instance_uid)

            # Mark the now previous version of this object as having been replaced
            # and drop it from the revised study
            series.final_idc_version = settings.PREVIOUS_VERSION
            study.seriess.remove(series)
        else:
            # The series is unchanged. Just add it to the study.
            # Stamp this series showing when it was checked
            series.min_timestamp = datetime.utcnow()
            series.max_timestamp = datetime.utcnow()
            # Make sure the collection is marked as done and expanded
            # Shouldn't be needed if the previous version is done
            series.done = True
            series.expanded = True
            progresslogger.debug('      p%s: Series %s unchanged',  args.pid, series.series_instance_uid)

    for series in retired_objects:
        retire_series(args, series)
        study.seriess.remove(series)

    study.expanded = True
    sess.commit()
    return

# def build_study(sess, args, all_sources, study_index, version, collection, patient, study, data_collection_doi_url, analysis_collection_dois):
def build_study(sess, args, all_sources, study_index, version, collection, patient, study, dois_urls_licenses):

    try:
        begin = time.time()
        successlogger.debug("    p%s: Expand Study %s, %s", args.pid, study.study_instance_uid, study_index)
        if not study.expanded:
            # expand_study(sess, args, all_sources, version, collection, patient, study, data_collection_doi_url, analysis_collection_dois)
            expand_study(sess, args, all_sources, version, collection, patient, study, dois_urls_licenses)
        successlogger.info("    p%s: Expanded Study %s, %s, %s series, expand time: %s", args.pid, study.study_instance_uid, study_index, len(study.seriess), time.time()-begin)
        for series in study.seriess:
            series_index = f'{study.seriess.index(series) + 1} of {len(study.seriess)}'
            if not series.done:
                build_series(sess, args, all_sources, series_index, version, collection, patient, study, series)
                # Verify that source is consistent
                if (series.hashes[instance_source.tcia.value] == "" and series.sources.tcia == True) or \
                        (series.hashes[instance_source.idc.value] == "" and series.sources.idc == True):
                    breakpoint()
            else:
                successlogger.info("      p%s: Series %s, %s, previously built", args.pid, series.series_instance_uid, series_index)

        if all([series.done for series in study.seriess]):
            study.max_timestamp = max([series.max_timestamp for series in study.seriess if series.max_timestamp != None])
            # Get a list of what DB thinks are the study's hashes
            idc_hashes = all_sources.idc_study_hashes(study)
            study.hashes = idc_hashes
            study.sources = accum_sources(study, study.seriess)
            study.study_instances = sum([series.series_instances for series in study.seriess])

            skipped = is_skipped(args.skipped_collections, collection.collection_id)
            src_hashes = all_sources.src_study_hashes(collection.collection_id, study.study_instance_uid, skipped)
            revised = [(x != y) and not z for x, y, z in \
                       zip(idc_hashes[:-1], src_hashes, skipped)]
            if any(revised):
                # raise Exception('Hash match failed for study %s', study.study_instance_uid)
                errlogger.error('Hash match failed for study %s', study.study_instance_uid)
            else:
                study.done = True
                sess.commit()
                duration = str(timedelta(seconds=(time.time() - begin)))
                successlogger.info("    p%s: Built Study %s, %s,  in %s", args.pid, study.study_instance_uid, study_index, duration)
    except Exception as exc:
        errlogger.error('  p%s build_study failed: %s for %s', args.pid, exc, study.study_instance_uid)
        raise exc


