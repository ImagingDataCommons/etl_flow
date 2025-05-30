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
from idc.models import Series, Instance, instance_source
from ingestion.instance import clone_instance, build_instances_idc, build_instances_tcia
from ingestion.utilities.utils import is_skipped
from python_settings import settings


# successlogger = logging.getLogger('root.success')
# progresslogger = logging.getLogger('root.progress')
# errlogger = logging.getLogger('root.err')


def clone_series(series, uuid):
    new_series = Series(uuid=uuid)
    for key, value in series.__dict__.items():
        if key not in ['_sa_instance_state', 'uuid', 'studies', 'instances']:
            setattr(new_series, key, value)
    for instance in series.instances:
        new_series.instances.append(instance)
    return new_series

def retire_series(args, series):
    # If this object has children from source, mark them as retired
    progresslogger.debug('      p%s: Series %s:%s retiring', args.pid, series.series_instance_uid, series.uuid)
    for instance in series.instances:
        progresslogger.debug('        p%s: Instance %s:%s retiring', args.pid, instance.sop_instance_uid, instance.uuid)
        instance.final_idc_version = settings.PREVIOUS_VERSION
    series.final_idc_version = settings.PREVIOUS_VERSION


def expand_series(sess, args, all_sources, version, collection, patient, study, series):
    skipped = is_skipped(args.skipped_collections, collection.collection_id)
    # Get the instances that the sources know about
    # All sources should be from a single source
    instances = all_sources.instances(collection, series, skipped)

    if len(instances) != len(set(instances)):
        errlogger.error("\tp%s: Duplicate instance in expansion of series %s", args.pid,
                        series.series_instance_uid)
        raise RuntimeError("p%s: Duplicate instance in  expansion of series %s", args.pid,
                           series.series_instance_uid)

    if series.is_new:
        # All patients are new by definition
        new_objects = instances
        retired_objects = []
        existing_objects = []
    else:
        # Get a list of the instances that we currently have in this series
        # We assume that a series has instances from a single source
        # breakpoint()
        idc_objects = {object.sop_instance_uid: object for object in series.instances}

        # If any (non-skipped) source has an object but IDC does not, it is new. Note that we don't get objects from
        # skipped collections
        new_objects = sorted([id for id, source in instances.items() \
               if id not in idc_objects ])
        # An object in IDC will continue to exist if any non-skipped source has the object or IDC's object has a
        # skipped source. I.E. if an object has a skipped source then, we can't ask the source about it so assume
        # it exists.
        existing_objects = [obj for id, obj in idc_objects.items() \
            if id in instances or (obj.source and skipped[obj.source.value])]
        # An object in IDC is retired if it no longer exists in IDC
        retired_objects = [obj for id, obj in idc_objects.items() \
               if not obj in existing_objects ]

    for instance in sorted(new_objects):
        new_instance = Instance()
        new_instance.sop_instance_uid=instance
        new_instance.uuid=str(uuid4())
        new_instance.size=0
        new_instance.revised=True
        new_instance.done=False
        new_instance.is_new=True
        new_instance.expanded=False
        new_instance.init_idc_version=settings.CURRENT_VERSION
        new_instance.rev_idc_version=settings.CURRENT_VERSION
        new_instance.source = instances[instance]
        new_instance.hash = None
        new_instance.timestamp = datetime.utcnow()
        new_instance.final_idc_version = 0
        series.instances.append(new_instance)
        progresslogger.debug('        p%s: Instance %s is new', args.pid, new_instance.sop_instance_uid)

    for instance in existing_objects:
        idc_hash = instance.hash
        src_hash = all_sources.src_instance_hashes(instance.sop_instance_uid, instances[instance.sop_instance_uid])
        revised = idc_hash != src_hash
        # if any(revised):
        if revised:
            rev_instance = clone_instance(instance, str(uuid4()))
            rev_instance.revised = True
            rev_instance.done = False
            rev_instance.is_new = False
            rev_instance.expanded = True
            rev_instance.timestamp = datetime.utcnow()
            rev_instance.source = instances[instance.sop_instance_uid]
            rev_instance.hash = None
            rev_instance.size = 0
            rev_instance.rev_idc_version = settings.CURRENT_VERSION
            series.instances.append(rev_instance)
            progresslogger.debug('        p%s: Instance %s is revised', args.pid, rev_instance.sop_instance_uid)

            # Mark the now previous version of this object as having been replaced
            # and drop it from the revised series
            instance.final_idc_version = settings.CURRENT_VERSION-1
            series.instances.remove(instance)

        else:
            instance.timestamp = datetime.utcnow()
            # Make sure the collection is marked as done and expanded
            # Shouldn't be needed if the previous version is done
            instance.done = True
            instance.expanded = True
            progresslogger.debug('        p%s: Instance %s unchanged', args.pid, instance.sop_instance_uid)
            # series.instances.append(instance)

    for instance in retired_objects:
        # rootlogger.debug('        p%s: Instance %s:%s retiring', instance.sop_instance_uid, instance.uuid)
        instance.final_idc_version = settings.PREVIOUS_VERSION
        series.instances.remove(instance)

    series.expanded = True
    sess.commit()
    return 0


def build_series(sess, args, all_sources, series_index, version, collection, patient, study, series):
    try:
        begin = time.time()
        successlogger.debug("      p%s: Expand Series %s; %s", args.pid, series.series_instance_uid, series_index)
        if not series.expanded:
            failed = expand_series(sess, args, all_sources, version, collection, patient, study, series)
            if failed:
                return
        successlogger.info("      p%s: Expanded Series %s; %s; %s instances, expand time: %s", args.pid, series.series_instance_uid, series_index, len(series.instances), time.time()-begin)
        try:
            # Verify that series has a single source
            assert sum(1 for source in series.sources if source) == 1
        except Exception as exc:
            errlogger.error(f'Series does not have exactly one source')

        if not all(instance.done for instance in series.instances):
            if series.sources.tcia:
                build_instances_tcia(sess, args, collection, patient, study, series)
            if series.sources.idc:
                # Get instance data from idc DB table/ GCS bucket.
                build_instances_idc(sess, args, collection, patient, study, series)

        if all(instance.done for instance in series.instances):
            # series.min_timestamp = min(instance.timestamp for instance in series.instances)
            series.max_timestamp = max(instance.timestamp for instance in series.instances)
            # Get a list of what DB thinks are the series's hashes
            idc_hashes = all_sources.idc_series_hashes(series)
            # # Get a list of what the sources think are the series's hashes
            series.hashes = idc_hashes
            series.series_instances = len(series.instances)

            skipped = is_skipped(args.skipped_collections, collection.collection_id)
            src_hashes = all_sources.src_series_hashes(collection.collection_id, series.series_instance_uid, skipped)
            revised = [(x != y) and not z for x, y, z in \
                       zip(idc_hashes[:-1], src_hashes, skipped)]
            if any(revised):
                # raise Exception('Hash match failed for series %s', series.series_instance_uid)
                errlogger.error('Hash match failed for series %s', series.series_instance_uid)
            else:
                series.done = True
                sess.commit()
                duration = str(timedelta(seconds=(time.time() - begin)))
                successlogger.info("      p%s: Built Series %s, %s, in %s", args.pid, series.series_instance_uid, series_index, duration)
    except Exception as exc:
        errlogger.error('  p%s build_series failed: %s for %s', args.pid, exc, series.series_instance_uid)
        raise exc