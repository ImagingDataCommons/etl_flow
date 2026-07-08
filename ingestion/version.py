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
from idc.models import instance_source, Version, Collection
from ingestion.utilities.utils import accum_sources, is_skipped
from ingestion.collection import clone_collection, build_collection, retire_collection
from egestion.egest import egest_version

from python_settings import settings

# successlogger = logging.getLogger('root.success')
# progresslogger = logging.getLogger('root.progress')
# errlogger = logging.getLogger('root.err')


def clone_version(previous_version, new_version):
    new_version = Version(version=new_version)
    for key, value in previous_version.__dict__.items():
        if key not in ['_sa_instance_state','collections','version']:
            setattr(new_version, key, value)
    for collection in previous_version.collections:
        new_version.collections.append(collection)
    return new_version

def expand_version(sess, args, all_sources, version):
    # Get a list of collection_names that are in the pre_ingestion snapshot. Note that in the case
    # of a collection not previously in the Pre DB, the collection is added to the collection_id_map table
    #
    # For each collection, returns a vector of booleans, one for each source.
    # A boolean is True if the corresponding source 'has' the collection.
    collections = all_sources.collections()

    # Get the set of IDC collections that IDC has in the DB, and the sources hash
    idc_objects_results = version.collections

    # Index by idc_collection_uuid
    idc_objects = {c.idc_collection_uuid: c for c in idc_objects_results}

    # # We now extend the lists of skipped collections to include collections for which neither the DB nor the
    # # source have the collection. Note this is per source.
    # for idc_collection_uuid, collection in idc_objects.items():
    #     if idc_collection_uuid in collections:
    #         for source in instance_source:
    #             if source.name != 'all_sources':
    #                 if not collection.sources[source.value] and \
    #                         not collections[idc_collection_uuid]['sources'][source.value]:
    #                     if not collection.collection_name in args.skipped_collections:
    #                         args.skipped_collections[collection.collection_name] = [False,False]
    #                     args.skipped_collections[collection.collection_name][source.value] = True
    #     else:
    #         for source in instance_source:
    #             if source.name != 'all_sources':
    #                 if not collection.sources[source.value]:
    #                     if not collection.collection_name in args.skipped_collections:
    #                         args.skipped_collections[collection.collection_name] = [False,False]
    #                     args.skipped_collections[collection.collection_name][source.value] = True



    # We exclude collections that are skipped from all sources
    # for idc_object in list(idc_objects):
    #     if idc_objects[idc_object].collection_name in args.skipped_collections \
    #             and all(args.skipped_collections[idc_objects[idc_object].collection_name]):
    #         progresslogger.info(f'p%s: Excluding collection {idc_objects[idc_object].collection_name}. Skipped in all sources.')
    #         idc_objects.pop(idc_object)


    # We exclude collections that are skipped from all sources
    for idc_object in list(idc_objects):
        if idc_objects[idc_object].collection_name in args.skipped_collections:
            progresslogger.info(f'p%s: Excluding collection {idc_objects[idc_object].collection_name}. Skipped in all sources.')
            idc_objects.pop(idc_object)


    # Collections that are not previously known about by any source.
    new_objects = sorted( [id for id in collections if id not in idc_objects])

    # An object in IDC will continue to exist if any non-skipped source has the object or IDC's object has a
    # skipped source. I.E. if an object has a skipped source then, we can't ask the source about it so assume
    # it exists.
    # existing_objects = [obj for id, obj in idc_objects.items() if \
    #     id in collections or any([a and b for a, b in zip(obj.sources, is_skipped(args.skipped_collections, id))])]
    existing_objects = [obj for id, obj in idc_objects.items() if \
        id in collections or is_skipped(args.skipped_collections, id)]

    # Collections that are no longer known about by any source
    retired_objects = [obj for id, obj in idc_objects.items() \
       if not obj in existing_objects]

    for idc_collection_uuid in sorted(new_objects,
        key=lambda idc_collection_uuid: collections[idc_collection_uuid]['collection_name']):
        # if not collections[idc_collection_uuid]['collection_name'] in skipped:
        # The collection is new, so we must ingest it
        new_collection = Collection()
        new_collection.collection_name = collections[idc_collection_uuid]['collection_name']
        new_collection.idc_collection_uuid = idc_collection_uuid
        new_collection.uuid = str(uuid4())
        new_collection.min_timestamp = datetime.utcnow()
        new_collection.revised = True
        new_collection.hashes = ""
        new_collection.init_idc_version=settings.CURRENT_VERSION
        new_collection.rev_idc_version=settings.CURRENT_VERSION
        new_collection.final_idc_version=0
        new_collection.done = False
        new_collection.is_new = True
        new_collection.expanded = False

        version.collections.append(new_collection)
        progresslogger.info('p%s: Collection %s is new', args.pid, new_collection.collection_name)

    for collection in existing_objects:
        print(f"Check {collection.collection_name} for revisions")
        # if not collection.collection_name in skipped:
        idc_hash = collection.hash
        skipped =  collection.collection_name in args.skipped_collections
        # if this collection is excluded from a source, then ignore differing source and idc hashes in that source
        src_hash = all_sources.src_collection_hash(collection.collection_name, skipped)
        # revised = [(x != y) and not z for x, y, z in \
        #            zip(idc_hashes[:-1], src_hashes, skipped)]
        revised = idc_hash != src_hash and not skipped
        # The NBIA hash API is fast but unreliable on collection hashes. So if the 'tcia' hash doesn't match our
        # hash for the corresponding collection, we compute the tcia collection hash from patient hashes, which
        # have been reliable.
        # if src_hashes[all_sources.]
        if revised:
            # breakpoint()  # Should timestamps be updated here?
            rev_collection = clone_collection(collection, uuid=str(uuid4()))

            # Here is where we update the collecton ID in case it has changed
            rev_collection.collection_name = collections[collection.idc_collection_uuid]['collection_name']

            rev_collection.done = False
            rev_collection.is_new = False
            rev_collection.expanded = False
            rev_collection.hash = collection.hash
            rev_collection.revised = True
            rev_collection.rev_idc_version = settings.CURRENT_VERSION
            version.collections.append(rev_collection)
            progresslogger.info('p%s: Collection %s is revised',  args.pid, rev_collection.collection_name)

            # Mark the now previous version of this object as having been replaced
            # and drop it from the revised version
            collection.final_idc_version = settings.PREVIOUS_VERSION
            version.collections.remove(collection)
            continue
        elif collection.collection_name != collections[collection.idc_collection_uuid]['collection_name']:
            # The collection_name has changed. Treat as a revised collection even though the hash is unchanged
            progresslogger.info('**Collection_id changed %s. Generating revision',collection.collection_name)
            rev_collection = clone_collection(collection, uuid=str(uuid4()))

            # Here is where we update the collecton ID in case it has changed
            rev_collection.collection_name = collections[collection.idc_collection_uuid]['collection_name']
            rev_collection.rev_idc_version = settings.CURRENT_VERSION

            # The collection is otherwise done
            collection.done = True
            collection.expanded = True

            version.collections.append(rev_collection)
            progresslogger.info('p%s: Collection %s is renamed',  args.pid, rev_collection.collection_name)

            # Mark the now previous version of this object as having been replaced
            # and drop it from the revised version
            collection.final_idc_version = settings.PREVIOUS_VERSION
            version.collections.remove(collection)

        else:
            # The collection is unchanged. Just add it to the version
            collection.min_timestamp = datetime.utcnow()
            collection.max_timestamp = datetime.utcnow()
            # Make sure the collection is marked as done and expanded
            # Shouldn't be needed if the previous version is done
            collection.done = True
            collection.expanded = True
            progresslogger.info('p%s: Collection %s unchanged', args.pid, collection.collection_name)

    for collection in retired_objects:
        breakpoint()
        # Mark the now previous version of this object as having been retired
        retire_collection(args, collection)
        version.collections.remove(collection)
        progresslogger.info(f'p{args.pid}: Collection {collection.collection_name} is retired')

    progresslogger.info(f'\n\nVersion expansion summary')
    new_collections = []
    revised_collections = {}
    for collection in version.collections:
        if not collection.done:
            if collection.init_idc_version == collection.rev_idc_version:
                new_collections.append(collection.collection_name)
            else:
                revised_collections[collection.collection_name] = collection.revised
    progresslogger.info('New collections:')
    for collection_name in sorted(new_collections):
        progresslogger.info(collection_name)
    progresslogger.info('\nRevised collections:')

    for collection_name in sorted(revised_collections.keys()):
        progresslogger.info(f'{collection_name}: {revised_collections[collection_name]}')
    progresslogger.info('\nRetired collections:')
    for collection in retired_objects:
        progresslogger.info(collection.collection_name)
    if args.stop_after_collection_summary:
        exit()

    version.expanded = True
    sess.commit()
    progresslogger.info("Expanded version")

def build_version(sess, args, all_sources, version):
    begin = time.time()
    progresslogger.info("p%s: Expand version %s", args.pid, settings.CURRENT_VERSION)
    if not version.expanded:
        expand_version(sess, args, all_sources, version)
    idc_collections = sorted(version.collections, key=lambda collection: collection.collection_name)
    progresslogger.info("p%s: Expanded Version %s; %s collections", args.pid, settings.CURRENT_VERSION, len(idc_collections))
    for collection in idc_collections:
        collection_index = f'{idc_collections.index(collection) + 1} of {len(idc_collections)}'
        if not collection.done:
            progresslogger.info(f'Building collection {collection.collection_name}')
            build_collection(sess, args, all_sources, collection_index, version, collection)
            pass
        else:
            progresslogger.info("p%s: Collection %s, %s, previously built", args.pid, collection.collection_name, collection_index)

    # Check if we are really done
    if all([collection.done for collection in idc_collections]):
        version.max_timestamp = max([collection.max_timestamp for collection in version.collections if collection.max_timestamp != None])

        # Get the new version's hashes
        idc_hash = all_sources.idc_version_hash(version)
        # Check whether the hashes have changed. If so then declare this a new version
        # otherwise revert the version number
        previous_hash = list(sess.query(Version).filter(Version.version == settings.PREVIOUS_VERSION).first().hash)
        if idc_hash != previous_hash:
            version.hash = idc_hash
            version.done = True
            version.revised = True
            duration = str(timedelta(seconds=(time.time() - begin)))
            successlogger.info("Built Version %s, in %s", version.version, duration)
        else:
            # There was nothing new, so remove the new version from the DB
            breakpoint()
            egest_version(sess, args, version)
            sess.delete(version)
            progresslogger.info('Deleted version %s', version.version)

            successlogger.info("Version unchanged, remains at %s", settings.PREVIOUS_VERSION)
        sess.commit()

    else:
        progresslogger.info("Not all collections are done. Rerun.")
