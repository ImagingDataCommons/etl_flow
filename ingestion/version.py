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
from idc.models import Version, Collection
from ingestion.utils import accum_sources, get_merkle_hash
from ingestion.collection import clone_collection, build_collection, retire_collection
from ingestion.egest import egest_version

rootlogger = logging.getLogger('root')
errlogger = logging.getLogger('root.err')


def clone_version(previous_version, new_version):
    new_version = Version(version=new_version)
    for key, value in previous_version.__dict__.items():
        if key not in ['_sa_instance_state','collections','version']:
            setattr(new_version, key, value)
    for collection in previous_version.collections:
        new_version.collections.append(collection)
    return new_version


def expand_version(sess, args, all_sources, version, skips):
    # Get the collections that the sources know about
    # Returned data includes the sources vector
    collections = all_sources.collections()

    # Because collection IDs can change, collections is indexed by the idc_collection_id
    # of each collections. This mapping is maintained in the collection_id_map table.
    ### It must be manually updated with a new (collection_id, idc_collection_id) when the ingestion
    ### process will see a change in the collection_id for some collection.

    # Get the most recent set of collections.
    idc_objects_results = version.collections
    idc_objects = {c.idc_collection_id: c for c in idc_objects_results}

    # Collections that are not previously known ahout by any source
    new_objects =sorted( [idc_collection_id for idc_collection_id in collections if idc_collection_id not in idc_objects])

    # Collections that are no longer known about by any source
    retired_objects = [idc_objects[id] for id in idc_objects if id not in collections]

    # Collections that are in the previous version and still known about by some source
    existing_objects = sorted([idc_objects[id] for id in collections if id in idc_objects],
                              key=lambda collection: collection.collection_id)

    for idc_collection_id in sorted(new_objects,
            key=lambda idc_collection_id: collections[idc_collection_id]['collection_id']):
        if not collections[idc_collection_id]['collection_id'] in skips:
            # The collection is new, so we must ingest it
            new_collection = Collection()
            new_collection.collection_id = collections[idc_collection_id]['collection_id']
            new_collection.idc_collection_id = idc_collection_id
            new_collection.uuid = str(uuid4())
            if args.build_mtm_db:
                new_collection.min_timestamp = collections[idc_collection_id]['min_timestamp']
                new_collection.max_timestamp = collections[idc_collection_id]['max_timestamp']
                new_collection.sources = collections[idc_collection_id]['sources']
                new_collection.hashes = collections[idc_collection_id]['hashes']
                new_collection.revised = False
            else:
                new_collection.min_timestamp = datetime.utcnow()
                new_collection.revised = collections[idc_collection_id]['sources']
                new_collection.sources = [False, False]
                new_collection.hashes = None
            new_collection.init_idc_version=args.version
            new_collection.rev_idc_version=args.version
            new_collection.final_idc_version=0
            new_collection.done = False
            new_collection.is_new = True
            new_collection.expanded = False

            version.collections.append(new_collection)
            rootlogger.debug('p%s: Collection %s is new', args.id, new_collection.collection_id)

    for collection in existing_objects:
        if not collection.collection_id in skips:
            # idc_hashes = all_sources.idc_collection_hashes(collection)
            idc_hashes = collection.hashes
            src_hashes = all_sources.src_collection_hashes(collection.collection_id)
            # revised = all_sources.collection_was_revised(collection, collections[collection.idc_collection_id]['sources'])
            revised = [x != y for x, y in zip(idc_hashes[:-1], src_hashes)]
            if any(revised):
                # If any sources has an updated version of this object, create a new version.
                # rootlogger.debug('**Collection %s needs revision',collection.collection_id)
                rev_collection = clone_collection(collection, uuid=str(uuid4()))

                # Here is where we update the collecton ID in case it has changed
                rev_collection.collection_id = collections[collection.idc_collection_id]['collection_id']

                rev_collection.done = False
                rev_collection.is_new = False
                rev_collection.expanded = False
                if args.build_mtm_db:
                    rev_collection.min_timestamp = collections[collection.idc_collection_id]['min_timestamp']
                    rev_collection.max_timestamp = collections[collection.idc_collection_id]['max_timestamp']
                    rev_collection.sources = collections[collection.idc_collection_id]['sources']
                    rev_collection.hashes = collections[collection.idc_collection_id]['hashes']
                    rev_collection.rev_idc_version = collections[collection.idc_collection_id]['rev_idc_version']
                else:
                    # new_collection.hashes = src_hashes
                    # new_collection.hashes.append(get_merkle_hash([hash for hash in src_hashes if hash]))
                    rev_collection.hashes = None
                    rev_collection.revised = revised
                    rev_collection.rev_idc_version = args.version
                version.collections.append(rev_collection)
                rootlogger.debug('p%s: Collection %s is revised',  args.id, rev_collection.collection_id)

                # Mark the now previous version of this object as having been replaced
                # and drop it from the revised version
                collection.final_idc_version = args.previous_version
                version.collections.remove(collection)
            elif collection.collection_id != collections[collection.idc_collection_id]['collection_id']:
                # The collection_id has changed. Treat as a revised collection even though the hash is unchanged
                rootlogger.debug('**Collection_id changed %s. Generating revision',collection.collection_id)
                rev_collection = clone_collection(collection, uuid=str(uuid4()))

                # Here is where we update the collecton ID in case it has changed
                rev_collection.collection_id = collections[collection.idc_collection_id]['collection_id']
                if args.build_mtm_db:
                    rev_collection.min_timestamp = collections[collection.idc_collection_id]['min_timestamp']
                    rev_collection.max_timestamp = collections[collection.idc_collection_id]['max_timestamp']
                rev_collection.rev_idc_version = args.version

                # The collection is otherwise done
                collection.done = True
                collection.expanded = True

                version.collections.append(rev_collection)
                rootlogger.debug('p%s: Collection %s is renamed',  args.id, rev_collection.collection_id)

                # Mark the now previous version of this object as having been replaced
                # and drop it from the revised version
                collection.final_idc_version = args.previous_version
                version.collections.remove(collection)

            else:
                # The collection is unchanged. Just add it to the version
                if not args.build_mtm_db:
                    collection.min_timestamp = datetime.utcnow()
                    collection.max_timestamp = datetime.utcnow()
                    # Make sure the collection is marked as done and expanded
                    # Shouldn't be needed if the previous version is done
                    collection.done = True
                    collection.expanded = True
                rootlogger.debug('p%s: Collection %s unchanged', args.id, collection.collection_id)

    for collection in retired_objects:
        breakpoint()
        if not collection.collection_id in skips:
            # rootlogger.debug('p%s: Collection %s retiring', args.id, collection.collection_id)
            # Mark the now previous version of this object as having been retired
            retire_collection(args, collection)
            version.collections.remove(collection)

    version.expanded = True
    sess.commit()
    rootlogger.info("Expanded version")

def build_version(sess, args, all_sources, version):
    begin = time.time()
    try:
        skips = open(args.skips).read().splitlines()
    except:
        skips = []
    rootlogger.debug("p%s: Expand version %s", args.id, args.version)
    if not version.expanded:
        expand_version(sess, args, all_sources, version, skips)
    idc_collections = sorted(version.collections, key=lambda collection: collection.collection_id)
    rootlogger.info("p%s: Expanded Version %s; %s collections", args.id, args.version, len(idc_collections))
    for collection in idc_collections:
        if not collection.collection_id in skips:
            collection_index = f'{idc_collections.index(collection) + 1} of {len(idc_collections)}'
            if not collection.done:
                build_collection(sess, args, all_sources, collection_index, version, collection)
            else:
                rootlogger.info("p%s: Collection %s, %s, previously built", args.id, collection.collection_id, collection_index)

    # Check if we are really done
    if all([collection.done for collection in idc_collections]):
        version.max_timestamp = max([collection.max_timestamp for collection in version.collections if collection.max_timestamp != None])

        if args.build_mtm_db:
            version.hashes = all_sources.src_version_hashes(version)
            # hashes = version.hashes
            version.done = True
            sess.commit()
            duration = str(timedelta(seconds=(time.time() - begin)))
            rootlogger.info("Completed Version %s, in %s", version.version, duration)
        else:
            # Get the new version's hashes
            idc_hashes = all_sources.idc_version_hashes(version)
            # Check whether the hashes have changed. If so then declare this a new version
            # otherwise revert the version number
            previous_hashes = list(sess.query(Version).filter(Version.version == args.previous_version).first().hashes)
            if idc_hashes != previous_hashes:
                version.hashes = idc_hashes
                version.sources = accum_sources(version, version.collections)
                version.done = True
                version.revised = [True, True]
            else:
                # Revert the version
                egest_version(sess, args, version)
                rootlogger.info("Version unchanged, remains at %s", args.previous_version)
            sess.commit()
            duration = str(timedelta(seconds=(time.time() - begin)))
            rootlogger.info("Completed Version %s, in %s", version.version, duration)

    else:
        rootlogger.info("Not all collections are done. Rerun.")
