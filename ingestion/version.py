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
from ingestion.utils import accum_sources
from ingestion.collection import clone_collection, build_collection

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
    # If we are here, we are beginning work on this version.
    # try:
    #     todos = open(args.todos).read().splitlines()
    # except:
    #     todos = []

    # Get the collections that the sources know about
    collections = all_sources.collections()

    # Get the collections in the previous version
    idc_objects_results = sess.query(Collection)
    idc_objects = {c.collection_id:c for c in idc_objects_results}

    # New collections
    new_objects = sorted([collection_id for collection_id in collections if collection_id not in idc_objects])
    # Collections that are no longer known about by any source
    retired_objects = sorted([idc_objects[id] for id in idc_objects if id not in collections], key=lambda collection: collection.collection_id)
    # Collections that are in the previous version and still known about by the sources
    existing_objects = sorted([idc_objects[id] for id in collections if id in idc_objects], key=lambda collection: collection.collection_id)

    for collection in retired_objects:
        if not collection.collection_id in skips:
            rootlogger.info('Collection %s retiring', collection.collection_id)
            # Mark the now previous version of this object as having been retired
            collection.final_idc_version = collection.previous_version

            # Remove from the collection table, moving instance data to the retired table
            # retire_collection(sess, args, collection, instance_source['path'].value)
            # if not any(collection.sources):
            #     sess.delete(collection)

    for collection in existing_objects:
        if not collection.collection_id in skips:
            if all_sources.collection_was_updated(collection):
                # If the sources have an updated version of this object, create a new version.
                rootlogger.debug('**Collection %s needs revision',collection.collection_id)
                # collection.min_timestamp = datetime.utcnow()
                rev_collection = clone_collection(collection, uuid=str(uuid4()))
                assert args.version == collections[collection.collection_id]['rev_idc_version']
                # rev_collection.uuid = str(uuid4())
                rev_collection.revised = True
                rev_collection.done = False
                rev_collection.is_new = False
                rev_collection.expanded = False
                if args.build_mtm_db:
                    rev_collection.min_timestamp = collections[collection.collection_id]['min_timestamp']
                    rev_collection.max_timestamp = collections[collection.collection_id]['max_timestamp']
                    rev_collection.sources = collections[collection.collection_id]['sources']
                    rev_collection.hashes = collections[collection.collection_id]['hashes']
                    rev_collection.rev_idc_version = collections[collection.collection_id]['rev_idc_version']
                else:
                    rev_collection.rev_idc_version = args.version
                version.collections.append(rev_collection)

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
                rootlogger.debug('Collection %s unchanged',collection.collection_id)
                # version.collections.append(collection)

    for collection_id in new_objects:
        if not collection_id in skips:
            # The collection is new, so we must ingest it
            rev_collection = Collection()
            rev_collection.collection_id = collection_id
            rev_collection.idc_collection_id = str(uuid4())
            rev_collection.uuid = str(uuid4())
            if args.build_mtm_db:
                rev_collection.min_timestamp = collections[collection_id]['min_timestamp']
                rev_collection.max_timestamp = collections[collection_id]['max_timestamp']
                rev_collection.sources = collections[collection_id]['sources']
                rev_collection.hashes = collections[collection_id]['hashes']
            else:
                rev_collection.min_timestamp = datetime.utcnow()
                rev_collection.sources = (False,False)
                rev_collection.hashes = ("","","")
            rev_collection.max_timestamp = rev_collection.min_timestamp
            rev_collection.init_idc_version=args.version
            rev_collection.rev_idc_version=args.version
            rev_collection.final_idc_version=0
            rev_collection.revised = False
            rev_collection.done = False
            rev_collection.is_new = True
            rev_collection.expanded = False

            version.collections.append(rev_collection)

    version.expanded = True
    sess.commit()
    rootlogger.info("Expanded version")

def build_version(sess, args, all_sources, version):
    # Session = sessionmaker(bind= sql_engine)
    # version = version_is_done(sess, args.version)
    begin = time.time()
    try:
        skips = open(args.skips).read().splitlines()
    except:
        skips = []
    if not version.expanded:
        expand_version(sess, args, all_sources, version, skips)
    idc_collections = [c for c in sess.query(Collection).order_by('collection_id')]
    rootlogger.info("Version %s; %s collections", args.version, len(idc_collections))
    for collection in idc_collections:
        if not collection.collection_id in skips:
        # if True:
            collection_index = f'{idc_collections.index(collection) + 1} of {len(idc_collections)}'
            if not collection.done:
                build_collection(sess, args, all_sources, collection_index, version, collection)
            else:
                rootlogger.info("Collection %s, %s, previously built", collection.collection_id, collection_index)

    # Check if we are really done
    # if all([collection.done for collection in idc_collections if not collection.collection_id in skips]):
    if all([collection.done for collection in idc_collections]):
        version.max_timestamp = max([collection.max_timestamp for collection in version.collections if collection.max_timestamp != None])

        if args.build_mtm_db:
            # hashes = ["","",""]
            # for source_id,source in all_sources.sources.items():
            #     hashes[source_id.source_id] = source.idc_version_hash(version)

            version.hashes = all_sources.src_version_hashes(version)
            # hashes = version.hashes
            version.done = True
            sess.commit()
            duration = str(timedelta(seconds=(time.time() - begin)))
            rootlogger.info("Version %s, completed in %s", version.version, duration)
        else:
            hashes = all_sources.idc_version_hashes(version)
            try:
                if all_sources.src_version_hashes(version.version) != hash:
                    errlogger.error('Hash match failed for version %s', version.version)
                else:
                    # Check whether the hash has changed. If so then declare this a new version
                    # otherwise revert the version number
                    if hashes != version.hashes:
                        version.hashes = hashes
                        version.sources = accum_sources(version, version.collections)
                        version.done = True
                        version.revised = True
                    else:
                        # Revert the version, to show it is unchanged
                        version.version = args.previous_version
                        rootlogger.info("Version unchanged, remains at %s", version.version-1)
                    version.done = True
                    sess.commit()
                duration = str(timedelta(seconds=(time.time() - begin)))
                rootlogger.info("Version %s, completed in %s", version.version, duration)

            except Exception as exc:
                errlogger.error('Could not validate version hash for %s: %s', version.version, exc)

    else:
        rootlogger.info("Not all collections are done. Rerun.")
